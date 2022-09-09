package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.converter.ObjectIdentifierGenerator;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;

import static reactor.core.publisher.Mono.fromFuture;

@Service
@Slf4j
@Profile("(test & s3) | !test")
public class S3FileStore implements FileStore {

    // max objects per request of deleteObjects
    private static final int DELETE_OBJECTS_MAX = 1000;
    private static final Duration READY_TIMEOUT = Duration.ofSeconds(93L);

    private static final ChecksumAlgorithm CHECKSUM_ALGORITHM = ChecksumAlgorithm.SHA1;
    private final S3AsyncClient s3Client;
    private final String bucketName;
    private final Mono<Void> isReady;

    public S3FileStore(S3AsyncClient s3Client, @Qualifier("ROOT") Bucket bucket) {
        this.s3Client = s3Client;
        this.bucketName = bucket.name();
        isReady = this.generateIsReady();
    }


    @EventListener
    public void handleEvent(ApplicationStartedEvent event) {
        // Doesn't run async so this is intended to block application startup
        // until isReady completes
        try {
            this.isReady.block( READY_TIMEOUT );
        } catch (Exception e) {
            throw new IllegalStateException( "Failed to prepare S3FileStore", e );
        }
    }

    @Override
    public Mono<Void> isReady() throws RuntimeException {
        return isReady;
    }

    @Override
    public Mono<Boolean> createBucketIfNotExists() {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucketName)
                .build();
        return fromFuture( s3Client.createBucket( request ) )
                .map(r -> true)
                .onErrorReturn( BucketAlreadyOwnedByYouException.class, false )
                .retry(3)
                .timeout( Duration.ofSeconds( 30 ) );

    }

    // True if bucket exists, false if not, exception if error occurs
    public Mono<Boolean> bucketExists() throws RuntimeException {
        HeadBucketRequest request = HeadBucketRequest.builder().bucket( bucketName ).build();
        return fromFuture( s3Client.headBucket( request ) )
                .map(r -> true)
                .onErrorReturn( NoSuchBucketException.class, false );
    }

    @Override
    public <T extends FileDto> Mono<Void> putFile(Flux<ByteBuffer> data, T fileDto, CloudUser cloudUser) throws RuntimeException {
        // uses fileId, checksum, size and probably in the future, content type from fileDto
        var request = PutObjectRequest.builder()
                .bucket( bucketName )
                .contentLength( fileDto.getSize() )
                // add content type to file
                .contentType( MediaType.APPLICATION_OCTET_STREAM_VALUE )
                .key(ObjectIdentifierGenerator.generate( fileDto, cloudUser ).key() )
                .checksumAlgorithm( CHECKSUM_ALGORITHM )
                .checksumSHA1( fileDto.getChecksum() )
                .build();
        return Mono.fromFuture( s3Client.putObject( request, AsyncRequestBody.fromPublisher(data) ) )
                .then();
    }

    @Override
    public <T extends FileDto> Flux<ByteBuffer> getFile(T fileDto, CloudUser cloudUser) {
        var request = GetObjectRequest.builder()
                .bucket( bucketName )
                .key( ObjectIdentifierGenerator.generate( fileDto, cloudUser ).key() )
                .build();
        return Mono.fromFuture(s3Client.getObject( request, AsyncResponseTransformer.toPublisher() ) )
                .flatMapMany( Flux::from );
    }

    @Override
    public Mono<Void> deleteFiles(Flux<UUID> fileIds, CloudUser cloudUser) throws RuntimeException {
        Flux<ObjectIdentifier> objectIdentifiers = fileIds.map(fileId -> ObjectIdentifierGenerator.generate(fileId, cloudUser) );
        return deleteObjects( objectIdentifiers );
    }

    Mono<Void> deleteObjects(Flux<ObjectIdentifier> objects) throws RuntimeException {
        return objects.buffer( DELETE_OBJECTS_MAX )
                .map(objectIdentifiers -> Delete.builder().objects( objectIdentifiers ).build() )
                .map(delete -> DeleteObjectsRequest.builder().bucket( bucketName ).delete( delete ).build() )
                .flatMap( request -> Mono.fromFuture(s3Client.deleteObjects( request ) )
                        .map(response -> {
                            System.out.println(response);
                            if ( response.hasErrors() && !response.errors().isEmpty() ) {
                                throw new RuntimeException("Error while deleting file");
                            }
                            return response.hasDeleted() && !response.deleted().isEmpty();
                        } ) ).then();
    }

    Mono<Boolean> deleteObjects(Flux<ObjectIdentifier> objects, Bucket bucket) {
        return objects.buffer( DELETE_OBJECTS_MAX )
                .map(objectIdentifiers -> Delete.builder().objects( objectIdentifiers ).build() )
                .map(delete -> DeleteObjectsRequest.builder().bucket( bucket.name() ).delete( delete ).build() )
                .flatMap( request -> Mono.fromFuture(s3Client.deleteObjects( request ) )
                        .map(response -> {
                            System.out.println(response);
                            if ( response.hasErrors() && !response.errors().isEmpty() ) {
                                return false;
                            }
                            return response.hasDeleted() && !response.deleted().isEmpty();
                        } ) ).all(b -> b);
    }

    // for testing
    Mono<Bucket> deleteAllObjectsInBucket(Bucket bucket) {
        var objects = this.listObjects(bucket)
                .map(S3Object::key)
                .map( k -> ObjectIdentifier
                        .builder()
                        .key( k ).build() );
        return deleteObjects( objects, bucket ).filter( res -> false).map( failure -> { throw new IllegalStateException("unable to delete bucket"); } )
                .thenReturn(bucket);
    }

    // for testing: this is not suitable for production.  Will only be able to list first 1000 objects
    Flux<S3Object> listObjects() {
        var request = ListObjectsV2Request.builder()
                .bucket( bucketName )
                .build();
        return Mono.fromFuture(s3Client.listObjectsV2( request ) )
                .mapNotNull( ListObjectsV2Response::contents )
                .flatMapMany( Flux::fromIterable );
    }

    // for testing
    Flux<Bucket> listBuckets() {
        return Mono.fromFuture(s3Client.listBuckets() )
                .map( ListBucketsResponse::buckets )
                .flatMapMany( Flux::fromIterable );
    }

    // for testing: this is not suitable for production.  Will only be able to list first 1000 objects
    Flux<S3Object> listObjects(Bucket bucket) {
        var request = ListObjectsV2Request.builder()
                .bucket( bucket.name() )
                .build();
        return Mono.fromFuture(s3Client.listObjectsV2( request ) )
                .mapNotNull( ListObjectsV2Response::contents )
                .flatMapMany( Flux::fromIterable );
    }

    // for testing
    Mono<Void> deleteBucket(Bucket bucket) throws RuntimeException {
        var request = DeleteBucketRequest.builder()
                .bucket( bucket.name() )
                .build();
        return fromFuture( s3Client.deleteBucket( request ) ).then();
    }

    private Mono<Void> generateIsReady() {
        return this.createBucketIfNotExists()
                .timeout(READY_TIMEOUT)
                .then()
                .cache();

    }
}
