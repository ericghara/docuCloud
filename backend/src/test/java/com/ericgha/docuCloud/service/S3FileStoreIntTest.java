package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.configuration.AppConfig;
import com.ericgha.docuCloud.configuration.AwsConfig;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.testconainer.EnableMinioTestContainerContextCustomizerFactory.EnableMinioTestContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;


@SpringBootTest(classes = {S3FileStore.class, S3AsyncClient.class, AwsConfig.class, AppConfig.class})
@Slf4j
@EnableMinioTestContainer
@ActiveProfiles(value = {"test","s3","dev"})
public class S3FileStoreIntTest {

    @Autowired
    S3FileStore s3FileStore;

    @Autowired
    S3AsyncClient s3Client;

    @Autowired
    Bucket bucket;

    private final CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    private final CloudUser user1 = CloudUser.builder()
            .userId( UUID.fromString( "ffffffff-ffff-ffff-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    @BeforeEach
    void before() {
        s3FileStore.listBuckets()
                .flatMap( s3FileStore::deleteAllObjectsInBucket )
                .flatMap( s3FileStore::deleteBucket )
                // it seems that create bucket was being evaluated eagerly and sometimes would
                // fire before deleteBucket when using a simple then
                .then(  Mono.defer(() -> s3FileStore.createBucketIfNotExists() ) ).block();
    }

    @Test
    @DisplayName( "is ready complete, indicating bucket created" )
    void isReadyReturnsTrue() {
        StepVerifier.create( s3FileStore.isReady() )
                .expectNextCount( 0 )
                .verifyComplete();
    }

    @DisplayName( "Bucket Exists returns true if bucket exists" )
    @Test
    void bucketExists() {
        // fix this test it currently only works in local environment
        Mono<Boolean> exists = s3FileStore.bucketExists();
        StepVerifier.create( exists )
                .expectNext( true )
                .verifyComplete();
    }

    @Test
    @DisplayName( "createBucket creates a new bucket" )
    void createBucketReturnsTrue() {
        S3FileStore newFilestore = new S3FileStore( s3Client, Bucket.builder().name( UUID.randomUUID().toString() ).build() );
        // cannot directly test this (without partial mocking)
        StepVerifier.create( newFilestore.isReady() )
                .expectNextCount( 0 )
                .verifyComplete();
    }

    @Test
    @DisplayName( "createBucket creates a new bucket" )
    void createBucketCreatesABucket() {
        S3FileStore newFilestore = new S3FileStore( s3Client, Bucket.builder().name( UUID.randomUUID().toString() ).build() );
        StepVerifier.create( newFilestore.isReady()
                        .then(Mono.defer( newFilestore::bucketExists ) ) )
                .expectNext( true )
                .verifyComplete();
    }

    @Test
    @DisplayName( "putFile uploads a file" )
    void putFileReturnsVoid() throws NoSuchAlgorithmException {
        byte[] data = new byte[256];
        byte[] digest = MessageDigest.getInstance( "SHA-1" ).digest(data);
        // minio currently doesn't validate uploaded file with checksum
        // so unfortunately that portion of putFile is not tested
        String checksum = Base64.getEncoder().encodeToString( digest );
        Flux<ByteBuffer> dataFlux = Flux.fromIterable( List.of(ByteBuffer.wrap( data ) ) );
        FileDto fileDto = FileDto.builder().fileId( UUID.randomUUID() )
                .checksum( checksum )
                .size( (long) data.length )
                .build();
        var putMono = s3FileStore.putFile( dataFlux, fileDto, user0 );
        StepVerifier.create( putMono ).expectNextCount( 0 ).verifyComplete();
    }

    @Test
    @DisplayName( "getFile returns the expected file" )
    void getFileReturnsFile() throws NoSuchAlgorithmException {
        byte[] data = new byte[256];
        byte[] digest = MessageDigest.getInstance( "SHA-1" ).digest(data);
        String checksum = Base64.getEncoder().encodeToString( digest );
        Flux<ByteBuffer> dataFlux = Flux.fromIterable( List.of(ByteBuffer.wrap( data ) ) );
        FileDto fileDto = FileDto.builder().fileId( UUID.randomUUID() )
                .checksum( checksum )
                .size( (long) data.length )
                .build();
        var putMono = Mono.defer( () -> s3FileStore.putFile( dataFlux, fileDto, user0 ) );
        StepVerifier.create( putMono.thenMany( Flux.defer(() -> s3FileStore.getFile(fileDto, user0) ) ) )
                .assertNext( buffer -> assertArrayEquals(data,  buffer.array() ) )
                .verifyComplete();
    }

    @Test
    @DisplayName( "deleteFiles deletes expected file" )
    void deleteFile() throws NoSuchAlgorithmException {
        byte[] data = new byte[256];
        byte[] digest = MessageDigest.getInstance( "SHA-1" ).digest(data);
        // minio currently doesn't validate uploaded file with checksum
        // so unfortunately that portion of putFile is not tested
        String checksum = Base64.getEncoder().encodeToString( digest );
        Flux<ByteBuffer> dataFlux = Flux.fromIterable( List.of(ByteBuffer.wrap( data ) ) );
        FileDto fileDto = FileDto.builder().fileId( UUID.randomUUID() )
                .checksum( checksum )
                .size( (long) data.length )
                .build();
        var putMono = s3FileStore.putFile( dataFlux, fileDto, user0 );
        var delMono = Mono.defer(
                () -> s3FileStore.deleteFiles( Mono.just(fileDto.getFileId() ).flux(), user0 ) );
        var existsMono = Flux.defer( () -> s3FileStore.getFile( fileDto, user0 ) );
        StepVerifier.create( putMono.then(delMono)
                .thenMany( existsMono ) ).expectError( NoSuchKeyException.class ).verify();
    }
}
