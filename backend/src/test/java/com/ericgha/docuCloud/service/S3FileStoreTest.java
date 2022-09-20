package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.converter.ObjectIdentifierGenerator;
import com.ericgha.docuCloud.dto.CloudUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ActiveProfiles(value = {"test", "s3", "dev"})
@ExtendWith(MockitoExtension.class)
public class S3FileStoreTest {

    @Mock
    S3AsyncClient S3ClientMock;

    Bucket bucket = Bucket.builder().name( "ROOT" ).build();

    private final CloudUser user = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    S3FileStore s3FileStore;

    @BeforeEach
    void before() {
        doReturn( Mono.just( CreateBucketResponse.builder().build() ).toFuture() )
                .when( S3ClientMock ).createBucket( any( CreateBucketRequest.class ) );
        s3FileStore = new S3FileStore( S3ClientMock, bucket );
        s3FileStore.isReady().block( Duration.ofMillis( 100 ) );
    }

    @Test
    @DisplayName("deleteObjects throws IllegalArgumentException when num objects to delete is greater than DELETE_FILES_MAX")
    void deleteObjectsThrowsIllegalArgumentExceptionWhenTooManyObjects() {
        List<ObjectIdentifier> toDelete = ObjectIdentifierGenerator.generate(
                Stream.generate( UUID::randomUUID )
                        .limit( s3FileStore.getDeleteFilesMax() + 1 )
                        .toList(), user );
        // No request should be sent if num files exceeds DELETE_FILES_MAX
        verify( S3ClientMock, times( 0 ) )
                .deleteObjects( any( DeleteObjectsRequest.class ) );
        s3FileStore.deleteObjects( Mono.just( toDelete ) ).as( StepVerifier::create ).verifyError( IllegalArgumentException.class );
    }
}
