package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.testconainer.EnableMinioTestContainerContextCustomizerFactory.EnableMinioTestContainer;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnablePostgresTestContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@SpringBootTest
@EnablePostgresTestContainer
@EnableMinioTestContainer
@ActiveProfiles(value = {"test", "s3", "dev"})
public class DocumentServiceIntTest {

    @Autowired
    private DocumentService documentService;

    @Autowired
    private S3FileStore s3FileStore;

    @Autowired
    private DSLContext dsl;

    private final CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    private final CloudUser user1 = CloudUser.builder()
            .userId( UUID.fromString( "ffffffff-ffff-ffff-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    @BeforeEach
    void before() throws URISyntaxException, IOException {
        s3FileStore.listBuckets().flatMap( s3FileStore::deleteAllObjectsInBucket )
                .flatMap( s3FileStore::deleteBucket )
                // it seems that create bucket was being evaluated eagerly and sometimes would
                // fire before deleteBucket when using a simple then
                .then( Mono.defer( () -> s3FileStore.createBucketIfNotExists() ) ).block();
        Path schemaFile = Paths.get( this.getClass()
                .getClassLoader()
                .getResource( "tests-schema.sql" )
                .toURI() );
        String sql = Files.readString( schemaFile );
        Mono.from( dsl.query( sql ) ).block();
    }
}
