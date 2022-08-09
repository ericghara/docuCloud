package com.ericgha.docuCloud.repository.testutil.file;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTree;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTreeFactory;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnabledPostgresTestContainer;
import jakarta.annotation.PostConstruct;
import org.jooq.DSLContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;

import static com.ericgha.docuCloud.repository.testutil.assertion.OffsetDateTimeAssertion.pastDateTimeWithinLast;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@EnabledPostgresTestContainer
class TestFilesTest {
    private static final String TREE_FACTORY_CSV = """
            ROOT, ""
            FILE, "fileObj0"
            FILE, "fileObj1"
            DIR, "dir0"
            FILE, "dir0.fileObj2"
            FILE, "dir0.fileObj3"
            """;

    @Autowired
    private DSLContext dsl;
    @Autowired
    private TestFileTreeFactory treeFactory;
    @Autowired
    private TestFilesFactory fileFactory;
    @Autowired
    private FileTestQueries fileQueries;

    private TestFileTree tree0;
    private TestFileTree tree1;

    CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    CloudUser user1 = CloudUser.builder()
            .userId( UUID.fromString( "fffffff-ffff-ffff-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    @PostConstruct
    void postConstruct() throws IOException, URISyntaxException {
        Path input = Paths.get( this.getClass().getClassLoader()
                .getResource( "tests-schema.sql" ).toURI() );
        String sql = Files.readString( input );
        Mono.from( dsl.query( sql ) ).block();
        this.tree0 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user0 );
        this.tree1 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user1 );
    }

    @Test
    @DisplayName( "createFileViewRecord creates a new file record linked to expected object" )
    void createFileViewRecordExistingFile() {
        String checksum = "fileRes0";
        String objPath = "fileObj0";
        TestFiles files0 = fileFactory.construct( tree0 );
        files0.createFileViewRecord( objPath, checksum );
        Flux<FileViewRecord> found = fileQueries.fetchRecordsByChecksum(checksum, tree0.getUserId() );
        UUID expectedObjectId = tree0.fetchByObjectPath( objPath ).getObjectId();
        StepVerifier.create( found ).assertNext( fvr -> {
            assertEquals(expectedObjectId, fvr.getObjectId() );
            assertNotNull(fvr.getFileId() );
            assertEquals(tree0.getUserId(), fvr.getUserId() );
            pastDateTimeWithinLast(fvr.getUploadedAt(), Duration.ofSeconds(1) );
            pastDateTimeWithinLast(fvr.getLinkedAt(), Duration.ofSeconds(1) );
            assertEquals(0L, fvr.getSize() );
        } )
        .verifyComplete();
    }

    @Test
    @DisplayName("createFileViewRecord records object <-> file link")
    void createFileViewRecordStoresRecord() {
        String checksum = "fileRes0";
        String objPath = "fileObj0";
        TestFiles files0 = fileFactory.construct( tree0 );
        files0.createFileViewRecord( objPath, checksum );
//        NavigableSet<TreeRecord> objects =

    }

}