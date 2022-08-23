package com.ericgha.docuCloud.repository.testtool.assertion;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.repository.testtool.file.FileTestQueries;
import com.ericgha.docuCloud.repository.testtool.file.TestFiles;
import com.ericgha.docuCloud.repository.testtool.file.TestFilesFactory;
import com.ericgha.docuCloud.repository.testtool.tree.TestFileTree;
import com.ericgha.docuCloud.repository.testtool.tree.TestFileTreeFactory;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnabledPostgresTestContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@EnabledPostgresTestContainer
class TestFileAssertionIntTest {


    @Autowired
    DSLContext dsl;
    @Autowired
    TestFileTreeFactory treeFactory;
    @Autowired
    TestFilesFactory fileFactory;
    @Autowired
    FileTestQueries fileTestQueries;

    TestFileTree tree0;
    TestFiles files0;

    private static final CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    private static final String TREE_FACTORY_CSV = """
            ROOT, ""
            FILE, "fileObj0"
            DIR, "dir0"
            FILE, "dir0.fileObj1"
            FILE, "fileObj2"
            FILE, "dir0.fileObj3"
            FILE, "fileObj4"
            """;

    private static final String FILE_FACTORY_CSV = """
            fileObj0, fileRes0
            dir0.fileObj1, fileRes0
            fileObj2, fileRes1
            """;

    @BeforeEach
    void before() throws IOException, URISyntaxException {
        Path schemaFile = Paths.get( this.getClass().getClassLoader()
                .getResource( "tests-schema.sql" ).toURI() );
        String sql = Files.readString( schemaFile );
        Mono.from( dsl.query( sql ) ).block();
        tree0 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user0 );
        files0 = fileFactory.constructFromCsv( FILE_FACTORY_CSV, tree0 );
    }

    @Test
    @DisplayName("assertRepositoryState does not throw when adjacencies match repository state")
    void assertRepositoryStateDoesNotThrow() {
        assertDoesNotThrow( () ->
                TestFileAssertion.assertRepositoryState( files0, FILE_FACTORY_CSV ) );
    }

    @Test
    @DisplayName("assertRepositoryState throws when adjacencies contains incorrect checksum")
    void assertRepositoryStateThrowsIncorrectChecksum() {
        String adjacencies = """
                fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                # following line incorrect
                fileObj2, fileRes0
                """;
        assertThrows( AssertionError.class, () ->
                TestFileAssertion.assertRepositoryState( files0, adjacencies ) );
    }

    @Test
    @DisplayName("assertRepositoryState throws when adjacencies contains incorrect pathStr")
    void assertRepositoryStateThrowsIncorrectPathStr() {
        String adjacencies = """
                fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                # Following line incorrect
                fileObj3, fileRes1
                """;
        assertThrows( AssertionError.class, () ->
                TestFileAssertion.assertRepositoryState( files0, adjacencies ) );
    }

    @Test
    @DisplayName("assertContains does not throw with valid adjacencies")
    void assertContainsDoesNotThrow() {
        files0.insertFileViewRecord( "dir0.fileObj3", "fileRes3" );
        assertDoesNotThrow( () -> TestFileAssertion.assertContains( files0, FILE_FACTORY_CSV ) );
    }

    @Test
    @DisplayName("assertContains throws when invalid adjacency")
    void assertContainsThrowsWithInvalidAdjacency() {
        String adjacencies = """
                fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                # Following line incorrect
                fileObj3, fileRes1
                """;
        assertThrows( AssertionError.class, () -> TestFileAssertion.assertContains( files0, adjacencies ) );
    }

    @Test
    @DisplayName("assertNoChanges does not throw when no changes")
    void assertNoChanges() {
        assertDoesNotThrow( () -> TestFileAssertion.assertNoChanges( files0 ) );
    }

    @Test
    @DisplayName("assertNoChanges throws when changes made")
    void assertNoChangesThrowsWhenChangeMade() {
        FileViewDto untrackedLink = FileViewDto.builder().fileId( UUID.randomUUID() )
                .objectId( tree0.getOrigRecord( "dir0.fileObj3" ).getObjectId() )
                .userId( tree0.getUserId() )
                .checksum( "fileRes3" )
                .size( 0L )
                .build();
        // must manually insert to prevent TestFiles from recording
        fileTestQueries.createFileWithLink( untrackedLink ).block();
        assertThrows( AssertionError.class,
                () -> TestFileAssertion.assertNoChanges( files0 ) );
    }

    @Test
    @DisplayName( "assertNoChanges for does not throw when no changes to listed adjacencies" )
    void assertNoChangesForDoesNotThrowWhenNoChanges() {
        FileViewDto untrackedLink = FileViewDto.builder().fileId( UUID.randomUUID() )
                .objectId( tree0.getOrigRecord( "dir0.fileObj3" ).getObjectId() )
                .userId( tree0.getUserId() )
                .checksum( "fileRes3" )
                .size( 0L )
                .build();
        // must manually insert to prevent TestFiles from recording
        fileTestQueries.createFileWithLink( untrackedLink ).block();
        // although change was made, its adjacency is not included in adjacencies
        String adjacencies = FILE_FACTORY_CSV;
        assertDoesNotThrow( () -> TestFileAssertion.assertNoChangesFor( files0, adjacencies ) );
    }

    @Test
    @DisplayName( "assertNoChangesFor throws when provided untracked adjacency" )
    void assertNoChangesForThrowsWhenUntrackedAdjacency() {
        FileViewDto untrackedLink = FileViewDto.builder().fileId( UUID.randomUUID() )
                .objectId( tree0.getOrigRecord( "dir0.fileObj3" ).getObjectId() )
                .userId( tree0.getUserId() )
                .checksum( "fileRes3" )
                .size( 0L )
                .build();
        // must manually insert to prevent TestFiles from recording
        fileTestQueries.createFileWithLink( untrackedLink ).block();
        // although change was made, its adjacency is not included in adjacencies
        String adjacencies = """
            fileObj0, fileRes0
            dir0.fileObj1, fileRes0
            fileObj2, fileRes1
            dir0.fileObj3, fileRes3
            """;
        assertThrows(AssertionError.class, () -> TestFileAssertion.assertNoChangesFor( files0, adjacencies ) );
    }

    @Test
    @DisplayName( "assertNoChangesFor throws when fileId changes" )
    void assertNoChangesForThrowsWhenFileIdChanges() {
        FileViewDto origFileView = files0.getOrigFileViewsFor( "fileObj2" ).pollFirst();
        FileDto origFile = files0.getOrigFileFor( "fileRes1" );
        // remove file and links to fileRes1
        fileTestQueries.deleteAllLinksTo( origFile ).block();
        fileTestQueries.deleteFile( origFile ).block();
        // re-insert it, new timestamps are generated
        fileTestQueries.createFileWithLink( origFileView ).block();
        assertThrows(AssertionError.class, () -> TestFileAssertion.assertNoChangesFor( files0, FILE_FACTORY_CSV ) );
    }
}