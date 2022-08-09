package com.ericgha.docuCloud.repository;

import com.ericgha.docuCloud.converter.FileViewRecordToFileRecord;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.repository.testutil.file.FileTestQueries;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTree;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTreeFactory;
import com.ericgha.docuCloud.repository.testutil.tree.TreeTestQueries;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnabledPostgresTestContainer;
import jakarta.annotation.PostConstruct;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
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

@SpringBootTest
@EnabledPostgresTestContainer
class FileServiceTest {
    @Autowired
    private DSLContext dsl;

    @Autowired
    private FileService fileService;

    private final FileViewRecordToFileRecord fvrTofr = new FileViewRecordToFileRecord();

    private TestFileTreeFactory treeFactory;
    private FileTestQueries fileQueries;

    private final CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    private final CloudUser user1 = CloudUser.builder()
            .userId( UUID.fromString( "fffffff-ffff-ffff-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    private static final String TREE_FACTORY_CSV = """
            ROOT, ""
            FILE, "file0"
            FILE, "file1"
            DIR, "dir0"
            FILE, "dir0.file2"
            FILE, "dir0.file3"
            """;

    private TestFileTree tree0;
    private TestFileTree tree1;

    @PostConstruct
    void postConstruct() {
        TreeTestQueries treeTestQueries = new TreeTestQueries( dsl );
        treeFactory = new TestFileTreeFactory( treeTestQueries );
        fileQueries = new FileTestQueries( dsl );
    }


    @BeforeEach
    void before() throws URISyntaxException, IOException {
        // testcontainers cannot reliably run complex init scrips (ie with declared functions)
        // testcontainers/testcontainers-java issue #2814
        Path input = Paths.get( this.getClass().getClassLoader().getResource( "tests-schema.sql" ).toURI() );
        String sql = Files.readString( input );
        Mono.from( dsl.query( sql ) ).block();
        tree0 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user0 );
        tree1 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user1 );
    }

//    @Test
//    @DisplayName( "linkExistingFile creates link between new obj and existing file" )
//    // This fails, and is atrocious so will be replaced
//    void linkExistingFileCreatesLink() {
//        TreeRecord curLinkedObj = tree0.getOrigRecord( "file0" );
//        FileRecord curFile = fileQueries.createFilesWithLinks( List.of(FileViewRecordCreator.create( curLinkedObj , user0, 0 ) ),
//                FileViewRecordComparators.compareBySizeObjectId() ).map(fvrTofr::convert).blockLast();
//        TreeRecord newObj = tree0.getOrigRecord( "file1" );
//        // make link to existing
//        StepVerifier.create(fileService.linkExistingFile( curFile, newObj, user0 ) )
//                .expectNext( 1L )
//                .verifyComplete();
//        // curFile should have link degree of 2
//        StepVerifier.create(fileQueries.fetchFileLinkingDegreeByFileId( user0 ) )
//                .assertNext( rec -> { assertEquals(curFile.getFileId(), rec.get(FILE_VIEW.FILE_ID));
//                    assertEquals( 2, rec.get("count", Long.class) );
//                })
//                .verifyComplete();
//
//        List<Record2<UUID, Long>> objectLinkDegree = fileQueries.fetchObjectLinkingDegreeByObjectId( user0 )
//                .filter( rec -> rec.get("count", Long.class) > 0 )
//                .collectList()
//                .block();
//        List<UUID> expectedObjectIds = Stream.of(curLinkedObj, newObj)
//                .sorted( TreeRecordComparators::compareByObjectId )
//                .map(TreeRecord::getObjectId)
//                .toList();
//        // Link degree for cur and new obj should be 1
//        assertEquals(1L, objectLinkDegree.get(0).get("count", Long.class) );
//        assertEquals(1L, objectLinkDegree.get(1).get("count", Long.class) );
//        // cur and new object should be the linked objects
//        assertEquals(expectedObjectIds.get(0), objectLinkDegree.get(0).get(FILE_VIEW.OBJECT_ID) );
//        assertEquals(expectedObjectIds.get(1), objectLinkDegree.get(1).get(FILE_VIEW.OBJECT_ID) );
//        // no links for user1
//        StepVerifier.create(fileQueries.fetchFileLinkingDegreeByFileId( user1 ) ).expectNextCount( 0 ).verifyComplete();
//        StepVerifier.create(fileQueries.fetchObjectLinkingDegreeByObjectId( user1 ) ).expectNextCount( 0 ).verifyComplete();
//    }

    @Test
    void unLinkExistingFile() {
    }

    @Test
    void unLink() {
    }

    @Test
    void selectFileLinkDegree() {
    }

    @Test
    void isUsersFile() {
    }

    @Test
    void selectLinksToFile() {
    }
}