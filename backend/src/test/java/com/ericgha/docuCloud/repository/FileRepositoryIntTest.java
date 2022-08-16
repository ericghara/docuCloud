package com.ericgha.docuCloud.repository;

import com.ericgha.docuCloud.converter.FileViewDtoToFileDto;
import com.ericgha.docuCloud.converter.FileViewDtoToTreeJoinFileDto;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.dto.TreeJoinFileDto;
import com.ericgha.docuCloud.repository.testutil.assertion.TestFileAssertion;
import com.ericgha.docuCloud.repository.testutil.file.TestFiles;
import com.ericgha.docuCloud.repository.testutil.file.TestFilesFactory;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTree;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTreeFactory;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnabledPostgresTestContainer;
import org.jooq.CommonTableExpression;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.Tables.FILE_VIEW;
import static org.jooq.impl.DSL.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EnabledPostgresTestContainer
class FileRepositoryIntTest {
    @Autowired
    private DSLContext dsl;

    @Autowired
    private FileRepository fileRepository;

    @Autowired
    private FileViewDtoToFileDto fileViewToFile;

    @Autowired
    private FileViewDtoToTreeJoinFileDto fileViewToTreeJoinFile;

    @Autowired
    private TestFileTreeFactory treeFactory;
    @Autowired
    private TestFilesFactory fileFactory;
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
            # Available objects: dir0.fileObj3, fileObj4
            """;

    private TestFileTree tree0;
    private TestFileTree tree1;
    private TestFiles files0;
    // selectivity challenge
    private TestFiles files1;

    @BeforeEach
    void before() throws URISyntaxException, IOException {
        // testcontainers cannot reliably run complex init scrips (ie with declared functions)
        // testcontainers/testcontainers-java issue #2814
        Path schemaFile = Paths.get( this.getClass().getClassLoader().getResource( "tests-schema.sql" ).toURI() );
        String sql = Files.readString( schemaFile );
        Mono.from( dsl.query( sql ) ).block();
        tree0 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user0 );
        tree1 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user1 );
        files0 = fileFactory.constructFromCsv( FILE_FACTORY_CSV, tree0 );
        files1 = fileFactory.constructFromCsv( FILE_FACTORY_CSV, tree1 );
    }

    @Test
    @DisplayName("createEdge creates link between new obj and existing file")
    void createEdgeCreatesExpectedAdjacency() {
        final String objPathStr = "dir0.fileObj3";
        final String fileResChecksum = "fileRes1";
        final String expectedState = """
                fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                fileObj2, fileRes1
                # new Link
                dir0.fileObj3, fileRes1
                """;
        TreeDto treeObjToLink = tree0.getOrigRecord( objPathStr );
        FileDto fileResToLink = files0.getOrigFileFor( fileResChecksum );
        Long foundCount = fileRepository.createEdge( fileResToLink, treeObjToLink, user0 ).block();
        assertEquals( 1L, foundCount );

        TestFileAssertion.assertRepositoryState( files0, expectedState );
        TestFileAssertion.assertNoChangesFor( files0, FILE_FACTORY_CSV );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName("deleteEdge step deletes expected record")
    void deleteEdgeStepDeletesExpected() {
        final String objPathStr = "dir0.fileObj1";
        final String fileResChecksum = "fileRes0";
        final String expectedState = """
                fileObj0, fileRes0
                # deleted: dir0.fileObj1, fileRes0
                fileObj2, fileRes1
                """;
        FileViewDto fileViewToDelete = files0.getOrigFileViewFor( objPathStr, fileResChecksum );
        TreeJoinFileDto edgeToDelete = fileViewToTreeJoinFile.convert( fileViewToDelete );
        TreeJoinFileDto deletedFileView = Mono.from( fileRepository.deleteEdgeStep( edgeToDelete, user0 ) )
                .map( TreeJoinFileDto::fromRecord )
                .block();
        assertEquals( edgeToDelete, deletedFileView );
        TestFileAssertion.assertNoChangesFor( files0, expectedState );
        TestFileAssertion.assertRepositoryState( files0, expectedState );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName("selectFileNodeDegree returns node degrees for given file_ids")
    void selectFileNodeDegreeReturnsNodeDegrees() {
        UUID fileRes0Id = files0.getOrigFileFor( "fileRes0" ).getFileId();
        UUID fileRes1Id = files0.getOrigFileFor( "fileRes1" ).getFileId();
        Map<UUID, Long> expected = Map.of( fileRes0Id, 2L,
                fileRes1Id, 1L );
        // select fileIds
        CommonTableExpression<Record1<UUID>> fileIds = name("rm_link").as(dsl.select( field( "file_id", UUID.class ) )
                .from( DSL.values( row( fileRes0Id ), row( fileRes1Id ) )
                        .as( "ids", "file_id" ) ) );
        // get node degrees for selected fileIds
        StepVerifier.create( fileRepository.selectFileNodeDegreeFrom( fileIds ) )
                .thenConsumeWhile( record -> {
                    var curId = record.get( FILE_VIEW.FILE_ID );
                    var curDegree = record.get( "count", Long.class );
                    assertTrue( expected.containsKey( curId ), "Incorrect fileId: + curId" );
                    assertEquals( expected.get( curId ), curDegree, "Unexpected node degree" );
                    return true;
                } )
                .verifyComplete();
    }

    @Test
    @DisplayName("selectFileNodeDegree returns count = null when fileId not found")
    void selectFileNodeDegreeReturnsNullCountWhenFileIdNotFound() {
        UUID wrongFileId = UUID.randomUUID();
        // select fileIds
        CommonTableExpression<Record1<UUID>> fileIds = name("rm_link").as(dsl.select( field( "file_id", UUID.class ) )
                .from( DSL.values( row( wrongFileId ) )
                        .as( "ids", "file_id" ) ) );
        // get node degrees for selected fileIds
        StepVerifier.create( fileRepository.selectFileNodeDegreeFrom( fileIds ) )
                .assertNext( r -> {
                    assertEquals(wrongFileId, r.get(field("file_id", UUID.class) ) );
                    assertNull( r.get(field("count", Long.class) ) );
                })
                .verifyComplete();
    }

    @Test
    @DisplayName( "rmEdge deletes an edge and returns file_id when link degree of file_id = 0 after delete" )
    void rmEdgeReturnsFileIdWhenDegreeIsZero() {
        FileViewDto toDelete = ( files0.getOrigFileViewFor( "fileObj2", "fileRes1" ) );
        FileDto expectedFile = fileViewToFile.convert(toDelete);
        FileDto foundFile = fileRepository.rmEdge( fileViewToTreeJoinFile.convert(toDelete), user0 ).block();
//        assertEquals( expectedFile, foundFile );
        String expectedState = """
                fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                # deleted: fileObj2, fileRes1
                """;
        TestFileAssertion.assertRepositoryState( files0, expectedState );
        TestFileAssertion.assertNoChangesFor( files0, expectedState );
        TestFileAssertion.assertNoChanges( files1 );
    }


//    @Test
//    @DisplayName("linkExistingFile creates link between new obj and existing file")
//        // This fails, and is atrocious so will be replaced
//    void linkExistingFileCreatesLink() {
//        TreeRecord curLinkedObj = tree0.getOrigRecord( "file0" );
//        FileDto curFile = fileQueries.createFilesWithLinks( List.of( FileViewDtoCreator.create( curLinkedObj, user0, 0 ) ),
//                FileViewDtoComparators.compareBySizeObjectId() ).map( fvrTofr::convert ).blockLast();
//        TreeRecord newObj = tree0.getOrigRecord( "file1" );
//        // make link to existing
//        StepVerifier.create( fileService.linkExistingFile( curFile, newObj, user0 ) )
//                .expectNext( 1L )
//                .verifyComplete();
//        // curFile should have link degree of 2
//        StepVerifier.create( fileQueries.fetchFileLinkingDegreeByFileId( user0 ) )
//                .assertNext( rec -> {
//                    assertEquals( curFile.getFileId(), rec.get( FILE_VIEW.FILE_ID ) );
//                    assertEquals( 2, rec.get( "count", Long.class ) );
//                } )
//                .verifyComplete();
//
//        List<Record2<UUID, Long>> objectLinkDegree = fileQueries.fetchObjectLinkingDegreeByObjectId( user0 )
//                .filter( rec -> rec.get( "count", Long.class ) > 0 )
//                .collectList()
//                .block();
//        List<UUID> expectedObjectIds = Stream.of( curLinkedObj, newObj )
//                .sorted( TreeRecordComparators::compareByObjectId )
//                .map( TreeRecord::getObjectId )
//                .toList();
//        // Link degree for cur and new obj should be 1
//        assertEquals( 1L, objectLinkDegree.get( 0 ).get( "count", Long.class ) );
//        assertEquals( 1L, objectLinkDegree.get( 1 ).get( "count", Long.class ) );
//        // cur and new object should be the linked objects
//        assertEquals( expectedObjectIds.get( 0 ), objectLinkDegree.get( 0 ).get( FILE_VIEW.OBJECT_ID ) );
//        assertEquals( expectedObjectIds.get( 1 ), objectLinkDegree.get( 1 ).get( FILE_VIEW.OBJECT_ID ) );
//        // no links for user1
//        StepVerifier.create( fileQueries.fetchFileLinkingDegreeByFileId( user1 ) ).expectNextCount( 0 ).verifyComplete();
//        StepVerifier.create( fileQueries.fetchObjectLinkingDegreeByObjectId( user1 ) ).expectNextCount( 0 ).verifyComplete();
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