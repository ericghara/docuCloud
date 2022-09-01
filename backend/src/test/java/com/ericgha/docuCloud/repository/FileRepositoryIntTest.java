package com.ericgha.docuCloud.repository;

import com.ericgha.docuCloud.converter.FileViewDtoToFileDto;
import com.ericgha.docuCloud.converter.FileViewDtoToTreeJoinFileDto;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.repository.testtool.assertion.TestFileAssertion;
import com.ericgha.docuCloud.repository.testtool.file.RandomFileGenerator;
import com.ericgha.docuCloud.repository.testtool.file.TestFiles;
import com.ericgha.docuCloud.repository.testtool.file.TestFilesFactory;
import com.ericgha.docuCloud.repository.testtool.tree.TestFileTree;
import com.ericgha.docuCloud.repository.testtool.tree.TestFileTreeFactory;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnablePostgresTestContainer;
import com.ericgha.docuCloud.util.comparator.FileViewDtoComparators;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.exception.DataAccessException;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.Tables.FILE_VIEW;
import static com.ericgha.docuCloud.jooq.enums.ObjectType.FILE;
import static com.ericgha.docuCloud.repository.testtool.assertion.OffsetDateTimeAssertion.assertPastDateTimeWithinLast;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EnablePostgresTestContainer
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
            .userId( UUID.fromString( "ffffffff-ffff-ffff-fedc-ba9876543210" ) )
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
    @DisplayName("createFileFor creates the expected file_view record")
    void createFileForCreatesExpectedRecord() {
        String expectedState = """
                fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                fileObj2, fileRes1
                dir0.fileObj3, fileRes2
                """;
        TreeDto objToLink = tree0.getOrigRecord( "dir0.fileObj3" );
        FileDto newFile = FileDto.builder().checksum( "fileRes2" ).size( 2L ).build();
        StepVerifier.create( fileRepository.createFileFor( objToLink, newFile, user0 ) )
                .assertNext( record -> {
                    assertEquals( objToLink.getObjectId(), record.getObjectId() );
                    assertNotNull( record.getFileId() );
                    assertEquals( user0.getUserId(), record.getUserId() );
                    assertPastDateTimeWithinLast( record.getLinkedAt(), Duration.ofSeconds( 1 ) );
                    assertPastDateTimeWithinLast( record.getUploadedAt(), Duration.ofSeconds( 1 ) );
                    assertEquals( newFile.getChecksum(), record.getChecksum() );
                    assertEquals( newFile.getSize(), record.getSize() );
                } )
                .verifyComplete();
        TestFileAssertion.assertRepositoryState( files0, expectedState );
        TestFileAssertion.assertNoChangesFor( files0, FILE_FACTORY_CSV );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName("createFileFor returns error if cannot insert file")
    void createFileForReturnsEmptyIfCannotCreate() {
        TreeDto objToLink = tree1.getOrigRecord( "dir0.fileObj3" ); // notice tree1, not user's file
        FileDto newFile = FileDto.builder().checksum( "fileRes2" ).size( 2L ).build();
        StepVerifier.create( fileRepository.createFileFor( objToLink, newFile, user0 ) )
                .verifyError( DataAccessException.class );
        TestFileAssertion.assertNoChanges( files0 );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName("rmEdge deletes an edge and the linked file when no links after delete returning file_id and true")
    void rmEdgeDeletesEdgeAndFileAndReturnsTrueWhenCreatesOrphan() {
        FileViewDto toDelete = ( files0.getOrigFileViewFor( "fileObj2", "fileRes1" ) );
        FileDto expectedFile = fileViewToFile.convert( toDelete );
        StepVerifier.create( fileRepository.rmEdge( fileViewToTreeJoinFile.convert( toDelete ), user0 ) )
                .assertNext( rec2 -> {
                    assertEquals( toDelete.getFileId(), rec2.get( "file_id" ) );
                    assertEquals( true, rec2.get( "orphan" ) );
                } )
                .verifyComplete();
        String expectedState = """
                fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                # deleted: fileObj2, fileRes1
                """;
        TestFileAssertion.assertRepositoryState( files0, expectedState );
        TestFileAssertion.assertNoChangesFor( files0, expectedState );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName("rmEdge deletes deletes ONLY the edge when file does not become orphan, returning file_id and false")
    void rmEdgeDeletesEdgeAndReturnsFalseWhenNotCreatesOrphan() {
        FileViewDto toDelete = ( files0.getOrigFileViewFor( "fileObj0", "fileRes0" ) );
        FileDto expectedFile = fileViewToFile.convert( toDelete );
        StepVerifier.create( fileRepository.rmEdge( fileViewToTreeJoinFile.convert( toDelete ), user0 ) )
                .assertNext( rec2 -> {
                    assertEquals( toDelete.getFileId(), rec2.get( "file_id" ) );
                    assertEquals( false, rec2.get( "orphan" ) );
                } )
                .verifyComplete();
        String expectedState = """
                # deleted: fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                fileObj2, fileRes1
                """;
        TestFileAssertion.assertRepositoryState( files0, expectedState );
        TestFileAssertion.assertNoChangesFor( files0, expectedState );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName("rmEdge returns empty if no file deleted")
    void rmEdgeReturnsEmptyWhenNoFile() {
        // notice files1, not user0's file
        FileViewDto toDelete = ( files1.getOrigFileViewFor( "fileObj0", "fileRes0" ) );
        StepVerifier.create( fileRepository.rmEdge( fileViewToTreeJoinFile.convert( toDelete ), user0 ) )
                .expectNextCount( 0 )
                .verifyComplete();
        TestFileAssertion.assertNoChanges( files0 );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName("rmEdges from deletes all edges from provided object and returns True for file_id deleted")
    void rmEdgesFromDeletesEdgesLinkedToFileAndRemovesOrphanFiles() {
        files0.insertFileViewRecord( "fileObj2", "fileRes0" );
        /* Current state:
            fileObj0, fileRes0
            dir0.fileObj1, fileRes0
            fileObj2, fileRes0
            fileObj2, fileRes1
         */
        String expectedState = """
                fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                # deleted: fileObj2, fileRes0
                # deleted: fileObj2, fileRes1
                """;
        UUID fileObj2Id = tree0.getOrigRecord( "fileObj2" ).getObjectId();
        UUID fileRes0Id = files0.getOrigFileFor( "fileRes0" ).getFileId();
        UUID fileRes1Id = files0.getOrigFileFor( "fileRes1" ).getFileId();
        // expect fileRes1 to be deleted from files table, and only edge removed for fileRes1
        Map<UUID, Boolean> expected = Map.of( fileRes0Id, false, fileRes1Id, true );
        StepVerifier.create( fileRepository.rmEdgesFrom( fileObj2Id, user0 ) )
                .thenConsumeWhile( rec2 -> {
                    UUID curId = rec2.get( FILE_VIEW.FILE_ID );
                    assertTrue( expected.containsKey( curId ), "unexpected file node" );
                    assertEquals(expected.get(curId), rec2.get("orphan") );
                    return true;
                } )
                .verifyComplete();
        TestFileAssertion.assertRepositoryState( files0, expectedState );
        TestFileAssertion.assertNoChangesFor( files0, expectedState );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName( "cpNewestFile createsEdge between newest file linked to source and destination" )
    void cpNewestFileCreatesExpectedEdge() {
        // newest file
        files0.insertFileViewRecord( "fileObj2", "fileRes0" );
        /* Current state:
            fileObj0, fileRes0
            dir0.fileObj1, fileRes0
            fileObj2, fileRes0
            fileObj2, fileRes1
         */
        String expectedState = """
                fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                fileObj2, fileRes0
                fileObj2, fileRes1
                # new edge
                dir0.fileObj3, fileRes0
                """;
        UUID sourceId = tree0.getOrigRecord("fileObj2").getObjectId();
        UUID destId = tree0.getOrigRecord("dir0.fileObj3").getObjectId();
        StepVerifier.create(fileRepository.cpNewestFile( sourceId, destId, user0 ) )
                .expectNext( 1L )
                .verifyComplete();
        TestFileAssertion.assertRepositoryState( files0, expectedState );
        TestFileAssertion.assertNoChangesFor( files0, FILE_FACTORY_CSV );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName( "cpAllFiles creates edges connecting all source files to all dest files" )
    void cpAllFilesCreatesExpectedEdges() {
        files0.insertFileViewRecord( "fileObj0", "fileRes2" );
        String expectedState = """
                fileObj0, fileRes0
                fileObj0, fileRes2
                dir0.fileObj1, fileRes0
                fileObj2, fileRes1
                dir0.fileObj3, fileRes0
                dir0.fileObj3, fileRes2
                """;
        UUID sourceId = tree0.getOrigRecord("fileObj0").getObjectId();
        UUID destId = tree0.getOrigRecord("dir0.fileObj3").getObjectId();
        StepVerifier.create( fileRepository.cpAllFiles( sourceId, destId, user0, dsl) )
                .expectNext( 2L )
                .verifyComplete();
        TestFileAssertion.assertRepositoryState( files0, expectedState );
        TestFileAssertion.assertNoChangesFor( files0, FILE_FACTORY_CSV );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName( "lsNewestFilesFor returns newest file linked to fileObject when limit 1" )
    void lsNewestFilesForReturnsNewestFileWhenLimit1() {
        TreeDto source = tree0.getOrigRecord( "fileObj2" );
        FileViewDto expected = files0.insertFileViewRecord( source.getPathStr(), "fileRes3" );
        /*  current state:
            fileObj0, fileRes0
            dir0.fileObj1, fileRes0
            fileObj2, fileRes1
            fileObj2, fileRes3 <- newest
         */
        StepVerifier.create( fileRepository.lsNewestFilesFor( source, 1, user0 ) )
                .expectNext( expected ).verifyComplete();
    }

    @Test
    @DisplayName( "lsFiles for returns expected records descending linked_at, uploaded_at, uuid order" )
    void lsFilesForReturnsExpectedFilesInExpectedOrder() {
        String addAdjacencies = """
                dir0.fileObj3, fileRes2
                dir0.fileObj3, fileRes3
                dir0.fileObj3, fileRes4
                dir0.fileObj3, fileRes5
                dir0.fileObj3, fileRes6
                dir0.fileObj3, fileRes7
                """;
        files0.insertFileViewRecords( addAdjacencies );
        var comp = FileViewDtoComparators.compareByLinkedAtUploadedAtFileId().reversed();
        // these are now in the sort order used for pagnation
        List<FileViewDto> allInOrder = files0.getOrigFileViewsFor( "dir0.fileObj3" )
                .stream()
                .sorted( comp )
                .toList();
        // go through allInOrder expecting next NUM_RECORDS AFTER that record
        for (int i = 0; i < allInOrder.size() ; i++) {
            int NUM_RECORDS = 4;
            // add 1 to i because we expect records after current (we are seeking)
            int expectedLast = Math.min( i + 1 + NUM_RECORDS, allInOrder.size() );
            List<FileViewDto> expected = allInOrder.subList( i+1, expectedLast );
            StepVerifier.create( fileRepository.lsNextFilesFor( allInOrder.get( i ), NUM_RECORDS, user0 ) )
                    .expectNextSequence( expected )
                    .verifyComplete();
        }
    }

    @Test
    @DisplayName( "countFilesFor returns number of fileResources linked to provided fileObject" )
    void countFilesForCountsFileResLinkedToFileObj() {
        TreeDto source = tree0.getOrigRecord( "fileObj0" );
        long expectedCnt = files0.getOrigFileViewsFor( source.getPathStr() ).size();
        StepVerifier.create( fileRepository.countFilesFor( source, user0 ) )
                .expectNext( expectedCnt )
                .verifyComplete();
    }

    @Test
    @DisplayName( "deleteMe" )
    // todo delete me
    void deleteMe(@Autowired TreeRepository treeRepository) {
        files0.insertFileViewRecord( "fileObj2", "fileRes0" );
        /* Current state:
            fileObj0, fileRes0
            dir0.fileObj1, fileRes0
            fileObj2, fileRes0
            fileObj2, fileRes1
         */
        String expectedState = """
                fileObj0, fileRes0
                dir0.fileObj1, fileRes0
                # deleted: fileObj2, fileRes0
                # deleted: fileObj2, fileRes1
                """;
        TreeDto fileObj2 = tree0.getOrigRecord( "fileObj2" );
        UUID fileRes0Id = files0.getOrigFileFor( "fileRes0" ).getFileId();
        UUID fileRes1Id = files0.getOrigFileFor( "fileRes1" ).getFileId();
        Flux<Record2<UUID,Boolean>> deleted = Flux.from(dsl.transactionPublisher(trx ->
                treeRepository.rmNormal(fileObj2, user0 )
                        .flatMapMany( treeDto ->  fileRepository.rmEdgesFrom( treeDto.getObjectId(), user0 ) ) ) );
        Map<UUID, Boolean> expected = Map.of( fileRes0Id, false, fileRes1Id, true );
        assertNotNull( tree0.fetchCurRecord( fileObj2 ) );
        StepVerifier.create( deleted )
                .thenConsumeWhile( rec2 -> {
                    UUID curId = rec2.get( FILE_VIEW.FILE_ID );
                    assertTrue( expected.containsKey( curId ), "unexpected file node" );
                    assertEquals(expected.get(curId), rec2.get("orphan") );
                    return true;
                } )
                .verifyComplete();
        assertNull( tree0.fetchCurRecord( fileObj2 ) );
        TestFileAssertion.assertRepositoryState( files0, expectedState );
        TestFileAssertion.assertNoChangesFor( files0, expectedState );
        TestFileAssertion.assertNoChanges( files1 );
    }

    @Test
    @DisplayName( "deleteMe 2" )
    // todo delete me
    public void deleteMe2(@Autowired TreeRepository treeRepository,
                          @Autowired TransactionalOperator transactionalOperator) {
        RandomFileGenerator randomFileGenerator = new RandomFileGenerator();
        // also remember to delete cfi autowire
        // randomFileGenerator
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf("file100") )
                .objectType( FILE )
                .build();
        FileDto file = randomFileGenerator.generate().fileDto();
        var insert0 =
                treeRepository.create(fileObj0, user0 )
                        .flatMap(created -> fileRepository.createFileFor( created, file, user0 ) );
//        StepVerifier.create( insert0 ).expectNextCount( 1 ).verifyComplete();
//        var insert1 = ConnectionFactoryUtils.getConnection( cfi ).flatMap( cnxn -> {
//            var ctxt = DSL.using(cnxn);
//            return treeRepository.create(fileObj1, user0, ctxt).flatMap(created -> fileRepository.createFileFor( created, file, user0, ctxt ) );
//        });
        StepVerifier.create( insert0 ).expectNextCount( 1 ).verifyComplete();
        System.out.println("ssdkflsdf");
    }

}