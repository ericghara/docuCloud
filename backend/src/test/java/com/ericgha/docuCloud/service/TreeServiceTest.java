package com.ericgha.docuCloud.service;


import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import com.ericgha.docuCloud.service.testutil.TestFileTree;
import com.ericgha.docuCloud.service.testutil.TestFileTreeFactory;
import com.ericgha.docuCloud.service.testutil.TreeRecordComparators;
import com.ericgha.docuCloud.service.testutil.TreeTestQueries;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnabledPostgresTestContainer;
import jakarta.annotation.PostConstruct;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Record6;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.BeforeEach;
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
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static com.ericgha.docuCloud.service.testutil.assertion.TestFileTreeAssertions.assertNoChanges;
import static com.ericgha.docuCloud.service.testutil.assertion.TestFileTreeAssertions.assertNoChangesFor;
import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EnabledPostgresTestContainer
class TreeServiceTest {

    @Autowired
    TreeService treeService;

    TestFileTreeFactory treeFactory;
    TreeRecordComparators treeRecordComparators;

    @Autowired
    DSLContext dsl;

    @PostConstruct
    void postConstruct() {
        TreeTestQueries treeTestQueries = new TreeTestQueries( dsl );
        treeFactory = new TestFileTreeFactory( treeTestQueries );
    }

    CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    CloudUser user1 = CloudUser.builder()
            .userId( UUID.fromString( "fffffff-ffff-ffff-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    @BeforeEach
    void before() throws URISyntaxException, IOException {
        // testcontainers cannot reliably run complex init scrips (ie with declared functions)
        // testcontainers/testcontainers-java issue #2814
        Path input = Paths.get( this.getClass().getClassLoader().getResource( "tests-schema.sql" ).toURI() );
        String sql = Files.readString( input );
        Mono.from( dsl.query( sql ) ).block();
    }

    @Test
    @DisplayName("select path by objectId")
    void selectByObjectId() {
        TestFileTree testFileTree = treeFactory.constructDefault( user0 );
        TreeRecord createdRecord = testFileTree.getOrigRecord( "dir0" );

        int expectedLevel = 1;
        StepVerifier.create( treeService.selectDirPathAndLevel( createdRecord, user0 ) )
                .assertNext( res -> {
                    assertEquals( createdRecord.getPath(), res.getValue( "path" ) );
                    assertEquals( expectedLevel, res.getValue( "level" ) );
                } )
                .verifyComplete();
    }

    @Test
    @DisplayName("Generate move paths for self and children")
    void selectMovePaths() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord toMove = tree0.getOrigRecord( "dir0" );
        String newBase = "newBase";

        Flux<String> foundPaths = Flux.from( treeService.createMovePath( Ltree.valueOf( newBase ), toMove, user0 ) )
                .map( newRecord -> newRecord.get( "path", Ltree.class ) )
                .map( Ltree::data )
                .sort();

        StepVerifier.create( foundPaths )
                .expectNext(
                        "newBase",
                        "newBase.dir1",
                        "newBase.dir1.dir2",
                        "newBase.dir3",
                        "newBase.dir3.file1"
                )
                .verifyComplete();
    }

    @Test
    @DisplayName("Creates the expected record")
    void create() {
        TreeRecord record = new TreeRecord( null, ObjectType.ROOT,
                Ltree.valueOf( "" ), null, null );
        OffsetDateTime currentTime = Mono.from( dsl.select( currentOffsetDateTime() ) ).block().component1();
        // returns correct record
        TreeRecord newRecord = treeService.create( record, user0 ).block();
        assertNotNull( newRecord.getObjectId() );
        assertEquals( record.getObjectType(), newRecord.getObjectType() );
        assertEquals( record.getPath(), newRecord.getPath() );
        assertEquals( user0.getUserId(), newRecord.getUserId() );
        assertTrue( 1 > ChronoUnit.SECONDS.between( newRecord.getCreatedAt(), currentTime ) );

        // inserts
        StepVerifier.create( Mono.from( dsl.selectCount().from( TREE )
                        .where( TREE.OBJECT_ID.eq( newRecord.getObjectId() ) ) ).map( count -> count.get( 0 ) ) )
                .expectNext( 1 ).
                verifyComplete();
    }

    @Test
    @DisplayName("Test of file move")
    void mvFileMovesAFile() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord beforeMove = tree0.getOrigRecord( "dir0.dir3.file1" );
        Ltree newPath = Ltree.valueOf( "dir0.dir1.file1" );
        Long rowsChanged = treeService.mvFile( newPath, beforeMove, user0 ).block();
        assertEquals( 1, rowsChanged, "Unexpected number of rows changed by move" );
        TreeRecord afterMove = tree0.fetchCurRecord( beforeMove );
        assertEquals( beforeMove.getObjectType(), afterMove.getObjectType() );
        assertEquals( beforeMove.getCreatedAt(), afterMove.getCreatedAt() );
        assertEquals( beforeMove.getUserId(), afterMove.getUserId() );
        assertEquals( newPath, afterMove.getPath() );
        assertNoChanges( tree1 );
        assertNoChangesFor( tree0, "", "dir0", "file0",
                "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3" );
    }

    @Test
    @DisplayName("mvFile does not move a dir spoofed as file")
    void mvFileDoesNotMoveDir() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord spoofedRecord = tree0.getOrigRecord( "dir0.dir1.dir2" );
        spoofedRecord.setObjectType( ObjectType.FILE ); // force db to handle objectType check;
        Ltree newPath = Ltree.valueOf( "dir0.dir3.dir2" );
        Long rowsChanged = treeService.mvFile( newPath, spoofedRecord, user0 ).block();

        assertEquals( 0, rowsChanged, "Unexpected number of rows changed by move" );
        assertNoChanges( tree0 );
        assertNoChanges( tree1 );
    }


    @Test
    @DisplayName("mvDir correctly renames target dir and updates child dirs")
    void mvDirRenamesDirAndChildren() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord curDir = tree0.getOrigRecord( "dir0" );
        Ltree newName = Ltree.valueOf( "dir100" );

        Mono<Long> rename = treeService.mvDir( newName, curDir, user0 );
        StepVerifier.create( rename )
                .expectNext( 5L )
                .verifyComplete();

        for (String path : List.of( "dir0", "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1" )) {
            String expectedNewPath = path.replace( "dir0", "dir100" );
            TreeRecord expectedNewRecord = tree0.getOrigRecord( path );
            expectedNewRecord.setPath( Ltree.valueOf( expectedNewPath ) );
            assertEquals( expectedNewRecord, tree0.fetchCurRecord( tree0.getOrigRecord( path ) ) );
        }
        assertNoChangesFor( tree0, "", "file0" );
        assertNoChanges( tree1 );
    }

    @Test
    @DisplayName("Rename correctly renames target dir and updates child dirs")
    void renameDirWithChildren() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord curDir = tree0.getOrigRecord( "dir0" );
        Ltree newName = Ltree.valueOf( "dir100" );

        Mono<Long> rename = treeService.mvDir( newName, curDir, user0 );
        StepVerifier.create( rename )
                .expectNext( 5L )
                .verifyComplete();

        for (String path : List.of( "dir0", "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1" )) {
            String expectedNewPath = path.replace( "dir0", "dir100" );
            TreeRecord expectedNewRecord = tree0.getOrigRecord( path );
            expectedNewRecord.setPath( Ltree.valueOf( expectedNewPath ) );
            assertEquals( expectedNewRecord, tree0.fetchCurRecord( tree0.getOrigRecord( path ) ) );
        }
        assertNoChangesFor( tree0, "", "file0" );
        assertNoChanges( tree1 );
    }


    @Test
    @DisplayName("mvDir moves a dir and children to another branch")
    void mvDirMovesDirAndChildren() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord origRecord = tree0.getOrigRecord( "dir0.dir3" );
        Ltree movePath = Ltree.valueOf( "dir0.dir1.dir2.dir3" );

        Mono<Long> move = treeService.mvDir( movePath, origRecord, user0 );

        StepVerifier.create( move )
                .expectNext( 2L )
                .verifyComplete();

        for (String path : List.of( "dir0.dir3", "dir0.dir3.file1" )) {
            String expectedNewPath = path.replace( "dir0.dir3", "dir0.dir1.dir2.dir3" );
            TreeRecord expectedNewRecord = tree0.getOrigRecord( path );
            expectedNewRecord.setPath( Ltree.valueOf( expectedNewPath ) );
            assertEquals( expectedNewRecord, tree0.fetchCurRecord( tree0.getOrigRecord( path ) ) );
        }
        assertNoChangesFor( tree0, "", "dir0", "file0", "dir0.dir1", "dir0.dir1.dir2" );
        assertNoChanges( tree1 );
    }

    @Test
    @DisplayName("mvDir doesn't move a spoofed file")
    void mvDirDoesNotMoveFileSpoofedAsDir() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord origRecord = tree0.getOrigRecord( "dir0.dir3.file1" );
        origRecord.setObjectType( ObjectType.DIR ); // spoofed object type;
        Ltree movePath = Ltree.valueOf( "file1" );

        Mono<Long> move = treeService.mvDir( movePath, origRecord, user0 );

        StepVerifier.create( move )
                .expectNext( 0L )
                .verifyComplete();

        assertNoChanges( tree0 );
        assertNoChanges( tree1 );
    }

    @Test
    @DisplayName("fetchDirCopyRecords generates new object_id, path and created_at")
    void fetchDirCopyRecordsGeneratesExpectedRecords() {
        // moves dir0.dir3 => dir0.dir2.dir2.dir3
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TreeRecord source = tree0.getOrigRecord( "dir0.dir3" );
        Ltree destination = Ltree.valueOf( "dir0.dir1.dir2.dir3" );
        // raw type for brevity here...
        Map<String, ? extends Record6> copyRecordsByPath = Flux.from( treeService.fetchDirCopyRecords( destination, source, user0 ) )
                .collectMap( r -> r.get( TREE.PATH ).data() ).block();

        TreeRecord origDir3 = tree0.getOrigRecord( "dir0.dir3" );
        var curDir3 = copyRecordsByPath.get( destination.data() );
        assertNotEquals( origDir3.getObjectId(), curDir3.get( TREE.OBJECT_ID ) );
        assertEquals( origDir3.getObjectType(), curDir3.get( TREE.OBJECT_TYPE ) );
        assertEquals( origDir3.getUserId(), curDir3.get( TREE.USER_ID ) );
        assertTrue( origDir3.get( TREE.CREATED_AT ).isBefore( curDir3.get( TREE.CREATED_AT ) ) );

        TreeRecord origFile1 = tree0.getOrigRecord( "dir0.dir3.file1" );
        var curFile1 = copyRecordsByPath.get( destination.data() + ".file1" );
        assertNotEquals( origFile1.getObjectId(), curFile1.get( TREE.OBJECT_ID ) );
        assertEquals( origFile1.getObjectType(), curFile1.get( TREE.OBJECT_TYPE ) );
        assertEquals( origFile1.getUserId(), curFile1.get( TREE.USER_ID ) );
        assertTrue( origFile1.get( TREE.CREATED_AT ).isBefore( curFile1.get( TREE.CREATED_AT ) ) );

        // this is just the creation of records *to* insert, so not mutating
        assertNoChanges( tree0 );
    }
    
    @Test
    @DisplayName("fetchDirCopyRecords does not return records when ObjectType is spoofed")
    void fetchDirCopyRecordsDoesNotFetchSpoofedObjectType() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TreeRecord source = tree0.getOrigRecord( "dir0.dir3.file1" );
        source.setObjectType( ObjectType.DIR );
        Ltree destination = Ltree.valueOf( "dir0.dir1.dir2.dir3" );
        StepVerifier.create(treeService.fetchDirCopyRecords( destination, source, user0 ) )
                .expectNextCount( 0 ).verifyComplete();
    }

    @Test
    @DisplayName("fetchDirCopyRecords does not fetch when incorrect user_id")
    void fetchDirCopyRecordsDoesNotFetchWhenIncorrectUserId() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        TreeRecord source = tree0.getOrigRecord( "dir0.dir3" );
        Ltree destination = Ltree.valueOf( "dir0.dir1.dir2.dir3" );
        StepVerifier.create(treeService.fetchDirCopyRecords( destination, source, user1 ) )
                .expectNextCount( 0 ).verifyComplete();
    }

    @Test
    @DisplayName("cpDir copies a dir and its children")
    void cpDirCopiesDirAndChildern() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        String srcPathStr = "dir0";
        String destPathSr = "dir100";
        TreeRecord source = tree0.getOrigRecord( srcPathStr );
        Ltree destination = Ltree.valueOf( destPathSr );

        Set<String> expectedPaths = Set.of( "dir100", "dir100.dir1", "dir100.dir1.dir2", "dir100.dir3", "dir100.dir3.file1" );

        List<Record3<UUID, UUID, ObjectType>> copy = treeService.cpDir( destination, source, user0 ).collectList().block();

        for (Record3<UUID, UUID, ObjectType> record : copy) {
            TreeRecord srcRec = tree0.fetchCurRecord( record.get( "source_id", UUID.class ) );
            TreeRecord cpRec = tree0.fetchCurRecord( record.get( "destination_id", UUID.class ) );
            String destFromSrc = srcRec.getPath().data().replace( srcPathStr, destPathSr );
            assertEquals( destFromSrc, cpRec.getPath().data() );
            assertTrue( record.get( "object_type" ) == srcRec.getObjectType()
                    && srcRec.getObjectType() == cpRec.getObjectType() );
        }

        Map<Boolean, List<String>> recordsByNew = tree0.fetchAllUserObjects()
                .stream().map( TreeRecord::getPath )
                .map( Ltree::data )
                .collect( Collectors.partitioningBy( expectedPaths::contains ) );

        assertNoChangesFor( tree0, recordsByNew.get( false ).toArray( new String[0] ) );
        assertTrue( expectedPaths.containsAll( recordsByNew.get( true ) ) );
        assertEquals( expectedPaths.size(), recordsByNew.get( true ).size() );
        assertNoChanges( tree1 );
    }

    @Test
    @DisplayName("fetch CopyFileRecords produces the correct record")
    void fetchCopyFileRecordsProducesCorrectRecord() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        String srcStr = "dir0.dir3.file1";
        String destStr = "dir0.file100";
        TreeRecord srcRecord = tree0.getOrigRecord( srcStr );
        var record = Mono.from(
                treeService.fetchFileCopyRecords( Ltree.valueOf( destStr ), srcRecord, user0 ) )
                .block();

        assertEquals( srcRecord.get(TREE.OBJECT_ID), record.get("source_id") );
        assertNotEquals( srcRecord.get(TREE.OBJECT_ID), record.get(TREE.OBJECT_ID) );
        assertEquals( ObjectType.FILE, record.get(TREE.OBJECT_TYPE) );
        assertEquals( record.get(TREE.PATH), Ltree.valueOf(destStr) );
        assertEquals( record.get(TREE.USER_ID), user0.getUserId() );
        assertTrue( record.get(TREE.CREATED_AT).isAfter( srcRecord.get(TREE.CREATED_AT) ) );
    }

    @Test
    @DisplayName("fetch CopyFileRecords does not fetch another users records")
    void fetchCopyFileRecordsDoesNotFetchAnotherUsersRecords() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        String srcStr = "dir0.dir3.file1";
        String destStr = "dir0.file100";
        TreeRecord srcRecord = tree0.getOrigRecord( srcStr );
        StepVerifier.create( treeService.fetchFileCopyRecords( Ltree.valueOf( destStr ), srcRecord, user1 ) )
                .expectNextCount( 0 )
                .verifyComplete();
    }

    @Test
    @DisplayName("cpFile returns Mono.empty() when ObjectType is spoofed")
    void cpFileReturnsMonoEmptyWhenIncorrectObjectType() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        String srcStr = "dir0.dir1";
        String destStr = "dir0.file100";
        TreeRecord srcRecord = tree0.getOrigRecord( srcStr );
        srcRecord.setObjectType( ObjectType.FILE ); // spoofed object type
        var record = Mono.from(
                        treeService.fetchFileCopyRecords( Ltree.valueOf( destStr ), srcRecord, user0 ) );
        StepVerifier.create( record ).expectNextCount( 0 ).verifyComplete();
    }


//    @Test
//    @DisplayName("Config check: UNIQUE path")
//    void uniqueConstraintPath() {
//        Flux<TreeRecord> records = Flux.concat( treeService.create( "a", ObjectType.ROOT ),
//                treeService.create( "a", ObjectType.ROOT ) );
//        StepVerifier.create( records ).expectNextCount( 1 )
//                .expectError( DataAccessException.class )
//                .verify();
//    }
//
//    @Test
//    @DisplayName("Insert trigger: ROOT level must equal 1")
//    void rootInsTrigger() {
//        Mono<TreeRecord> level0 = Mono.from( treeService.create( "", ObjectType.ROOT ) );
//        StepVerifier.create( level0 ).verifyError( DataAccessException.class );
//        Mono<TreeRecord> level1 = Mono.from( treeService.create( "a", ObjectType.ROOT ) );
//        StepVerifier.create( level1 ).expectNextCount( 1 ).verifyComplete();
//        Mono<TreeRecord> level2 = Mono.from( treeService.create( "a.b", ObjectType.ROOT ) );
//        StepVerifier.create( level2 ).verifyError( DataAccessException.class );
//    }
//
//    @Test
//    @DisplayName("Insert trigger: DIR or FILE prevent path level < 2")
//    void dirFileInsTriggerLevel() {
//        Mono<TreeRecord> level1Dir = Mono.from( treeService.create( "rootlevel", ObjectType.DIR ) );
//        StepVerifier.create( level1Dir ).verifyError( DataAccessException.class );
//
//        Mono<TreeRecord> level1File = Mono.from( treeService.create( "rootlevel", ObjectType.FILE ) );
//        StepVerifier.create( level1File ).verifyError( DataAccessException.class );
//    }
//
//    @Test
//    @DisplayName("Insert trigger: DIR and File orphan constraints")
//    void DirFileInsTriggerOrphan() {
//        // without parent
//        Mono<TreeRecord> orphanLevel2Dir = Mono.from( treeService.create( "rootlevel.folder", ObjectType.DIR ) );
//        StepVerifier.create( orphanLevel2Dir ).verifyError( DataAccessException.class );
//
//        Mono<TreeRecord> orphanLevel2File = Mono.from( treeService.create( "rootlevel.file", ObjectType.FILE ) );
//        StepVerifier.create( orphanLevel2File ).verifyError( DataAccessException.class );
//
//        // now add the parent
//        Mono.from( treeService.create( "rootlevel", ObjectType.ROOT ) ).block();
//
//        Mono<TreeRecord> parentedLevel2Dir = Mono.from( treeService.create( "rootlevel.folder", ObjectType.DIR ) );
//        StepVerifier.create( parentedLevel2Dir ).expectNextCount( 1 ).verifyComplete();
//
//        Mono<TreeRecord> parentedLevel2File = Mono.from( treeService.create( "rootlevel.file", ObjectType.FILE ) );
//        StepVerifier.create( parentedLevel2File ).expectNextCount( 1 ).verifyComplete();
//    }
//
//    @Test
//    @DisplayName("Update leaves_no_orphans trigger")
//    void updateLeavesNoOrphans() {
//        String rootId = treeService.create( "root", ObjectType.ROOT ).block().getObjectId();
//        String dirId = treeService.create( "root.dir", ObjectType.DIR ).block().getObjectId();
//        String subDirId = treeService.create( "root.dir.subDir", ObjectType.DIR ).block().getObjectId();
//
//        Mono<TreeRecord> rootFail = treeService.updatePath( rootId, "newRoot" );
//        StepVerifier.create( rootFail ).verifyError( DataAccessException.class );
//
//        Mono<TreeRecord> dirFail = treeService.updatePath( dirId, "root.newDir" );
//        StepVerifier.create( dirFail ).verifyError( DataAccessException.class );
//    }
//
//    @Test
//    void select() {
//        TreeRecord record = treeService.select( "user0" ).block();
//        System.out.println( record );
//    }
//
//    @Test
//    void fetchPath() {
//    }
//
//    @Test
//    void selectChildren() {
//        List<TreeRecord> family = List.of(
//                TreeRecordFactory.createFromPathStr( "r0", "r0", ObjectType.ROOT ),
//                TreeRecordFactory.createFromPathStr( "r0.d1", "r0.d1", ObjectType.DIR ),
//                TreeRecordFactory.createFromPathStr( "r0.d2", "r0.d2", ObjectType.DIR ),
//                TreeRecordFactory.createFromPathStr( "r0.d1.f1", "r0.d1.f1", ObjectType.FILE ) );
//        List<TreeRecord> dummies = List.of(
//                TreeRecordFactory.createFromPathStr( "r1", "r1", ObjectType.ROOT ),
//                TreeRecordFactory.createFromPathStr( "r1.d1", "r1.d1", ObjectType.DIR ),
//                TreeRecordFactory.createFromPathStr( "r1.d2", "r1.d2", ObjectType.DIR ),
//                TreeRecordFactory.createFromPathStr( "r1.d1.f1", "r1.d1.f1", ObjectType.FILE )
//        );
//        Flux.fromStream( Stream.concat( family.stream(), dummies.stream() ) )
//                .flatMapSequential( treeService::create, 1 ) // sequential required for concurrency of 1, but...
//                .blockLast();
//        List<TreeRecord> expected = family.subList( 1, family.size() );
//        Flux<TreeRecord> found = treeService
//                .selectChildren( Ltree.valueOf( "r0" ) );
//        StepVerifier.create( found ).expectNextSequence( expected ).verifyComplete();
//    }
//
//    @Test
//    void selectChildrenAndParent() {
//        List<TreeRecord> expected = List.of(
//                TreeRecordFactory.createFromPathStr( "r0", "r0", ObjectType.ROOT ),
//                TreeRecordFactory.createFromPathStr( "r0.d1", "r0.d1", ObjectType.DIR ),
//                TreeRecordFactory.createFromPathStr( "r0.d2", "r0.d2", ObjectType.DIR ),
//                TreeRecordFactory.createFromPathStr( "r0.d1.f1", "r0.d1.f1", ObjectType.FILE ) );
//        List<TreeRecord> dummies = List.of(
//                TreeRecordFactory.createFromPathStr( "r1", "r1", ObjectType.ROOT ),
//                TreeRecordFactory.createFromPathStr( "r1.d1", "r1.d1", ObjectType.DIR ),
//                TreeRecordFactory.createFromPathStr( "r1.d2", "r1.d2", ObjectType.DIR ),
//                TreeRecordFactory.createFromPathStr( "r1.d1.f1", "r1.d1.f1", ObjectType.FILE )
//        );
//        Flux.fromStream( Stream.concat( expected.stream(), dummies.stream() ) )
//                .flatMapSequential( treeService::create, 1 ) // sequential required for concurrency of 1, but...
//                .blockLast();
//        Flux<TreeRecord> found = treeService
//                .selectChildrenAndParent( Ltree.valueOf( "r0" ) );
//        StepVerifier.create( found ).expectNextSequence( expected ).verifyComplete();
//    }
}