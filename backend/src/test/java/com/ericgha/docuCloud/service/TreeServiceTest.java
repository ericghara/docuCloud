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
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static org.jooq.impl.DSL.now;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        TreeTestQueries treeTestQueries =  new TreeTestQueries( dsl );
        treeFactory = new TestFileTreeFactory( treeTestQueries );
        treeRecordComparators = new TreeRecordComparators();
    }

    CloudUser user0 = CloudUser.builder()
            .userId( "1234567-89ab-cdef-fedc-ba9876543210" )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    CloudUser user1 = CloudUser.builder()
            .userId( "fffffff-ffff-ffff-fedc-ba9876543210" )
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
        TestFileTree tree1 = treeFactory.constructDefault(user1 );

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
    @DisplayName( "Creates the expected record" )
    void create() {
        TreeRecord record = new TreeRecord( null, ObjectType.ROOT,
                Ltree.valueOf( "" ), null, null );
        LocalDateTime currentTime = Mono.from(dsl.select(now()) ).block().component1().toLocalDateTime();
        // returns correct record
        TreeRecord newRecord = treeService.create( record, user0 ).block();
        assertEquals( 36, newRecord.getObjectId().length() );
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
    @DisplayName( "Test of file move" )
    void mvFileMovesAFile() {
        TestFileTree tree0 = treeFactory.constructDefault(user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord beforeMove = tree0.getOrigRecord( "dir0.dir3.file1" );
        Ltree newPath = Ltree.valueOf( "dir0.dir1.file1" );
        Long rowsChanged = treeService.mvFile( newPath, beforeMove, user0 ).block();
        assertEquals(1, rowsChanged, "Unexpected number of rows changed by move");
        TreeRecord afterMove = tree0.fetchCurRecord( beforeMove );
        assertEquals(beforeMove.getObjectType(), afterMove.getObjectType() );
        assertEquals( beforeMove.getCreatedAt(), afterMove.getCreatedAt() );
        assertEquals( beforeMove.getUserId(), afterMove.getUserId() );
        assertEquals( newPath, afterMove.getPath() );
        tree1.assertNoChanges();
        tree0.assertNoChangesFor( "", "dir0", "file0",
                "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3" );
    }
    @Test
    @DisplayName( "mvFile does not move a dir spoofed as file" )
    void mvFileDoesNotMoveDir() {
        TestFileTree tree0 = treeFactory.constructDefault(user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord spoofedRecord = tree0.getOrigRecord( "dir0.dir1.dir2" );
        spoofedRecord.setObjectType( ObjectType.FILE ); // force db to handle objectType check;
        Ltree newPath = Ltree.valueOf("dir0.dir3.dir2");
        Long rowsChanged = treeService.mvFile( newPath, spoofedRecord, user0).block();

        assertEquals(0, rowsChanged, "Unexpected number of rows changed by move");
        tree0.assertNoChanges();
        tree1.assertNoChanges();
    }



    @Test
    @DisplayName( "mvDir correctly renames target dir and updates child dirs" )
    void mvDirRenamesDirAndChildren() {
        TestFileTree tree0 = treeFactory.constructDefault(user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord curDir = tree0.getOrigRecord( "dir0" );
        Ltree newName = Ltree.valueOf("dir100");

        Mono<Long> rename = treeService.mvDir( newName,  curDir, user0 );
        StepVerifier.create( rename )
                .expectNext( 5L )
                .verifyComplete();

        for (String path : List.of("dir0", "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1") ) {
            String expectedNewPath = path.replace("dir0", "dir100");
            TreeRecord expectedNewRecord = tree0.getOrigRecord( path );
            expectedNewRecord.setPath( Ltree.valueOf( expectedNewPath ) );
            assertEquals(expectedNewRecord, tree0.fetchCurRecord( tree0.getOrigRecord( path ) ) );
        }
        tree0.assertNoChangesFor( "", "file0" );
        tree1.assertNoChanges();
    }

    @Test
    @DisplayName( "Rename correctly renames target dir and updates child dirs" )
    void renameDirWithChildren() {
        TestFileTree tree0 = treeFactory.constructDefault(user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord curDir = tree0.getOrigRecord( "dir0" );
        Ltree newName = Ltree.valueOf("dir100");

        Mono<Long> rename = treeService.mvDir( newName,  curDir, user0 );
        StepVerifier.create( rename )
                .expectNext( 5L )
                .verifyComplete();

        for (String path : List.of("dir0", "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1") ) {
            String expectedNewPath = path.replace("dir0", "dir100");
            TreeRecord expectedNewRecord = tree0.getOrigRecord( path );
            expectedNewRecord.setPath( Ltree.valueOf( expectedNewPath ) );
            assertEquals(expectedNewRecord, tree0.fetchCurRecord( tree0.getOrigRecord( path ) ) );
        }
        tree0.assertNoChangesFor( "", "file0" );
        tree1.assertNoChanges();
    }


    @Test
    @DisplayName( "mvDir moves a dir and children to another branch" )
    void mvDirMovesDirAndChildren() {
        TestFileTree tree0 = treeFactory.constructDefault(user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord origRecord = tree0.getOrigRecord( "dir0.dir3" );
        Ltree movePath = Ltree.valueOf("dir0.dir1.dir2.dir3");

        Mono<Long> move = treeService.mvDir( movePath, origRecord, user0 );

        StepVerifier.create( move )
                .expectNext( 2L )
                .verifyComplete();

        for (String path : List.of("dir0.dir3", "dir0.dir3.file1") ) {
            String expectedNewPath = path.replace("dir0.dir3", "dir0.dir1.dir2.dir3");
            TreeRecord expectedNewRecord = tree0.getOrigRecord( path );
            expectedNewRecord.setPath( Ltree.valueOf( expectedNewPath ) );
            assertEquals(expectedNewRecord, tree0.fetchCurRecord( tree0.getOrigRecord( path ) ) );
        }
        tree0.assertNoChangesFor( "", "dir0", "file0", "dir0.dir1", "dir0.dir1.dir2" );
        tree1.assertNoChanges();
    }

    @Test
    @DisplayName( "mvDir doesn't move a spoofed file" )
    void mvDirDoesNotMoveFileSpoofedAsDir() {
        TestFileTree tree0 = treeFactory.constructDefault(user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord origRecord = tree0.getOrigRecord( "dir0.dir3.file1" );
        origRecord.setObjectType( ObjectType.DIR ); // spoofed object type;
        Ltree movePath = Ltree.valueOf("file1");

        Mono<Long> move = treeService.mvDir( movePath, origRecord, user0 );

        StepVerifier.create( move )
                .expectNext( 0L )
                .verifyComplete();

        tree0.assertNoChanges();
        tree1.assertNoChanges();
    }

    @Test
    @DisplayName("cpDir copies a dir and its children")
    void cpDirCopiesDirAndChildern() {
        TestFileTree tree0 = treeFactory.constructDefault(user0 );
        // a "challenge" tree with same paths but different user
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        TreeRecord source = tree0.getOrigRecord("dir0");
        Ltree destination = Ltree.valueOf( "dir100" );

        Flux<String> copy = treeService.cpDir(destination, source, user0)
                .sort( treeRecordComparators::compareByLtree ).map( r -> r.getPath().data() );
        List<String> expectedPaths = List.of("dir100", "dir100.dir1", "dir100.dir1.dir2", "dir100.dir3", "dir100.dir3.file1");

        StepVerifier.create(copy).expectNextSequence( expectedPaths ).verifyComplete();

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