package com.ericgha.docuCloud.repository;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTree;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTreeFactory;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnabledPostgresTestContainer;
import com.ericgha.docuCloud.util.comparators.TreeDtoComparators;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Record6;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static com.ericgha.docuCloud.repository.testutil.assertion.TestFileTreeAssertion.assertNoChanges;
import static com.ericgha.docuCloud.repository.testutil.assertion.TestFileTreeAssertion.assertNoChangesFor;
import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EnabledPostgresTestContainer
class TreeRepositoryTest {

    @Autowired
    TreeRepository treeRepository;

    @Autowired
    TestFileTreeFactory treeFactory;

    @Autowired
    DSLContext dsl;

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
        TreeDto createdRecord = testFileTree.getOrigRecord( "dir0" );

        int expectedLevel = 1;
        StepVerifier.create( treeRepository.selectDirPathAndLevel( createdRecord, user0 ) )
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

        TreeDto toMove = tree0.getOrigRecord( "dir0" );
        String newBase = "newBase";

        Flux<String> foundPaths = Flux.from( treeRepository.createMovePath( Ltree.valueOf( newBase ), toMove, user0 ) )
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
        TreeDto record = new TreeDto( null, ObjectType.ROOT,
                Ltree.valueOf( "" ), null, null );
        OffsetDateTime currentTime = Mono.from( dsl.select( currentOffsetDateTime() ) ).block().component1();
        // returns correct record
        TreeDto newRecord = treeRepository.create( record, user0 ).block();
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

        TreeDto beforeMove = tree0.getOrigRecord( "dir0.dir3.file1" );
        Ltree newPath = Ltree.valueOf( "dir0.dir1.file1" );
        Long rowsChanged = treeRepository.mvFile( newPath, beforeMove, user0 ).block();
        assertEquals( 1, rowsChanged, "Unexpected number of rows changed by move" );
        TreeDto afterMove = tree0.fetchCurRecord( beforeMove );
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

        TreeRecord spoofedRecord = tree0.getOrigRecord( "dir0.dir1.dir2" ).intoRecord();
        spoofedRecord.setObjectType( ObjectType.FILE ); // force db to handle objectType check;
        Ltree newPath = Ltree.valueOf( "dir0.dir3.dir2" );
        Long rowsChanged = treeRepository.mvFile( newPath, spoofedRecord.into( TreeDto.class ), user0 ).block();

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

        TreeDto curDir = tree0.getOrigRecord( "dir0" );
        Ltree newName = Ltree.valueOf( "dir100" );

        Mono<Long> rename = treeRepository.mvDir( newName, curDir, user0 );
        StepVerifier.create( rename )
                .expectNext( 5L )
                .verifyComplete();

        for (String path : List.of( "dir0", "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1" )) {
            String expectedNewPath = path.replace( "dir0", "dir100" );
            TreeRecord expectedNewRecord = tree0.getOrigRecord( path )
                    .intoRecord();
            expectedNewRecord.setPath( Ltree.valueOf( expectedNewPath ) );
            assertEquals( expectedNewRecord.into( TreeDto.class ), tree0.fetchCurRecord( tree0.getOrigRecord( path ) ) );
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

        TreeDto curDir = tree0.getOrigRecord( "dir0" );
        Ltree newName = Ltree.valueOf( "dir100" );

        Mono<Long> rename = treeRepository.mvDir( newName, curDir, user0 );
        StepVerifier.create( rename )
                .expectNext( 5L )
                .verifyComplete();

        for (String path : List.of( "dir0", "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1" )) {
            String expectedNewPath = path.replace( "dir0", "dir100" );
            TreeRecord expectedNewRecord = tree0.getOrigRecord( path )
                    .intoRecord();
            expectedNewRecord.setPath( Ltree.valueOf( expectedNewPath ) );
            assertEquals( expectedNewRecord.into( TreeDto.class ), tree0.fetchCurRecord( tree0.getOrigRecord( path ) ) );
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

        TreeDto origRecord = tree0.getOrigRecord( "dir0.dir3" );
        Ltree movePath = Ltree.valueOf( "dir0.dir1.dir2.dir3" );

        Mono<Long> move = treeRepository.mvDir( movePath, origRecord, user0 );

        StepVerifier.create( move )
                .expectNext( 2L )
                .verifyComplete();

        for (String path : List.of( "dir0.dir3", "dir0.dir3.file1" )) {
            String expectedNewPath = path.replace( "dir0.dir3", "dir0.dir1.dir2.dir3" );
            TreeRecord expectedNewRecord = tree0.getOrigRecord( path )
                    .intoRecord();
            expectedNewRecord.setPath( Ltree.valueOf( expectedNewPath ) );
            assertEquals( expectedNewRecord.into( TreeDto.class ), tree0.fetchCurRecord( tree0.getOrigRecord( path ) ) );
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

        TreeRecord origRecord = tree0.getOrigRecord( "dir0.dir3.file1" )
                .intoRecord();
        origRecord.setObjectType( ObjectType.DIR ); // spoofed object type;
        Ltree movePath = Ltree.valueOf( "file1" );

        Mono<Long> move = treeRepository.mvDir( movePath, origRecord.into( TreeDto.class ), user0 );

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
        TreeDto source = tree0.getOrigRecord( "dir0.dir3" );
        Ltree destination = Ltree.valueOf( "dir0.dir1.dir2.dir3" );
        // raw type for brevity here...
        Map<String, ? extends Record6> copyRecordsByPath = Flux.from( treeRepository.fetchDirCopyRecords( destination, source, user0 ) )
                .collectMap( r -> r.get( TREE.PATH ).data() ).block();

        TreeDto origDir3 = tree0.getOrigRecord( "dir0.dir3" );
        var curDir3 = copyRecordsByPath.get( destination.data() );
        assertNotEquals( origDir3.getObjectId(), curDir3.get( TREE.OBJECT_ID ) );
        assertEquals( origDir3.getObjectType(), curDir3.get( TREE.OBJECT_TYPE ) );
        assertEquals( origDir3.getUserId(), curDir3.get( TREE.USER_ID ) );
        assertTrue( origDir3.getCreatedAt().isBefore( curDir3.get( TREE.CREATED_AT ) ) );

        TreeDto origFile1 = tree0.getOrigRecord( "dir0.dir3.file1" );
        var curFile1 = copyRecordsByPath.get( destination.data() + ".file1" );
        assertNotEquals( origFile1.getObjectId(), curFile1.get( TREE.OBJECT_ID ) );
        assertEquals( origFile1.getObjectType(), curFile1.get( TREE.OBJECT_TYPE ) );
        assertEquals( origFile1.getUserId(), curFile1.get( TREE.USER_ID ) );
        assertTrue( origFile1.getCreatedAt().isBefore( curFile1.get( TREE.CREATED_AT ) ) );

        // this is just the creation of records *to* insert, so not mutating
        assertNoChanges( tree0 );
    }

    @Test
    @DisplayName("fetchDirCopyRecords does not return records when ObjectType is spoofed")
    void fetchDirCopyRecordsDoesNotFetchSpoofedObjectType() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TreeRecord source = tree0.getOrigRecord( "dir0.dir3.file1" )
                .intoRecord();
        source.setObjectType( ObjectType.DIR );
        Ltree destination = Ltree.valueOf( "dir0.dir1.dir2.dir3" );
        StepVerifier.create( treeRepository.fetchDirCopyRecords( destination, source.into( TreeDto.class ), user0 ) )
                .expectNextCount( 0 ).verifyComplete();
    }

    @Test
    @DisplayName("fetchDirCopyRecords does not fetch when incorrect user_id")
    void fetchDirCopyRecordsDoesNotFetchWhenIncorrectUserId() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        TreeDto source = tree0.getOrigRecord( "dir0.dir3" );
        Ltree destination = Ltree.valueOf( "dir0.dir1.dir2.dir3" );
        StepVerifier.create( treeRepository.fetchDirCopyRecords( destination, source, user1 ) )
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
        TreeDto source = tree0.getOrigRecord( srcPathStr );
        Ltree destination = Ltree.valueOf( destPathSr );

        Set<String> expectedPaths = Set.of( "dir100", "dir100.dir1", "dir100.dir1.dir2", "dir100.dir3", "dir100.dir3.file1" );

        List<Record3<UUID, UUID, ObjectType>> copy = treeRepository.cpDir( destination, source, user0 ).collectList().block();

        for (Record3<UUID, UUID, ObjectType> record : copy) {
            TreeDto srcRec = tree0.fetchCurRecord( record.get( "source_id", UUID.class ) );
            TreeDto cpRec = tree0.fetchCurRecord( record.get( "destination_id", UUID.class ) );
            String destFromSrc = srcRec.getPath().data().replace( srcPathStr, destPathSr );
            assertEquals( destFromSrc, cpRec.getPath().data() );
            assertTrue( record.get( "object_type" ) == srcRec.getObjectType()
                    && srcRec.getObjectType() == cpRec.getObjectType() );
        }

        Map<Boolean, List<String>> recordsByNew = tree0.fetchAllUserObjects()
                .stream().map( TreeDto::getPath )
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
        TreeDto srcRecord = tree0.getOrigRecord( srcStr );
        var record = Mono.from(
                        treeRepository.fetchFileCopyRecords( Ltree.valueOf( destStr ), srcRecord, user0 ) )
                .block();

        assertEquals( srcRecord.getObjectId(), record.get( "source_id" ) );
        assertNotEquals( srcRecord.getObjectId(), record.get( TREE.OBJECT_ID ) );
        assertEquals( ObjectType.FILE, record.get( TREE.OBJECT_TYPE ) );
        assertEquals( record.get( TREE.PATH ), Ltree.valueOf( destStr ) );
        assertEquals( record.get( TREE.USER_ID ), user0.getUserId() );
        assertTrue( record.get( TREE.CREATED_AT ).isAfter( srcRecord.getCreatedAt() ) );
    }

    @Test
    @DisplayName("fetch CopyFileRecords does not fetch another users records")
    void fetchCopyFileRecordsDoesNotFetchAnotherUsersRecords() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        String srcStr = "dir0.dir3.file1";
        String destStr = "dir0.file100";
        TreeDto srcRecord = tree0.getOrigRecord( srcStr );
        StepVerifier.create( treeRepository.fetchFileCopyRecords( Ltree.valueOf( destStr ), srcRecord, user1 ) )
                .expectNextCount( 0 )
                .verifyComplete();
    }

    @Test
    @DisplayName("cpFile returns Mono.empty() when ObjectType is spoofed")
    void cpFileReturnsMonoEmptyWhenIncorrectObjectType() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        String srcStr = "dir0.dir1";
        String destStr = "dir0.file100";
        TreeRecord srcRecord = tree0.getOrigRecord( srcStr )
                .intoRecord();
        srcRecord.setObjectType( ObjectType.FILE ); // spoofed object type
        var record = Mono.from(
                treeRepository.fetchFileCopyRecords( Ltree.valueOf( destStr ), srcRecord.into(TreeDto.class), user0 ) );
        StepVerifier.create( record ).expectNextCount( 0 ).verifyComplete();
    }

    @Test
    @DisplayName("selectDescendents returns UUIDs of descendents")
    void selectDescendentsReturnsDescendents() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        List<UUID> expected = Stream.of( "dir0", "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1" )
                .map( tree0::getOrigRecord )
                .map( TreeDto::getObjectId )
                .sorted()
                .toList();
        Flux<UUID> found = Flux.from( treeRepository.selectDescendents( tree0.getOrigRecord( "dir0" ), user0 ) )
                .map( rec -> rec.get( TREE.OBJECT_ID ) )
                .sort();
        StepVerifier.create( found )
                .expectNextSequence( expected )
                .verifyComplete();
    }

    @Test
    @DisplayName("selectDescendents returns 0 UUIDs when user_id spoofed")
    void selectDescendentsReturnsZeroDescendentsSpoofedUserId() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        Flux<UUID> found = Flux.from( treeRepository.selectDescendents( tree0.getOrigRecord( "dir0" ), user1 ) )
                .map( rec -> rec.get( TREE.OBJECT_ID ) )
                .sort();
        StepVerifier.create( found )
                .expectNextCount( 0 )
                .verifyComplete();
    }

    @Test
    @DisplayName("hasDescendents returns 1 when only descendent is self")
    void hasDescendents() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        TreeDto parent = tree0.getOrigRecord( "dir0.dir1.dir2" );

        Mono<Integer> numDesc = Mono.from( treeRepository.hasDescendents( parent, user0 ) )
                .map( rec -> rec.get( "count", Integer.class ) );
        StepVerifier.create( numDesc )
                .expectNext( 1 )
                .verifyComplete();
    }

    @Test
    @DisplayName("hasDescendents returns 0 with spoofed userId")
    void hasDescendentsReturnsZeroWhenNoDescendents() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        TreeDto parent = tree0.getOrigRecord( "dir0.dir1.dir2" );
        // note: user1
        Mono<Integer> numDesc = Mono.from( treeRepository.hasDescendents( parent, user1 ) )
                .map( rec -> rec.get( "count", Integer.class ) );
        StepVerifier.create( numDesc )
                .expectNext( 0 )
                .verifyComplete();
    }

    @Test
    @DisplayName("hasDescendents returns 0 objectType is ROOT")
    void hasDescendentsReturnsZeroWhenObjectTypeRoot() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        TreeDto parent = tree0.getOrigRecord( "" );
        // note: user1
        Mono<Integer> numDesc = Mono.from( treeRepository.hasDescendents( parent, user0 ) )
                .map( rec -> rec.get( "count", Integer.class ) );
        StepVerifier.create( numDesc )
                .expectNext( 0 )
                .verifyComplete();
    }

    @Test
    @DisplayName("hasDescendents returns 2 when multiple descendents")
    void hasDescendentsReturnsTwoWhenMultipleDescendents() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        TreeDto parent = tree0.getOrigRecord( "dir0" );
        // note: user1
        Mono<Integer> numDesc = Mono.from( treeRepository.hasDescendents( parent, user0 ) )
                .map( rec -> rec.get( "count", Integer.class ) );
        StepVerifier.create( numDesc )
                .expectNext( 2 )
                .verifyComplete();
    }

    @Test
    @DisplayName("rmDirRecursive deletes records and returns object_ids")
    void rmDriRecursiveDeletesAndReturnsObjectIds() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );

        String toDelete = "dir0";

        Flux<UUID> found = treeRepository.rmDirRecursive( tree0.getOrigRecord( toDelete ), user0 )
                .sort();
        Iterable<UUID> expected = Stream.of(
                        "dir0", "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1" )
                .map( p -> tree0.getOrigRecord( p ) )
                .map( TreeDto::getObjectId )
                .sorted()
                .toList();
        StepVerifier.create( found )
                .expectNextSequence( expected )
                .verifyComplete();

        assertEquals( Stream.of( tree0.getOrigRecord( "" ),
                                tree0.getOrigRecord( "file0" ) )
                        .sorted( TreeDtoComparators.compareByObjectId() )
                        .toList(),
                tree0.fetchAllUserObjects( ) );
        assertNoChanges( tree1 );
    }

    @ParameterizedTest
    @DisplayName("rmNormal deletes record and returns object_id")
    @ValueSource(strings = {"dir0.dir1.dir2", "file0"})
    void rmNormalDeletesRecordAndReturnsObjectId(String toDelete) {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        TreeDto recToDelete = tree0.getOrigRecord( toDelete );

        StepVerifier.create( treeRepository.rmNormal( recToDelete, user0 ) )
                .expectNext( recToDelete.getObjectId() )
                .verifyComplete();

        List<TreeDto> found = tree0.fetchAllUserObjects( );
        List<TreeDto> expected = tree0.getTrackedObjects( )
                .stream()
                .filter( r -> !r.equals( recToDelete ) ).toList();
        assertIterableEquals( expected, found );

        assertNoChanges( tree1 );
    }

    @Test
    @DisplayName("isObjectType returns true when tested with correct obj type")
    void isObjectTypeReturnsTrueWhenExpectedType() {
        BiFunction<ObjectType, TreeDto, Boolean> isObject = (ObjectType ot, TreeDto tr) ->
                Mono.from( treeRepository.isObjectType( ot, tr, user0 ) ).map( Record1::value1 ).block();
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        tree0.getTrackedObjects().forEach( rec ->
                assertTrue( isObject.apply( rec.getObjectType(), rec ) ) );
    }

    @Test
    @DisplayName("isObjectType returns false when incorrect objectType")
    void isObjectTypeReturnsFalseWhenIncorrectObjectType() {
        BiFunction<ObjectType, TreeDto, Boolean> isObject = (ObjectType ot, TreeDto tr) ->
                Mono.from( treeRepository.isObjectType( ot, tr, user0 ) ).map( Record1::value1 ).block();
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        ObjectType[] objects = ObjectType.values();
        tree0.getTrackedObjects().forEach( rec -> {
            var wrongObj = objects[( rec.getObjectType().ordinal() + 1 ) % objects.length];
            assertFalse( isObject.apply( wrongObj, rec ) );
        } );
    }

    @Test
    @DisplayName("isObjectType returns false when incorrect userId")
    void isObjectTypeReturnsFalseWhenWrongUserId() {
        BiFunction<ObjectType, TreeDto, Boolean> isObject = (ObjectType ot, TreeDto tr) ->
                Mono.from( treeRepository.isObjectType( ot, tr, user1 ) ).map( Record1::value1 ).block();
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        tree0.getTrackedObjects().forEach( rec ->
                assertFalse( isObject.apply( rec.getObjectType(), rec ) ) );
    }
}