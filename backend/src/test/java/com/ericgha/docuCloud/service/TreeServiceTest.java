package com.ericgha.docuCloud.service;


import com.ericgha.docuCloud.factory.TreeRecordFactory;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class TreeServiceTest {

    @Autowired
    TreeService treeService;

    @Autowired
    DSLContext dsl;

    static final PostgreSQLContainer<?> postgreSQLContainer;
    static final int mappedPort; // not for test, exposed to facilitate connection to database for debugging

    static {
        postgreSQLContainer = new PostgreSQLContainer<>( DockerImageName.parse( "postgres:14" ) )
                .withDatabaseName( "docu-cloud-test-db" )
                .withUsername( "admin" )
                .withPassword( "password" )
                .withReuse( true );
        postgreSQLContainer.start();
        mappedPort = postgreSQLContainer.getMappedPort( 5432 );
    }

    @BeforeEach
    void before() throws URISyntaxException, IOException {
        // testcontainers cannot reliably run complex init scrips (ie with declared functions)
        // testcontainers/testcontainers-java issue #2814
        Path input = Paths.get( this.getClass().getClassLoader().getResource( "tests-schema.sql" ).toURI() );
        String sql = Files.readString( input );
        Mono.from( dsl.query( sql ) ).block();
    }

    @DynamicPropertySource
    static void datasourceConfig(DynamicPropertyRegistry registry) {
        registry.add( "spring.r2dbc.url", () -> postgreSQLContainer.getJdbcUrl().replace( "jdbc:", "r2dbc:" ) );
        registry.add( "spring.r2dbc.password", postgreSQLContainer::getPassword );
        registry.add( "spring.r2dbc.username", postgreSQLContainer::getUsername );
    }

    @Test
    @DisplayName("select path by objectId")
    void selectByObjectId() {
        String pathStr = "root";
        String objectId = treeService.create( pathStr, ObjectType.ROOT ).block().getObjectId();

        Ltree expectedPath = Ltree.valueOf( pathStr );
        int expectedLevel = 1;
        StepVerifier.create( treeService.selectPathAndLevel( objectId ) )
                .assertNext( record -> {
                    assertEquals( expectedPath, record.getValue( "path" ) );
                    assertEquals( expectedLevel, record.getValue( "level" ) );
                } )
                .verifyComplete();
    }

    @Test
    @DisplayName("Generate move paths for self and children")
    void selectMovePaths() {
        String newRoot = "newRoot";
        String rootId = treeService.create( "root0", ObjectType.ROOT )
                .block()
                .getObjectId();
        Flux.concat(
                        treeService.create( "root0.dir0", ObjectType.DIR ),
                        treeService.create( "root0.dir0.dir1", ObjectType.DIR ),
                        treeService.create( "root0.dir0.dir2", ObjectType.DIR ),
                        treeService.create( "root0.dir0.dir2.file0", ObjectType.FILE ),
                        // shouldn't be selected
                        treeService.create( "dummyRoot", ObjectType.ROOT ),
                        treeService.create( "dummyRoot.dir0", ObjectType.DIR )
                )
                .blockLast();

        Flux<String> foundPaths = Flux.from( treeService.createMovePath( rootId, Ltree.valueOf( newRoot ) ) )
                .map( record -> record.get( "path", Ltree.class ) )
                .map( Ltree::data );

        StepVerifier.create( foundPaths )
                .expectNext(
                        "newRoot",
                        "newRoot.dir0",
                        "newRoot.dir0.dir1",
                        "newRoot.dir0.dir2",
                        "newRoot.dir0.dir2.file0"
                )
                .verifyComplete();
    }

    @Test
    void create() {
        // returns correct record
        TreeRecord record = treeService.create( "a", ObjectType.ROOT ).block();
        assertEquals( "a", record.getPath().data() );
        assertEquals( ObjectType.ROOT, record.getObjectType() );
        assertEquals( 36, record.getObjectId().length() );
        // inserts
        StepVerifier.create( Mono.from( dsl.selectCount().from( TREE )
                        .where( TREE.OBJECT_ID.eq( record.getObjectId() ) ) ).map( count -> count.get( 0 ) ) )
                .expectNext( 1 ).
                verifyComplete();
    }

    @Test
    void updatePath() {
        String oldPath = "a";
        String newPath = "b";
        String objectId = treeService.create( oldPath, ObjectType.ROOT ).block().getObjectId();
        Mono<TreeRecord> updateRec = treeService.updatePath( objectId, newPath );
        StepVerifier.create( updateRec ).assertNext( treeRecord -> {
                    assertEquals( objectId, treeRecord.getObjectId() );
                    assertEquals( newPath, treeRecord.getPath().data() );
                } )
                .verifyComplete();
    }

    @Test
    void rename() {
        String rootId = treeService.create( "root", ObjectType.ROOT ).block().getObjectId();
        treeService.create( "root.dir0", ObjectType.DIR ).block();
        treeService.create( "root.dir0.dir1", ObjectType.DIR ).block();
        treeService.create( "root.dir0.dir2", ObjectType.DIR ).block();
        treeService.create( "root.dir0.dir2.file0", ObjectType.FILE ).block();

        Mono<Long> rename = treeService.mvPath( rootId, Ltree.valueOf( "newRoot" ) );
        StepVerifier.create( rename )
                .expectNext( 5L )
                .verifyComplete();
        System.out.println(); // break here
    }

    @Test
    @DisplayName("Config check: UNIQUE path")
    void uniqueConstraintPath() {
        Flux<TreeRecord> records = Flux.concat( treeService.create( "a", ObjectType.ROOT ),
                treeService.create( "a", ObjectType.ROOT ) );
        StepVerifier.create( records ).expectNextCount( 1 )
                .expectError( DataAccessException.class )
                .verify();
    }

    @Test
    @DisplayName("Insert trigger: ROOT level must equal 1")
    void rootInsTrigger() {
        Mono<TreeRecord> level0 = Mono.from( treeService.create( "", ObjectType.ROOT ) );
        StepVerifier.create( level0 ).verifyError( DataAccessException.class );
        Mono<TreeRecord> level1 = Mono.from( treeService.create( "a", ObjectType.ROOT ) );
        StepVerifier.create( level1 ).expectNextCount( 1 ).verifyComplete();
        Mono<TreeRecord> level2 = Mono.from( treeService.create( "a.b", ObjectType.ROOT ) );
        StepVerifier.create( level2 ).verifyError( DataAccessException.class );
    }

    @Test
    @DisplayName("Insert trigger: DIR or FILE prevent path level < 2")
    void dirFileInsTriggerLevel() {
        Mono<TreeRecord> level1Dir = Mono.from( treeService.create( "rootlevel", ObjectType.DIR ) );
        StepVerifier.create( level1Dir ).verifyError( DataAccessException.class );

        Mono<TreeRecord> level1File = Mono.from( treeService.create( "rootlevel", ObjectType.FILE ) );
        StepVerifier.create( level1File ).verifyError( DataAccessException.class );
    }

    @Test
    @DisplayName("Insert trigger: DIR and File orphan constraints")
    void DirFileInsTriggerOrphan() {
        // without parent
        Mono<TreeRecord> orphanLevel2Dir = Mono.from( treeService.create( "rootlevel.folder", ObjectType.DIR ) );
        StepVerifier.create( orphanLevel2Dir ).verifyError( DataAccessException.class );

        Mono<TreeRecord> orphanLevel2File = Mono.from( treeService.create( "rootlevel.file", ObjectType.FILE ) );
        StepVerifier.create( orphanLevel2File ).verifyError( DataAccessException.class );

        // now add the parent
        Mono.from( treeService.create( "rootlevel", ObjectType.ROOT ) ).block();

        Mono<TreeRecord> parentedLevel2Dir = Mono.from( treeService.create( "rootlevel.folder", ObjectType.DIR ) );
        StepVerifier.create( parentedLevel2Dir ).expectNextCount( 1 ).verifyComplete();

        Mono<TreeRecord> parentedLevel2File = Mono.from( treeService.create( "rootlevel.file", ObjectType.FILE ) );
        StepVerifier.create( parentedLevel2File ).expectNextCount( 1 ).verifyComplete();
    }

    @Test
    @DisplayName("Update leaves_no_orphans trigger")
    void updateLeavesNoOrphans() {
        String rootId = treeService.create( "root", ObjectType.ROOT ).block().getObjectId();
        String dirId = treeService.create( "root.dir", ObjectType.DIR ).block().getObjectId();
        String subDirId = treeService.create( "root.dir.subDir", ObjectType.DIR ).block().getObjectId();

        Mono<TreeRecord> rootFail = treeService.updatePath( rootId, "newRoot" );
        StepVerifier.create( rootFail ).verifyError( DataAccessException.class );

        Mono<TreeRecord> dirFail = treeService.updatePath( dirId, "root.newDir" );
        StepVerifier.create( dirFail ).verifyError( DataAccessException.class );
    }

    @Test
    void select() {
        TreeRecord record = treeService.select( "user0" ).block();
        System.out.println( record );
    }

    @Test
    void fetchPath() {
    }

    @Test
    void selectChildren() {
        List<TreeRecord> family = List.of(
                TreeRecordFactory.createFromPathStr( "r0", "r0", ObjectType.ROOT ),
                TreeRecordFactory.createFromPathStr( "r0.d1", "r0.d1", ObjectType.DIR ),
                TreeRecordFactory.createFromPathStr( "r0.d2", "r0.d2", ObjectType.DIR ),
                TreeRecordFactory.createFromPathStr( "r0.d1.f1", "r0.d1.f1", ObjectType.FILE ) );
        List<TreeRecord> dummies = List.of(
                TreeRecordFactory.createFromPathStr( "r1", "r1", ObjectType.ROOT ),
                TreeRecordFactory.createFromPathStr( "r1.d1", "r1.d1", ObjectType.DIR ),
                TreeRecordFactory.createFromPathStr( "r1.d2", "r1.d2", ObjectType.DIR ),
                TreeRecordFactory.createFromPathStr( "r1.d1.f1", "r1.d1.f1", ObjectType.FILE )
        );
        Flux.fromStream( Stream.concat( family.stream(), dummies.stream() ) )
                .flatMapSequential( treeService::create, 1 ) // sequential required for concurrency of 1, but...
                .blockLast();
        List<TreeRecord> expected = family.subList( 1, family.size() );
        Flux<TreeRecord> found = treeService
                .selectChildren( Ltree.valueOf( "r0" ) );
        StepVerifier.create( found ).expectNextSequence( expected ).verifyComplete();
    }

    @Test
    void selectChildrenAndParent() {
        List<TreeRecord> expected = List.of(
                TreeRecordFactory.createFromPathStr( "r0", "r0", ObjectType.ROOT ),
                TreeRecordFactory.createFromPathStr( "r0.d1", "r0.d1", ObjectType.DIR ),
                TreeRecordFactory.createFromPathStr( "r0.d2", "r0.d2", ObjectType.DIR ),
                TreeRecordFactory.createFromPathStr( "r0.d1.f1", "r0.d1.f1", ObjectType.FILE ) );
        List<TreeRecord> dummies = List.of(
                TreeRecordFactory.createFromPathStr( "r1", "r1", ObjectType.ROOT ),
                TreeRecordFactory.createFromPathStr( "r1.d1", "r1.d1", ObjectType.DIR ),
                TreeRecordFactory.createFromPathStr( "r1.d2", "r1.d2", ObjectType.DIR ),
                TreeRecordFactory.createFromPathStr( "r1.d1.f1", "r1.d1.f1", ObjectType.FILE )
        );
        Flux.fromStream( Stream.concat( expected.stream(), dummies.stream() ) )
                .flatMapSequential( treeService::create, 1 ) // sequential required for concurrency of 1, but...
                .blockLast();
        Flux<TreeRecord> found = treeService
                .selectChildrenAndParent( Ltree.valueOf( "r0" ) );
        StepVerifier.create( found ).expectNextSequence( expected ).verifyComplete();
    }
}