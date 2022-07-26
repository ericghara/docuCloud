package com.ericgha.docuCloud.repository.testtool.tree;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnablePostgresTestContainer;
import org.jooq.DSLContext;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static com.ericgha.docuCloud.repository.testtool.assertion.TestFileTreeAssertion.assertNoChanges;
import static com.ericgha.docuCloud.repository.testtool.assertion.TestFileTreeAssertion.assertNoChangesFor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EnablePostgresTestContainer
public class TestFileTreeTest {

    @Autowired
    DSLContext dsl;

    @Autowired
    TestFileTreeFactory treeFactory;
    @Autowired
    TreeTestQueries treeTestQueries;

    CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString("1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    CloudUser user1 = CloudUser.builder()
            .userId( UUID.fromString("ffffffff-ffff-ffff-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    @BeforeEach
    void before() throws URISyntaxException, IOException {
        // testcontainers cannot reliably run complex init scrips (ie with declared functions)
        // testcontainers/testcontainers-java issue #2814
        Path schemaFile = Paths.get( this.getClass().getClassLoader()
                .getResource( "tests-schema.sql" ).toURI() );
        String sql = Files.readString( schemaFile );
        Mono.from( dsl.query( sql ) ).block();
    }

    @Test
    @DisplayName( "add adds the expected record" )
    void addAddsRecord() {
        String pathStr = "";
        ObjectType objectType = ObjectType.ROOT;
        var testTree = treeFactory.construct( user0 );
        TreeDto newRecord = testTree.add( objectType, pathStr);
        assertEquals(newRecord, testTree.getOrigRecord( "" ) );
        assertEquals( pathStr, newRecord.getPath().data() );
        assertEquals(  objectType, newRecord.getObjectType() );
        assertEquals( user0.getUserId() , newRecord.getUserId() );
    }

    @Test
    @DisplayName( "add stores the expected record" )
    void addStoresRecord() {
        String pathStr = "";
        ObjectType objectType = ObjectType.ROOT;
        var testTree = treeFactory.construct( user0 );
        TreeDto newRecord = testTree.add( objectType, pathStr);
        assertEquals( newRecord, testTree.getOrigRecord( "" ) );
    }

    @Test
    @DisplayName( "assertNoChanges doesn't throw when no changes" )
    void assertNoChangesDoesNotThrow() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        tree1.add(ObjectType.FILE, "file100");
        assertDoesNotThrow( () -> assertNoChanges(tree0) );
    }

    @Test
    @DisplayName( "assertChanges throws when untracked change" )
    void assertNoChangesThrows() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        treeTestQueries.create(ObjectType.FILE, Ltree.valueOf("file100"), user0.getUserId())
                .block();
        assertThrows( AssertionError.class, () -> assertNoChanges(tree0) );
        assertDoesNotThrow( () -> assertNoChanges(tree1) );
    }

    @Test
    @DisplayName( "assertNoChangesFor does not throw when no changes" )
    void assertNoChangesForDoesNotThrow() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        treeTestQueries.create(ObjectType.FILE, Ltree.valueOf("file100"), user0.getUserId())
                .block();
        assertDoesNotThrow( () -> assertNoChangesFor( tree0, "", "dir0", "file0", "dir0.dir1",
                "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1" ) );
    }

    @Test
    @DisplayName( "assertNoChangesFor throws when provided an untracked object" )
    void assertNoChangesForThrowsWhenProvidedUntrackedObject() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        treeTestQueries.create(ObjectType.FILE, Ltree.valueOf("file100"), user0.getUserId())
                .block();
        assertThrows(AssertionError.class, () -> assertNoChangesFor( tree0, "", "dir0", "file0", "dir0.dir1",
                "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1", "file100" ) );
    }

    @Test
    @DisplayName( "assertNoChangesFor throws when tracked object modified" )
    void assertNoChangesForThrowsWhenUntrackedObjectCreated() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TreeRecord modified = tree0.getOrigRecord( "dir0" ).intoRecord();
        modified.setCreatedAt( OffsetDateTime.now() );
        treeTestQueries.update(modified.into(TreeDto.class) )
                .block();
        assertThrows(AssertionError.class, () -> assertNoChangesFor( tree0, "", "dir0", "file0", "dir0.dir1",
                "dir0.dir1.dir2", "dir0.dir3", "dir0.dir3.file1") );
    }

    @Test
    @DisplayName("addFromCsv adds expected records")
    void addFromCsvAddsExpectedRecords() {
        String csv = """
               FILE, "dir0.dir1.file0"
                """;
        TreeTestQueries testQueriesMock = Mockito.mock(TreeTestQueries.class);
        Mockito.when( testQueriesMock.create( any(ObjectType.class), any( Ltree.class), any(UUID.class) ) )
                .thenAnswer( invocation -> Mono.just(new TreeDto(null, null, invocation.getArgument(1), null, null) ) );

        ArgumentCaptor<ObjectType> typeCaptor = ArgumentCaptor.forClass(ObjectType.class);
        ArgumentCaptor<Ltree> ltreeCaptor = ArgumentCaptor.forClass( Ltree.class );
        ArgumentCaptor<UUID> userIdCaptor = ArgumentCaptor.forClass( UUID.class );

        TestFileTreeFactory factory = new TestFileTreeFactory( testQueriesMock );
        TestFileTree testTree = factory.constructRoot( user0 );

        testTree.addFromCsv( csv );

        verify( testQueriesMock, Mockito.times(2) ).create(
                typeCaptor.capture(), ltreeCaptor.capture(), userIdCaptor.capture() );
        assertEquals( ObjectType.FILE, typeCaptor.getValue() );
        assertEquals( Ltree.valueOf("dir0.dir1.file0") , ltreeCaptor.getValue() );
        assertEquals( user0.getUserId(), userIdCaptor.getValue() );
    }

    @Test
    @DisplayName("getTrackedObjectsOfType returns expected records")
    void getTrackedObjectsOfTypeReturnsExpectedRecords() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        List<String> expected = List.of("dir0", "dir0.dir1", "dir0.dir1.dir2", "dir0.dir3");
        List<String> found = tree0.getTrackedObjectsOfType( ObjectType.DIR )
                .stream()
                .map( TreeDto::getPath )
                .map(Ltree::data)
                .sorted()
                .toList();
        assertIterableEquals( expected, found );
    }

    @Test
    @DisplayName( "fetchByObjectPath fetches expected TreeDto" )
    void fetchByObjectPath() {
        TestFileTree tree0 = treeFactory.constructDefault( user0 );
        TestFileTree tree1 = treeFactory.constructDefault( user1 );
        TreeDto expected = tree0.getOrigRecord( "dir0.dir1.dir2" );
        TreeDto found = tree0.fetchByObjectPath( "dir0.dir1.dir2" );
        assertEquals(expected, found);
    }
}
