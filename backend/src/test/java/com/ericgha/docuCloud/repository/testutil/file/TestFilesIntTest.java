package com.ericgha.docuCloud.repository.testutil.file;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTree;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTreeFactory;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnabledPostgresTestContainer;
import jakarta.annotation.PostConstruct;
import org.jooq.DSLContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.convert.converter.Converter;
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
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Stream;

import static com.ericgha.docuCloud.repository.testutil.assertion.CollectionAssertion.assertCollectionMapsEqual;
import static com.ericgha.docuCloud.repository.testutil.assertion.OffsetDateTimeAssertion.assertPastDateTimeWithinLast;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EnabledPostgresTestContainer
class TestFilesIntTest {
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

    @Autowired
    private Converter<FileViewDto, FileDto> fileViewToFile;

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
        Path schemaFile = Paths.get( this.getClass().getClassLoader()
                .getResource( "tests-schema.sql" ).toURI() );
        String sql = Files.readString( schemaFile );
        Mono.from( dsl.query( sql ) ).block();
        this.tree0 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user0 );
        this.tree1 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user1 );
    }

    @Test
    @DisplayName("insertFileViewRecord creates a new file record linked to expected object")
    void InsertFileViewRecordNewFile() {
        String checksum = "fileRes0";
        String objPath = "fileObj0";
        TestFiles files0 = fileFactory.construct( tree0 );
        files0.insertFileViewRecord( objPath, checksum );
        Flux<FileViewDto> found = fileQueries.fetchRecordsByChecksum( checksum, tree0.getUserId() );
        UUID expectedObjectId = tree0.fetchByObjectPath( objPath ).getObjectId();
        StepVerifier.create( found ).assertNext( fvr -> {
                    assertEquals( expectedObjectId, fvr.getObjectId() );
                    assertNotNull( fvr.getFileId() );
                    assertEquals( tree0.getUserId(), fvr.getUserId() );
                    assertPastDateTimeWithinLast( fvr.getUploadedAt(), Duration.ofSeconds( 1 ) );
                    assertPastDateTimeWithinLast( fvr.getLinkedAt(), Duration.ofSeconds( 1 ) );
                    assertEquals( 0L, fvr.getSize() );
                } )
                .verifyComplete();
    }

    @Test
    @DisplayName("insertFileViewRecord adds a link to an existing file")
    void InsertFileViewRecordExistingFile() {
        String checksum = "fileRes0";
        String initialObj = "fileObj0";
        String newObj = "dir0.fileObj2";
        TestFiles files0 = fileFactory.construct( tree0 );
        // create file
        files0.insertFileViewRecord( initialObj, checksum );
        // link another obj
        FileViewDto foundNewLink = files0.insertFileViewRecord( newObj, checksum );
        List<FileViewDto> expectedNewLink = fileQueries.fetchRecordsByObjectId( tree0.fetchByObjectPath( newObj ).getObjectId() )
                .collectList()
                .block();
        assertIterableEquals( expectedNewLink, List.of( foundNewLink ) );
        // fetch orignal link and pull out file record
        List<FileDto> origFile = fileQueries.fetchRecordsByObjectId( tree0.fetchByObjectPath( initialObj ).getObjectId() )
                .map( fileViewToFile::convert )
                .collectList()
                .block();
        // test both orig and new link point to the same file
        assertEquals( origFile, List.of( fileViewToFile.convert( foundNewLink ) ) );
    }

    @Test
    @DisplayName("insertFileViewRecord records object <-> file link")
    void insertFileViewRecordStoresRecord() {
        String checksum = "fileRes0";
        String objPath = "fileObj0";
        TestFiles files0 = fileFactory.construct( tree0 );
        TestFiles files1 = fileFactory.construct( tree1 );
        files0.insertFileViewRecord( objPath, checksum );
        // challenge record
        files1.insertFileViewRecord( objPath, checksum );
        NavigableMap<String, NavigableSet<FileViewDto>> allFileViews = files0.getOrigFileViewsGroupedByPathStr();
        assertEquals( 1, allFileViews.size() );
        Iterable<FileViewDto> expectedFileView = fileQueries.fetchRecordsByUserId( user0 )
                .toIterable();
        assertIterableEquals( expectedFileView, allFileViews.get( objPath ) );
        NavigableMap<String, NavigableSet<TreeDto>> allTreeDtos = files0.getTreeDtosByLinkedFileChecksum();
        assertEquals( 1, allTreeDtos.size() );
        assertIterableEquals( List.of( tree0.getOrigRecord( objPath ) ), allTreeDtos.get( checksum ) );
    }

    @Test
    @DisplayName("fetchTreeDtosGroupedByLinkedFileChecksum returns expected set")
    void fetchTreeDtosGroupedByLinkedFileChecksumReturnsExpected() {
        String csv = """
                fileObj0, fileRes0
                fileObj1, fileRes1
                dir0.fileObj2, fileRes1
                """;
        TestFiles files0 = fileFactory.constructFromCsv( csv, tree0 );
        // selectivity challenge
        TestFiles files1 = fileFactory.constructFromCsv( csv, tree1 );
        // manually create expected map
        TreeMap<String, NavigableSet<TreeDto>> expectedMap = new TreeMap<>();
        NavigableSet<TreeDto> fileRes0Objs = new TreeSet<>();
        fileRes0Objs.add( tree0.getOrigRecord( "fileObj0" ) );
        NavigableSet<TreeDto> fileRes1Objs = new TreeSet<>();
        Stream.of( "fileObj1", "dir0.fileObj2" ).map( tree0::getOrigRecord ).forEach( fileRes1Objs::add );
        expectedMap.put( "fileRes0", fileRes0Objs );
        expectedMap.put( "fileRes1", fileRes1Objs );

        assertIterableEquals( expectedMap.entrySet(), files0.fetchTreeDtosByLinkedFileChecksum().entrySet() );
    }

    @Test
    @DisplayName("fetchObjectsLinkedTo(string) returns treeDtos linked to checksum")
    void fetchObjectsLinkedToChecksumReturnsExpected() {
        String csv = """
                fileObj0, fileRes0
                fileObj1, fileRes1
                dir0.fileObj2, fileRes1
                """;
        TestFiles files0 = fileFactory.constructFromCsv( csv, tree0 );
        // selectivity challenge
        TestFiles files1 = fileFactory.constructFromCsv( csv, tree1 );
        // create expected sets
        NavigableSet<TreeDto> expectedFileRes0 = new TreeSet<>();
        expectedFileRes0.add( tree0.getOrigRecord( "fileObj0" ) );
        NavigableSet<TreeDto> expectedFileRes1 = new TreeSet<>();
        Stream.of( "fileObj1", "dir0.fileObj2" ).map( tree0::getOrigRecord ).forEach( expectedFileRes1::add );
        assertIterableEquals( expectedFileRes0, files0.fetchObjectsLinkedTo( "fileRes0" ) );
        assertIterableEquals( expectedFileRes1, files0.fetchObjectsLinkedTo( "fileRes1" ) );
    }

    @Test
    @DisplayName("fetchObjectsLinkedTo(UUID) returns treeDtos linked to FileId")
    void fetchObjectsLinkedToFileIdReturnsExpected() {
        String csv = """
                fileObj0, fileRes0
                fileObj1, fileRes1
                dir0.fileObj2, fileRes1
                """;
        TestFiles files0 = fileFactory.constructFromCsv( csv, tree0 );
        // selectivity challenge
        TestFiles files1 = fileFactory.constructFromCsv( csv, tree1 );
        // create expected sets
        NavigableSet<TreeDto> expectedFileRes0 = new TreeSet<>();
        expectedFileRes0.add( tree0.getOrigRecord( "fileObj0" ) );
        NavigableSet<TreeDto> expectedFileRes1 = new TreeSet<>();
        Stream.of( "fileObj1", "dir0.fileObj2" ).map( tree0::getOrigRecord ).forEach( expectedFileRes1::add );
        assertIterableEquals( expectedFileRes0, files0.fetchObjectsLinkedTo( files0.getOrigFileFor( "fileRes0" ).getFileId() ) );
        assertIterableEquals( expectedFileRes1, files0.fetchObjectsLinkedTo( files0.getOrigFileFor( "fileRes1" ).getFileId() ) );
    }

    @Test
    @DisplayName("fetchFileViewDtosLinkedTo(TreeDto) returns expected FileViewDtos")
    void fetchFileViewDtosLinkedToTreeDtoReturnsExpected() {
        String csv = """
                fileObj0, fileRes0
                fileObj1, fileRes1
                dir0.fileObj2, fileRes1
                """;
        TestFiles files0 = fileFactory.constructFromCsv( csv, tree0 );
        // selectivity challenge
        TestFiles files1 = fileFactory.constructFromCsv( csv, tree1 );
        // create expected sets
        assertIterableEquals(files0.fetchFileDtosLinkedTo("fileObj0"),
                List.of(files0.getOrigFileFor( "fileRes0" ) ) );
        assertIterableEquals(files0.fetchFileDtosLinkedTo("fileObj1"),
                List.of(files0.getOrigFileFor( "fileRes1" ) ) );
        assertIterableEquals(files0.fetchFileDtosLinkedTo("dir0.fileObj2"),
                List.of(files0.getOrigFileFor( "fileRes1" ) ) );
    }


    @Test
    @DisplayName( "fetchFileViewDtosGroupedByObjectPathStr returns the expected map" )
    void fetchFileViewDtosGroupedByObjectPathStrReturnsExpectedMap() {
        String csv = """
                fileObj0, fileRes0
                fileObj1, fileRes1
                dir0.fileObj2, fileRes1
                dir0.fileObj3, fileRes1
                """;
        TestFiles files0 = fileFactory.constructFromCsv(csv, tree0);
        // selectivity challenge
        TestFiles files1 = fileFactory.constructFromCsv( csv, tree1 );
        assertCollectionMapsEqual( files0.getOrigFileViewsGroupedByPathStr(), files0.fetchFileViewDtosGroupedByObjectPathStr() );
    }

    @Test
    @DisplayName( "fetchFileViewDtosGroupedByObjectPathStr(stream) returns expected records" )
    void fetchFileViewDtosGroupedByObjectPathStrCreatesMapOfExpectedRecords() {
        String csv = """
                fileObj0, fileRes0
                fileObj1, fileRes1
                dir0.fileObj2, fileRes1
                dir0.fileObj3, fileRes1
                """;
        TestFiles files0 = fileFactory.constructFromCsv(csv, tree0);
        // selectivity challenge
        TestFiles files1 = fileFactory.constructFromCsv( csv, tree1 );
        var expected = files0.getOrigFileViewsGroupedByPathStr();
        // add a obj and resource
        tree0.add(ObjectType.FILE, "fileObj4");
        files0.insertFileView( "fileObj4", "fileRes0" );
        // limit wanted to original adjacencies
        var wanted = ObjectResourceAdjacencyParser.parse(csv);
        var found = files0.fetchFileViewDtosGroupedByObjectPathStr(wanted);
        // comapre snapshot before additions to adjacency list without addition
        assertCollectionMapsEqual( expected, found );
    }


    @Test
    @DisplayName("getOrigFileViewFor returns expected record")
    void getOrigFileViewFor() {
        String csv = """
                fileObj0, fileRes0
                fileObj1, fileRes0
                dir0.fileObj2, fileRes0
                """;
        TestFiles files0 = fileFactory.constructFromCsv( csv, tree0 );
        FileViewDto expected = files0.insertFileViewRecord( "dir0.fileObj3", "fileRes0" );
        FileViewDto found = files0.getOrigFileViewFor( "dir0.fileObj3", "fileRes0" );
        assertEquals(expected, found);
    }
}