package com.ericgha.docuCloud.repository.testutil.file;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.dto.TreeJoinFileDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTree;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTreeFactory;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnabledPostgresTestContainer;
import jakarta.annotation.PostConstruct;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.ericgha.docuCloud.jooq.Tables.FILE_VIEW;
import static com.ericgha.docuCloud.repository.testutil.assertion.OffsetDateTimeAssertion.assertPastDateTimeWithinLast;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EnabledPostgresTestContainer
class FileTestQueriesTest {
    private static final String TREE_FACTORY_CSV = """
            ROOT, ""
            FILE, "file0"
            FILE, "file1"
            DIR, "dir0"
            FILE, "dir0.file2"
            FILE, "dir0.file3"
            """;

    @Autowired
    private DSLContext dsl;

    @Autowired
    private FileTestQueries queries;

    @Autowired
    private TestFileTreeFactory treeFactory;
    private TestFileTree tree0;
    private TestFileTree tree1;

    CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    CloudUser user1 = CloudUser.builder()
            .userId( UUID.fromString( "ffffffff-ffff-ffff-fedc-ba9876543210" ) )
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
    void recordsByUser() {
        List<TreeDto> treeDtos0 = tree0.getTrackedObjectsOfType( ObjectType.FILE );
        List<FileViewDto> fvr0 = queries.createFilesWithLinks(
                        FileViewDtoCreator.create( treeDtos0, user0) )
                .collectList()
                .block();
        List<TreeDto> treeDtos1 = tree1.getTrackedObjectsOfType( ObjectType.FILE );
        List<FileViewDto> fvr1 = queries.createFilesWithLinks(
                        FileViewDtoCreator.create( treeDtos1, user1 ) )
                .collectList()
                .block();
        StepVerifier.create( queries.fetchRecordsByUserId( user0) )
                .expectNextSequence( fvr0 )
                .verifyComplete();
        StepVerifier.create( queries.fetchRecordsByUserId( user1) )
                .expectNextSequence( fvr1 )
                .verifyComplete();
    }

    @Test
    @DisplayName("fetchLink fetches the expected link")
    void fetchLinkReturnsLink() {
        List<TreeDto> treeDtos = tree0.getTrackedObjectsOfType( ObjectType.FILE );
        Disposable createdLinks = queries.createFilesWithLinks(
                        FileViewDtoCreator.create( treeDtos, user0 ) )
                .subscribe( fvr -> {
                    FileViewDto found = Mono.from( queries.fetchFileViewDto( fvr ) ).block();
                    // using property of FileViewDtoCreator where size is index in input array
                    assertEquals( fvr.getSize(), found.getSize() );
                } );
    }

    @Test
    @DisplayName("createLinks creates the expected link")
    void createLinksCreatesLink() {
        List<TreeDto> treeDtos = tree0.getTrackedObjectsOfType( ObjectType.FILE );
        FileViewDto initialFile = FileViewDtoCreator.create( treeDtos.get( 0 ),
                user0, 0 );
        queries.createFilesWithLinks( List.of( initialFile ) ).blockLast(); // insert a file
        FileViewDto linkToFile = FileViewDto.builder()
                .objectId( treeDtos.get( 1 ).getObjectId() )
                .fileId( initialFile.getFileId() )
                .userId( initialFile.getUserId() )
                .build();
        Flux<TreeJoinFileDto> createdLink = queries.createLinks( List.of( linkToFile ) );
        StepVerifier.create( createdLink ).assertNext( tjfR -> {
                    assertEquals( linkToFile.getObjectId(), tjfR.getObjectId() );
                    assertEquals( linkToFile.getFileId(), tjfR.getFileId() );
                    assertTrue( OffsetDateTime.now().isAfter( tjfR.getLinkedAt() ) &&
                            OffsetDateTime.now().minusSeconds( 1 ).isBefore( tjfR.getLinkedAt() ) );
                } )
                .verifyComplete();
    }

    @Test
    void fetchFileLinkingDegree() {
        List<TreeDto> treeDtos = tree0.getTrackedObjectsOfType( ObjectType.FILE );
        // create 2 1 degree links
        List<FileViewDto> newFileRecords = FileViewDtoCreator.create( treeDtos.subList( 0, 2 ), user0 );
        queries.createFilesWithLinks( newFileRecords ).blockLast();
        // add 2 more links to 2nd.  Degree 2nd = 3;
        List<FileViewDto> extraLinks = IntStream.range( 0, 2 ).boxed()
                .map( i -> FileViewDto.builder()
                        .fileId( newFileRecords.get( 1 ).getFileId() )
                        .objectId( treeDtos.get( 2 + i ).getObjectId() )
                        .userId( user0.getUserId() )
                        .build() )
                .toList();
        queries.createLinks( extraLinks ).blockLast();
        List<Record2<UUID, Long>> degreeByFileId = queries.fetchFileLinkingDegreeByFileId( user0 )
                .sort( Comparator.comparing( rec2 -> rec2.get( "count", Long.class ) ) )
                .collectList()
                .block();
        assertEquals( newFileRecords.get( 0 ).getFileId(), degreeByFileId.get( 0 ).get( FILE_VIEW.FILE_ID ) );
        assertEquals( 1L, degreeByFileId.get( 0 ).get( "count", Long.class ) );
        assertEquals( newFileRecords.get( 1 ).getFileId(), degreeByFileId.get( 1 ).get( FILE_VIEW.FILE_ID ) );
        assertEquals( 3L, degreeByFileId.get( 1 ).get( "count", Long.class ) );
    }

    @Test
    void createFilesWithLinks() {
        List<TreeDto> treeDtos = tree0.getTrackedObjectsOfType( ObjectType.FILE );
        List<FileViewDto> expectedRecords = FileViewDtoCreator.create( treeDtos, user0);
        List<FileViewDto> createdFileRecords = queries.createFilesWithLinks( expectedRecords )
                .collectList()
                .block();
        Flux<Tuple2<FileViewDto, FileViewDto>> zip = Flux.zip(Flux.fromIterable( expectedRecords ),
                Flux.fromIterable( createdFileRecords ) );
        StepVerifier.create( zip ).thenConsumeWhile( expFnd -> {
            FileViewDto expected = expFnd.getT1();
            FileViewDto found = expFnd.getT2();
            assertEquals( expected.getObjectId(), found.getObjectId(), "Unexpected object_id" );
            assertNotNull( found.getFileId(), "file_id was null" );
            assertEquals( expected.getUserId(), found.getUserId(), "Unexpected user_id" );
            assertPastDateTimeWithinLast(found.getUploadedAt(), Duration.ofSeconds(1) );
            assertPastDateTimeWithinLast(found.getUploadedAt(), Duration.ofSeconds(1) );
            assertEquals( expected.getChecksum(), found.getChecksum(), "unexpected checksum" );
            assertEquals( expected.getSize(), found.getSize() );
            return true;
        } ).verifyComplete();
    }

    @Test
    void fetchObjectLinkingDegree() {
        // linking degree will become index number in treeDtos
        List<TreeDto> treeDtos = tree0.getTrackedObjectsOfType( ObjectType.FILE );
        List<FileViewDto> toCreate = IntStream.range( 0, 4 ).boxed().flatMap(
                        i -> FileViewDtoCreator.create( treeDtos.subList( 4 - i, 4 ), user0 ).stream() )
                .toList();
        queries.createFilesWithLinks( toCreate ).blockLast();
        List<Record2<UUID, Long>> degree = queries.fetchObjectLinkingDegreeByObjectId( user0 )
                .sort( Comparator.comparing( rec2 -> rec2.get( "count", Long.class ) ) )
                .collectList().block();
        degree.forEach( rec2 -> assertEquals(
                treeDtos.get( rec2.get( "count", Long.class ).intValue() ).getObjectId(),
                rec2.get( FILE_VIEW.OBJECT_ID ) ) );
    }
}