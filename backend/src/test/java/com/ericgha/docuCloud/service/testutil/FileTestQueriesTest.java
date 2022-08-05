package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnabledPostgresTestContainer;
import jakarta.annotation.PostConstruct;
import org.jooq.DSLContext;
import org.jooq.Record2;
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
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.ericgha.docuCloud.jooq.Tables.FILE_VIEW;
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

    private static final Comparator<FileViewRecord> fvrComparator = FileViewRecordComparators.sizeObjectId();
    @Autowired
    private DSLContext dsl;

    private FileTestQueries queries;
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
        Path input = Paths.get( this.getClass().getClassLoader()
                .getResource( "tests-schema.sql" ).toURI() );
        String sql = Files.readString( input );
        Mono.from( dsl.query( sql ) ).block();
        this.queries = new FileTestQueries( dsl );
        TreeTestQueries treeTestQueries = new TreeTestQueries( dsl );
        TestFileTreeFactory treeFactory = new TestFileTreeFactory( treeTestQueries );
        this.tree0 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user0 );
        this.tree1 = treeFactory.constructFromCsv( TREE_FACTORY_CSV, user1 );
    }

    @Test
    void recordsByUser() {
    }

    @Test
    void fetchLink() {
    }

    @Test
    void createLinks() {
    }

    @Test
    void fetchFileLinkingDegree() {
        List<TreeRecord> treeRecords = tree0.getTrackedObjectsOfType( ObjectType.FILE );
        // create 2 1 degree links
        List<FileViewRecord> newFileRecords = FileViewRecordCreator.create( treeRecords.subList( 0, 2 ), user0, fvrComparator );
        // add 2 more links to 2nd.  Degree 2nd = 3;
        List<FileViewRecord> extraLinks = IntStream.range(0,2).boxed()
                .map( i -> new FileViewRecord().setFileId( newFileRecords.get(1).getFileId() )
                        .setObjectId( treeRecords.get(2+i).getObjectId() )
                        .setUserId( user0.getUserId() ) )
                .toList();
        queries.createLinks(extraLinks, fvrComparator).blockLast();
        List<Record2<UUID,Long>> degreeByFileId = queries.fetchFileLinkingDegree( user0 )
                .sort(Comparator.comparing(rec2 -> rec2.get("count", Long.class)))
                .collectList()
                .block();
        assertEquals(newFileRecords.get(0).getFileId(), degreeByFileId.get(0).get(FILE_VIEW.FILE_ID) );
        assertEquals( 1L, degreeByFileId.get(0).get("count", Long.class) );
        assertEquals(newFileRecords.get(1).getFileId(), degreeByFileId.get(1).get(FILE_VIEW.FILE_ID) );
        assertEquals( 3L, degreeByFileId.get(1).get("count", Long.class) );
    }

    @Test
    void createFilesWithLinks() {
        List<TreeRecord> treeRecords = tree0.getTrackedObjectsOfType( ObjectType.FILE );
        List<FileViewRecord> newFileRecords = FileViewRecordCreator.create( treeRecords, user0, fvrComparator );
        Flux<FileViewRecord> createdFileRecords = queries.createFilesWithLinks( newFileRecords, fvrComparator );
        StepVerifier.create(createdFileRecords).thenConsumeWhile( fvRec -> {
            int index = fvRec.getSize().intValue(); // remember size is used internally as index by FileViewRecordCreator
            assertEquals(newFileRecords.get(index).getObjectId(), fvRec.getObjectId(), "Unexpected object_id" );
            assertNotNull(fvRec.getFileId(), "file_id was null" );
            assertEquals( newFileRecords.get(index).getUserId(), fvRec.getUserId(), "Unexpected user_id" );
            assertTrue( OffsetDateTime.now().isAfter( fvRec.getUploadedAt() ) &&
                    OffsetDateTime.now().minusSeconds( 1 ).isBefore( fvRec.getUploadedAt() ) , "unexpected uploaded_at time" );
            assertEquals( fvRec.getUploadedAt(), fvRec.getLinkedAt(), "unexpected linked_at time" );
            assertEquals( newFileRecords.get(index).getChecksum(), fvRec.getChecksum(), "unexpected checksum" );
            //size field tested implicitly by using as index.
            return true;
        } ).verifyComplete();
    }

    @Test
    void fetchObjectLinkingDegree() {
        // linking degree will become index number in treeRecords
        List<TreeRecord> treeRecords = tree0.getTrackedObjectsOfType( ObjectType.FILE );
        List<FileViewRecord> toCreate = IntStream.range(0,4).boxed().flatMap(
                        i -> FileViewRecordCreator.create(treeRecords.subList( 4-i, 4), user0, fvrComparator).stream() )
                .toList();
        queries.createFilesWithLinks( toCreate, fvrComparator ).blockLast();
        List<Record2<UUID,Long>> degree = queries.fetchObjectLinkingDegree( user0 )
                .sort( Comparator.comparing( rec2 -> rec2.get("count", Long.class) ) )
                .collectList().block();
        degree.forEach( rec2 -> assertEquals(
                treeRecords.get(rec2.get("count", Long.class).intValue()).getObjectId(),
                rec2.get(FILE_VIEW.OBJECT_ID) ) );
    }
}