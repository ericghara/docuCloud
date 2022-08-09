package com.ericgha.docuCloud.repository.testutil.file;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import com.ericgha.docuCloud.util.comparators.FileViewRecordComparators;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FileViewRecordCreatorTest {

    private final CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    @Test
    @DisplayName("create(TreeRecord, CloudUser, long) generates expected FileViewRecord")
    void createSingleGeneratesExpected() {
        Long index = new SplittableRandom().nextLong();
        UUID objectId = UUID.randomUUID();

        TreeRecord treeRecord = new TreeRecord();
        treeRecord.setObjectId( objectId );

        FileViewRecord fileViewRecord = FileViewRecordCreator.create( treeRecord, user0, index );
        assertEquals( objectId, fileViewRecord.getObjectId() );
        assertNotNull( fileViewRecord.getFileId() );
        assertEquals( user0.getUserId(), fileViewRecord.getUserId() );
        assertNull( fileViewRecord.getUploadedAt() );
        assertNull( fileViewRecord.getLinkedAt() );
        assertEquals( "file" + index, fileViewRecord.getChecksum() );
        assertEquals( index, fileViewRecord.getSize() );
    }

    @Test
    @DisplayName(( "create(List<TreeRecord>, CloudUser, long) generates correct number of records and calls create(TreeRecord, CloudUser, long)" +
            "5 times with expected parameters" ))
    void testCreate() {
        List<Long> indexes = LongStream.range( 0, 5 )
                .boxed()
                .toList();
        List<TreeRecord> treeRecords = indexes.stream()
                .map( l -> new TreeRecord().setObjectId( UUID.randomUUID() ) )
                .toList();

        ArgumentCaptor<TreeRecord> recordCaptor = ArgumentCaptor.forClass( TreeRecord.class );
        ArgumentCaptor<CloudUser> userCaptor = ArgumentCaptor.forClass( CloudUser.class );
        ArgumentCaptor<Long> longCaptor = ArgumentCaptor.forClass( Long.class );

        try (MockedStatic<FileViewRecordCreator> creatorMock = mockStatic( FileViewRecordCreator.class,
                withSettings().defaultAnswer( CALLS_REAL_METHODS ) )) {
            assertEquals( 5, FileViewRecordCreator.create( treeRecords, user0, FileViewRecordComparators.compareByObjectIdFileId() ).size() );
            creatorMock.verify( () -> FileViewRecordCreator.create(
                            recordCaptor.capture(), userCaptor.capture(), longCaptor.capture() ),
                    times( 5 ) );
        }

        assertIterableEquals( treeRecords, recordCaptor.getAllValues() );
        assertTrue( List.of( user0 ).containsAll( userCaptor.getAllValues() ) );
        assertIterableEquals( indexes, longCaptor.getAllValues() );
    }
}