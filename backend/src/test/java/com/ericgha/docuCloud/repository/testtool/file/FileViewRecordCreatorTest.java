package com.ericgha.docuCloud.repository.testtool.file;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
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

class FileViewDtoCreatorTest {

    private final CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    @Test
    @DisplayName("create(TreeDto, CloudUser, long) generates expected FileViewDto")
    void createSingleGeneratesExpected() {
        Long index = new SplittableRandom().nextLong();
        UUID objectId = UUID.randomUUID();

        TreeDto treeDto = TreeDto.builder()
                .objectId( objectId )
                .build();

        FileViewDto fileViewDto = FileViewDtoCreator.create( treeDto, user0, index );
        assertEquals( objectId, fileViewDto.getObjectId() );
        assertNotNull( fileViewDto.getFileId() );
        assertEquals( user0.getUserId(), fileViewDto.getUserId() );
        assertNull( fileViewDto.getUploadedAt() );
        assertNull( fileViewDto.getLinkedAt() );
        assertEquals( "file" + index, fileViewDto.getChecksum() );
        assertEquals( index, fileViewDto.getSize() );
    }

    @Test
    @DisplayName(( "create(List<TreeDto>, CloudUser, long) generates correct number of records and calls create(TreeDto, CloudUser, long)" +
            "5 times with expected parameters" ))
    void testCreate() {
        List<Long> indexes = LongStream.range( 0, 5 )
                .boxed()
                .toList();
        List<TreeDto> treeDtos = indexes.stream()
                .map( l -> TreeDto.builder()
                        .objectId( UUID.randomUUID() )
                        .build() )
                .toList();

        ArgumentCaptor<TreeDto> recordCaptor = ArgumentCaptor.forClass( TreeDto.class );
        ArgumentCaptor<CloudUser> userCaptor = ArgumentCaptor.forClass( CloudUser.class );
        ArgumentCaptor<Long> longCaptor = ArgumentCaptor.forClass( Long.class );

        try (MockedStatic<FileViewDtoCreator> creatorMock = mockStatic( FileViewDtoCreator.class,
                withSettings().defaultAnswer( CALLS_REAL_METHODS ) )) {
            assertEquals( 5, FileViewDtoCreator.create( treeDtos, user0 ).size() );
            creatorMock.verify( () -> FileViewDtoCreator.create(
                            recordCaptor.capture(), userCaptor.capture(), longCaptor.capture() ),
                    times( 5 ) );
        }

        assertIterableEquals( treeDtos, recordCaptor.getAllValues() );
        assertTrue( List.of( user0 ).containsAll( userCaptor.getAllValues() ) );
        assertIterableEquals( indexes, longCaptor.getAllValues() );
    }
}