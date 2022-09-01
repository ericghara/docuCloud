package com.ericgha.docuCloud.repository.testtool.file;

import com.ericgha.docuCloud.converter.FileViewDtoToFileDto;
import com.ericgha.docuCloud.converter.FileViewDtoToTreeJoinFileDto;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.dto.TreeJoinFileDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.repository.testtool.tree.TestFileTree;
import com.ericgha.docuCloud.util.comparator.FileViewDtoComparators;
import com.ericgha.docuCloud.util.comparator.TreeDtoComparators;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.convert.converter.Converter;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class TestFilesTest {

    @Mock
    FileTestQueries fileQueries;
    @Mock
    TestFileTree tree;

    TestFiles testFiles;
    Converter<FileViewDto, FileDto> fileViewToFile = new FileViewDtoToFileDto();
    Converter<FileViewDto, TreeJoinFileDto> fileViewToTreeJoinFile = new FileViewDtoToTreeJoinFileDto();

    Comparator<TreeDto> treeComp = TreeDtoComparators.compareByObjectId();

    Comparator<FileViewDto> fileViewComp = FileViewDtoComparators.compareByObjectIdFileId();

    CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    TreeDto.TreeDtoBuilder treeObjectPrototype = TreeDto.builder().objectId( UUID.randomUUID() )
            .objectType( ObjectType.FILE )
            .userId( user0.getUserId() )
            .createdAt( OffsetDateTime.now() );

    FileViewDto.FileViewDtoBuilder fileViewProtoType = FileViewDto.builder()
            .userId( user0.getUserId() )
            .uploadedAt( OffsetDateTime.now() )
            .linkedAt( OffsetDateTime.now() )
            .checksum( "fileRes0" )
            .size( 0L );

    @BeforeEach
    private void beforeEach() {
        testFiles = new TestFiles( tree, fileQueries, fileViewToFile );
    }

    @Test
    @DisplayName( "InsertFileViewDto creates expected return value on linking object to new file" )
    void insertFileViewDtoCreatesExpectedReturnNewFile() {
        TreeDto obj0 = treeObjectPrototype.objectId( UUID.randomUUID() ).path( Ltree.valueOf( "fileObj0" ) ).build();
        String checksum = "fileRes0";

        Mockito.when(tree.getOrigRecord(any(String.class))).thenReturn( obj0 );
        Mockito.when(fileQueries.createFileWithLink(any(FileViewDto.class) ) ).thenAnswer( a -> Mono.just(a.getArgument( 0 ) ) );

        TestFiles.ObjectFileLink ofl = testFiles.insertFileView( obj0.getPathStr(), checksum );

        assertEquals(obj0, ofl.treeDto() );
        FileViewDto foundFileView = ofl.fileViewDto();

        assertEquals( obj0.getObjectId(), foundFileView.getObjectId() );
        assertNotNull(foundFileView.getFileId() );
        assertEquals(user0.getUserId(), foundFileView.getUserId() );
        assertNull(foundFileView.getLinkedAt() );
        assertNull(foundFileView.getUploadedAt() );
        assertEquals( checksum, foundFileView.getChecksum() );
        assertEquals( 0L, foundFileView.getSize() );
    }

    @Test
    @DisplayName( "InsertFileViewDto creates expected return value on linking an object to an existing file" )
    void insertFileViewDtoCreatesExpectedReturnExistingFile() {
        String pathStr0 = "fileObj0";
        String pathStr1 = "fileObj1";
        String checksum = "fileRes0";
        TreeDto obj0 = treeObjectPrototype.objectId( UUID.randomUUID() ).path( Ltree.valueOf( pathStr0 ) ).build();
        TreeDto obj1 = treeObjectPrototype.objectId( UUID.randomUUID() ).path( Ltree.valueOf( pathStr1 ) ).build();
        FileViewDto fileView0 = fileViewProtoType.fileId( UUID.randomUUID() ).objectId( obj0.getObjectId() ).checksum( checksum ).build();
        // spoof an existing link fileRes0 <-> fileObj0
        testFiles.recordNewFileViewDto( new TestFiles.ObjectFileLink( obj0, fileView0 ) );

        Mockito.when(tree.getOrigRecord(any(String.class))).thenReturn(obj1);
        Mockito.when(fileQueries.createLink(any(FileViewDto.class) ) )
                .thenReturn( Mono.just(fileViewToTreeJoinFile.convert(fileView0) ) );
        // note in reality this would return a new fileView but the point is, whatever is fetched is returned
        Mockito.when(fileQueries.fetchFileViewDto( any(UUID.class), any(UUID.class) ) )
                .thenAnswer( a -> Mono.just(fileView0) );
        ArgumentCaptor<FileViewDto> recordToInsertCapture = ArgumentCaptor.forClass(FileViewDto.class);
        // add a link b/t fileObj1 <-> fileRes0
        TestFiles.ObjectFileLink link1 = testFiles.insertFileView( pathStr1, checksum );

        verify(fileQueries).createLink(recordToInsertCapture.capture() );
        // assert link b/t fileObj1 <-> fileRes0
        assertEquals( obj1.getObjectId(), recordToInsertCapture.getValue().getObjectId(), "objectId" );
        assertEquals( fileView0.getFileId(), recordToInsertCapture.getValue().getFileId(), "fileId" );
        assertEquals( obj1.getUserId(), recordToInsertCapture.getValue().getUserId(), "userId" );
        assertEquals(obj1, link1.treeDto() );
        assertEquals( fileView0, link1.fileViewDto() );  // make sure whatever was fetched is returned
    }

    @Test
    @DisplayName( "RecordNewFileViewDto adds records to maps" )
    void recordNewFileViewDtoAddsRecordsToMaps() {
        String pathStr0 = "fileObj0";
        String checksum = "fileRes0";
        TreeDto obj0 = treeObjectPrototype.objectId( UUID.randomUUID() ).path( Ltree.valueOf( pathStr0 ) ).build();
        FileViewDto fileView0 = fileViewProtoType.fileId( UUID.randomUUID() ).objectId( obj0.getObjectId() ).checksum( checksum ).build();
        testFiles.recordNewFileViewDto( new TestFiles.ObjectFileLink( obj0, fileView0 ) );

        assertIterableEquals( List.of(fileView0), testFiles.getOrigFileViewsFor( pathStr0 ) );
        assertIterableEquals( List.of(obj0), testFiles.getOrigTreeObjectsFor( checksum ) );
        assertEquals( fileViewToFile.convert(fileView0), testFiles.getOrigFileFor( checksum ) );
    }
}
