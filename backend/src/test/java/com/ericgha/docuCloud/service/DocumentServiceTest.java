package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.converter.FileViewDtoToFileDto;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.TreeAndFileView;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.exceptions.IllegalObjectTypeException;
import com.ericgha.docuCloud.repository.FileRepository;
import com.ericgha.docuCloud.repository.TreeRepository;
import com.ericgha.docuCloud.repository.testtool.file.RandomFileGenerator;
import com.ericgha.docuCloud.repository.testtool.file.UpdateFailureException;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.enums.ObjectType.DIR;
import static com.ericgha.docuCloud.jooq.enums.ObjectType.FILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith( SpringRunner.class )
@ActiveProfiles(value = {"test", "s3", "dev"})
@SpringBootTest(classes={DocumentService.class, FileViewDtoToFileDto.class})
@ExtendWith(MockitoExtension.class)
public class DocumentServiceTest {

    @MockBean
    FileStore fileStoreMock;

    @MockBean
    FileRepository fileRepositoryMock;

    @MockBean
    TreeRepository treeRepositoryMock;

    @MockBean
    DSLContext dslContextMock;

    @MockBean
    JooqTransaction jooqTxMock;

    @Autowired
    DocumentService documentService;

    private final RandomFileGenerator randomFileGenerator = new RandomFileGenerator();
    private final CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    @Test
    @DisplayName( "createFile throws IllegalObjectTypeException when ObjectType not file" )
    void createFileThrowsIllegalObjectTypeExceptionWhenNotFile() {
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf("file0") )
                .objectType( DIR )
                .build();
        RandomFileGenerator.FileDtoAndData fileAndData = randomFileGenerator.generate();
        Mono<TreeAndFileView> insertReq = Mono.defer(() -> documentService.createFile( fileObj0, fileAndData.fileDto(), fileAndData.data(), user0 ) );
        StepVerifier.create( insertReq )
                .verifyError( IllegalObjectTypeException.class );
    }

    @Test
    @DisplayName( "createFile throws IllegalObjectTypeException when ObjectType is Null" )
    void createFileThrowsIllegalObjectTypeExceptionWhenNull() {
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf("file0") )
                .objectType( null )
                .build();
        RandomFileGenerator.FileDtoAndData fileAndData = randomFileGenerator.generate();
        Mono<TreeAndFileView> insertReq = Mono.defer(() -> documentService.createFile( fileObj0, fileAndData.fileDto(), fileAndData.data(), user0 ) );
        StepVerifier.create( insertReq )
                .verifyError( IllegalObjectTypeException.class );
    }

    @Test
    @DisplayName( "mv calls mvFile when source is file" )
    void mvCallsMvFileForFile() {
        doReturn( Mono.just(Long.MAX_VALUE) ).when(treeRepositoryMock).mvFile(any(TreeDto.class), any(Ltree.class), any(CloudUser.class) );

        var treeObj = TreeDto.builder().objectId( UUID.randomUUID() )
                .objectType( FILE )
                .path( Ltree.valueOf( "x" ) )
                .createdAt( OffsetDateTime.now() )
                .userId( user0.getUserId() )
                .build();
        var dest = Ltree.valueOf( "y" );
        documentService.mv( treeObj, dest, user0 ).as(StepVerifier::create)
                .expectNext( Long.MAX_VALUE )
                .verifyComplete();
    }

    @Test
    @DisplayName( "mv throws when mono empty returned" )
    void mvCallsMvThrows() {
        doReturn( Mono.empty() ).when(treeRepositoryMock).mvFile(any(TreeDto.class), any(Ltree.class), any(CloudUser.class) );

        var treeObj = TreeDto.builder().objectId( UUID.randomUUID() )
                .objectType( FILE )
                .path( Ltree.valueOf( "x" ) )
                .createdAt( OffsetDateTime.now() )
                .userId( user0.getUserId() )
                .build();
        var dest = Ltree.valueOf( "y" );
        documentService.mv( treeObj, dest, user0 ).as(StepVerifier::create)
                .verifyError( UpdateFailureException.class );
    }

    @Test
    @DisplayName( "mv calls mvDir when source is Dir" )
    void mvCallsMvDirForDir() {
        doReturn( Mono.just(Long.MAX_VALUE) ).when(treeRepositoryMock).mvDir(any(TreeDto.class), any(Ltree.class), any(CloudUser.class) );

        var treeObj = TreeDto.builder().objectId( UUID.randomUUID() )
                .objectType( DIR )
                .path( Ltree.valueOf( "x" ) )
                .createdAt( OffsetDateTime.now() )
                .userId( user0.getUserId() )
                .build();
        var dest = Ltree.valueOf( "y" );
        documentService.mv( treeObj, dest, user0 ).as(StepVerifier::create)
                .expectNext( Long.MAX_VALUE )
                .verifyComplete();
    }

    @Test
    @DisplayName( "mvDir throws UpdateFailureException when any error returned" )
    void mvCallsMvDirThrowsUpdateFailureExceptionOnAnyError() {
        doReturn( Mono.error(new DataAccessException( "test exception" ) ) )
                .when(treeRepositoryMock)
                .mvDir(any(TreeDto.class), any(Ltree.class), any(CloudUser.class) );

        var treeObj = TreeDto.builder().objectId( UUID.randomUUID() )
                .objectType( DIR )
                .path( Ltree.valueOf( "x" ) )
                .createdAt( OffsetDateTime.now() )
                .userId( user0.getUserId() )
                .build();
        var dest = Ltree.valueOf( "y" );
        documentService.mv( treeObj, dest, user0 ).as(StepVerifier::create)
                .verifyError( UpdateFailureException.class );
    }

    @Test
    @DisplayName( "mvDir throws UpdateFailureException when mvDir returns 0" )
    void mvCallsMvDirThrowsUpdateFailureExceptionWhenZeroUpdate() {
        doReturn( Mono.just(0L ) )
                .when(treeRepositoryMock)
                .mvDir(any(TreeDto.class), any(Ltree.class), any(CloudUser.class) );

        var treeObj = TreeDto.builder().objectId( UUID.randomUUID() )
                .objectType( DIR )
                .path( Ltree.valueOf( "x" ) )
                .createdAt( OffsetDateTime.now() )
                .userId( user0.getUserId() )
                .build();
        var dest = Ltree.valueOf( "y" );
        documentService.mv( treeObj, dest, user0 ).as(StepVerifier::create)
                .verifyError( UpdateFailureException.class );
    }
}
