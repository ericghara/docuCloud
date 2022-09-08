package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.converter.FileViewDtoToFileDto;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.TreeAndFileView;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.exceptions.IllegalObjectTypeException;
import com.ericgha.docuCloud.repository.FileRepository;
import com.ericgha.docuCloud.repository.TreeRepository;
import com.ericgha.docuCloud.repository.testtool.file.RandomFileGenerator;
import org.jooq.DSLContext;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.UUID;

import static com.ericgha.docuCloud.jooq.enums.ObjectType.DIR;

@RunWith( SpringRunner.class )
@ActiveProfiles(value = {"test", "s3", "dev"})
@SpringBootTest(classes={DocumentService.class, FileViewDtoToFileDto.class})
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



}
