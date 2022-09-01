package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.TreeAndFileView;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.exceptions.InsertFailureException;
import com.ericgha.docuCloud.repository.TreeRepository;
import com.ericgha.docuCloud.repository.testtool.file.RandomFileGenerator;
import com.ericgha.docuCloud.repository.testtool.file.RandomFileGenerator.FileDtoAndData;
import com.ericgha.docuCloud.testconainer.EnableMinioTestContainerContextCustomizerFactory.EnableMinioTestContainer;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnablePostgresTestContainer;
import org.jooq.DSLContext;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.enums.ObjectType.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EnablePostgresTestContainer
@EnableMinioTestContainer
@ActiveProfiles(value = {"test", "s3", "dev"})
public class DocumentServiceIntTest {

    @Autowired
    private DocumentService documentService;

    @Autowired
    private S3FileStore s3FileStore;

    @Autowired
    private DSLContext dsl;

    @Autowired
    private TreeRepository treeRepository;

    private final RandomFileGenerator randomFileGenerator = new RandomFileGenerator();

    private final CloudUser user0 = CloudUser.builder()
            .userId( UUID.fromString( "1234567-89ab-cdef-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    private final CloudUser user1 = CloudUser.builder()
            .userId( UUID.fromString( "ffffffff-ffff-ffff-fedc-ba9876543210" ) )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    @BeforeEach
    void before() throws URISyntaxException, IOException {
        s3FileStore.listBuckets().flatMap( s3FileStore::deleteAllObjectsInBucket )
                .flatMap( s3FileStore::deleteBucket )
                // it seems that create bucket was being evaluated eagerly and sometimes would
                // fire before deleteBucket when using a simple then
                .then( Mono.defer( () -> s3FileStore.createBucketIfNotExists() ) ).block();
        Path schemaFile = Paths.get( this.getClass()
                .getClassLoader()
                .getResource( "tests-schema.sql" )
                .toURI() );
        String sql = Files.readString( schemaFile );
        // delete and rebuild tables
        Mono.from( dsl.query( sql ) ).block();
        // create a root dir for user0
        treeRepository.create(
                TreeDto.builder().objectType( ROOT )
                        .path( Ltree.valueOf( "" ) )
                        .build(),
                user0, dsl).block();
    }

    /*
    ------------------------------- Resources used ------------------------------
    # Key - : not used, * : referenced through fKey, X: used 
    Method                          TreeRepository     FileRepository   FileStore
    ls                                   X                   X              -
    fetchFirstPageFileVersions           *                   X              -
    fetchNextPage                        *                   X              -
    rmTreeObject                         X                   X              X     
    rmVersion                            *                   X              X
    createRoot                           X                   -              -
    createDir                            X                   -              -
    addFileVersion                       *                   X              X
    createFile                           X                   X              X
    mv                                   X                   -              -
    cp                                   X                   X              -
     */

    @Test
    @DisplayName("Create root returns treeDto")
    void createRootReturnsTreeDto() {
        // use user1 b/c user0's root autogenerated by beforeEach()
        Mono<TreeDto> rootDto = documentService.createRoot( user1 );
        StepVerifier.create( rootDto ).assertNext( created -> {
                    assertEquals( ROOT, created.getObjectType() );
                    assertEquals( "", created.getPathStr() );
                    assertEquals( user1.getUserId(), created.getUserId() );
                } )
                .verifyComplete();
    }

    @Test
    @DisplayName("Create root throws insertFailure exception when no record is inserted")
    void createRootThrows() {
        Mono<TreeDto> insert = documentService.createRoot( user1 );
        StepVerifier.create( insert.then( insert ) ).verifyError( InsertFailureException.class );
    }

    @Test
    @DisplayName("Create dir returns treeDto")
    void createDirReturnsTreeDto() {
        TreeDto dir0 = TreeDto.builder()
                .path( Ltree.valueOf("dir0") )
                .objectType( DIR )
                .build();
        StepVerifier.create( documentService.createDir( dir0, user0 ) )
                .assertNext( created -> {
                    assertEquals( dir0.getPathStr(), created.getPathStr() );
                    assertEquals( dir0.getObjectType(), created.getObjectType() );
                    assertEquals( user0.getUserId(), created.getUserId() );
                } )
                .verifyComplete();
    }

    @Test
    @DisplayName( "createDir throws Insertion error when failure to insert" )
    void createDirThrows() {
        TreeDto dir0 = TreeDto.builder()
                .path( Ltree.valueOf("dir0") )
                .objectType( DIR )
                .build();
        // user1 has no root folder, so this creates an orphan
        StepVerifier.create( documentService.createDir( dir0, user1 ) )
                .verifyError( InsertFailureException.class );
    }

    @Test
    @DisplayName( "createFile returns TreeAndFileView on successful file creation" )
    void createFileReturnsTreeAndFileViewOnSuccess() {
        TreeDto fileObj0 = TreeDto.builder().path(Ltree.valueOf("file0") )
                .objectType( FILE )
                .build();
        FileDtoAndData fileAndData = randomFileGenerator.generate();
        Mono<TreeAndFileView> insertReq = documentService.createFile( fileObj0, fileAndData.fileDto(), fileAndData.data(), user0 );
        StepVerifier.create( insertReq )
                .expectNextCount( 1 )
                .verifyComplete();
//        System.out.println("sdfsd");
    }


    @Test
    @DisplayName( "createFile throws InsertFailureException when no insert" )
    void createFileThrowsInsertFailureException() {
        // creates orphan
        TreeDto fileObj0 = TreeDto.builder().path(Ltree.valueOf("dir0.file0") )
                .objectType( FILE )
                .build();
        FileDtoAndData fileAndData = randomFileGenerator.generate();
        Mono<TreeAndFileView> insertReq = documentService.createFile( fileObj0, fileAndData.fileDto(), fileAndData.data(), user0 );
        StepVerifier.create( insertReq )
                .verifyError(InsertFailureException.class);
    }

    @Test
    @DisplayName( "addFileVersion returns TreeAndFileView on successful PUT" )
    void addFileVersionReturnsTreeAndFileView() {
        TreeDto fileObj0 = TreeDto.builder().path(Ltree.valueOf("file0") )
                .objectType( FILE )
                .build();
        FileDtoAndData fileAndData0 = randomFileGenerator.generate();
        FileDtoAndData fileAndData1 = randomFileGenerator.generate();
        // create the file
        Mono<TreeAndFileView> insertReq = documentService.createFile( fileObj0, fileAndData0.fileDto(), fileAndData0.data(), user0 )
                .map(TreeAndFileView::treeDto)
                .flatMap(fullTreeDto -> documentService.addFileVersion( fullTreeDto, fileAndData1.fileDto(), fileAndData1.data(), user0 ) );
        StepVerifier.create( insertReq )
                .expectNextCount( 1 )
                .verifyComplete();
    }

    @Test
    @DisplayName("Test jooq insert")
    void testInsert() {
        TreeDto fileObj0 = TreeDto.builder().path(Ltree.valueOf("file0") )
                .objectType( FILE )
                .build();
        var insert = documentService.test( fileObj0, user0 );
        StepVerifier.create( insert ).expectNextCount( 1 ).verifyComplete();
        System.out.println("sdfsd");
    }
}