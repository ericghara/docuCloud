package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.converter.FileViewDtoToFileDto;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeAndFileView;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.exceptions.DeleteFailureException;
import com.ericgha.docuCloud.exceptions.IllegalObjectTypeException;
import com.ericgha.docuCloud.exceptions.InsertFailureException;
import com.ericgha.docuCloud.exceptions.RecordNotFoundException;
import com.ericgha.docuCloud.repository.FileRepository;
import com.ericgha.docuCloud.repository.TreeRepository;
import com.ericgha.docuCloud.repository.testtool.file.FileTestQueries;
import com.ericgha.docuCloud.repository.testtool.file.RandomFileGenerator;
import com.ericgha.docuCloud.repository.testtool.file.RandomFileGenerator.FileDtoAndData;
import com.ericgha.docuCloud.repository.testtool.tree.TreeTestQueries;
import com.ericgha.docuCloud.testconainer.EnableMinioTestContainerContextCustomizerFactory.EnableMinioTestContainer;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnablePostgresTestContainer;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.ericgha.docuCloud.jooq.enums.ObjectType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

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
    private JooqTransaction jooqTx;

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
    void before(@Autowired TreeRepository treeRepository) throws URISyntaxException, IOException {
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
        jooqTx.withConnection( dsl -> dsl.query( sql ) ).publishOn( Schedulers.boundedElastic() ).block();
        // create a root dir for user0
        treeRepository.create(
                        TreeDto.builder().objectType( ROOT )
                                .path( Ltree.valueOf( "" ) )
                                .build(),
                        user0 )
                .publishOn( Schedulers.boundedElastic() )
                .block();
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
                .path( Ltree.valueOf( "dir0" ) )
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
    @DisplayName("createDir throws Insertion error when failure to insert")
    void createDirThrows() {
        TreeDto dir0 = TreeDto.builder()
                .path( Ltree.valueOf( "dir0" ) )
                .objectType( DIR )
                .build();
        // user1 has no root folder, so this creates an orphan
        StepVerifier.create( documentService.createDir( dir0, user1 ) )
                .verifyError( InsertFailureException.class );
    }

    @Test
    @DisplayName("createFile returns TreeAndFileView on successful file creation")
    void createFileReturnsTreeAndFileViewOnSuccess() {
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf( "file0" ) )
                .objectType( FILE )
                .build();
        FileDtoAndData fileAndData = randomFileGenerator.generate();
        Mono<TreeAndFileView> insertReq = documentService.createFile( fileObj0, fileAndData.fileDto(), fileAndData.data(), user0 );
        StepVerifier.create( insertReq )
                .expectNextCount( 1 )
                .verifyComplete();
    }

    @Test
    @DisplayName("createFile throws InsertFailureException when no insert")
    void createFileThrowsInsertFailureException() {
        // creates orphan
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf( "dir0.file0" ) )
                .objectType( FILE )
                .build();
        FileDtoAndData fileAndData = randomFileGenerator.generate();
        Mono<TreeAndFileView> insertReq = documentService.createFile( fileObj0, fileAndData.fileDto(), fileAndData.data(), user0 );
        StepVerifier.create( insertReq )
                .verifyError( InsertFailureException.class );
    }


    @Test
    @DisplayName("createFile rolls back tree insert on file insert failure")
    void createFileRollsBack(@Autowired TreeRepository treeRepository) {
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf( "file0" ) )
                .objectType( FILE )
                .build();
        Flux<ByteBuffer> data = randomFileGenerator.generate().data();
        // no data
        FileDto file = FileDto.builder().build();
        FileDtoAndData fileAndData = new FileDtoAndData( file, data );
        Mono<TreeAndFileView> insertReq = documentService.createFile( fileObj0, fileAndData.fileDto(), fileAndData.data(), user0 );
        StepVerifier.create( insertReq )
                .verifyError( InsertFailureException.class );
        treeRepository.ls( fileObj0, user0 )
                .as( StepVerifier::create )
                .expectNextCount( 0 )
                .verifyComplete();
    }


    @Test
    @DisplayName("addFileVersion returns TreeAndFileView on successful PUT")
    void addFileVersionReturnsTreeAndFileView(@Autowired TransactionalOperator txop) {
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf( "file0" ) )
                .objectType( FILE )
                .build();
        FileDtoAndData fileAndData0 = randomFileGenerator.generate();
        FileDtoAndData fileAndData1 = randomFileGenerator.generate();
        // create the file
        Mono<TreeAndFileView> insertReq = documentService.createFile( fileObj0, fileAndData0.fileDto(), fileAndData0.data(), user0 )
                .as( txop::transactional )
                .map( TreeAndFileView::treeDto )
                .flatMap( fullTreeDto -> documentService.addFileVersion( fullTreeDto, fileAndData1.fileDto(), fileAndData1.data(), user0 ) );
        StepVerifier.create( insertReq )
                .expectNextCount( 1 )
                .verifyComplete();
    }

    @Test
    @DisplayName("cp copies a file")
    void cpCopiesAFile(@Autowired TreeRepository treeRepository, @Autowired FileRepository fileRepository) {
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf( "file0" ) )
                .objectType( FILE )
                .build();
        TreeDto fileObj1 = TreeDto.builder()
                .path( Ltree.valueOf( "file1" ) )
                .build();
        FileDtoAndData fileAndData = randomFileGenerator.generate();
        Mono<TreeAndFileView> insertReq = documentService.createFile( fileObj0, fileAndData.fileDto(), fileAndData.data(), user0 );
        Mono<Void> cpReq = documentService.cp( fileObj0, fileObj1.getPath(), true, user0 );
        //create fileObj0 then, cp fileObj0 to fileObj1
        insertReq.then( cpReq ).as( StepVerifier::create )
                .verifyComplete();
        // get fullTreeObjects (with objectId)
        Flux<TreeDto> foundTreeObjs = Flux.concat( treeRepository.ls( fileObj0, user0 ),
                        treeRepository.ls( fileObj1, user0 ) )
                .cache();
        // use objectIds to find files linked to tree obj
        Flux<String> checksums = foundTreeObjs.flatMap( fullFileObj ->
                        fileRepository.lsNewestFilesFor( fullFileObj, Integer.MAX_VALUE, user0 ) )
                .map( FileViewDto::getChecksum );
        // verify treeObj paths
        foundTreeObjs.map( TreeDto::getPath ).as( StepVerifier::create )
                .expectNext( fileObj0.getPath(), fileObj1.getPath() )
                .verifyComplete();
        // verify linked file checksums
        checksums.as( StepVerifier::create )
                .expectNext( fileAndData.fileDto().getChecksum(),
                        fileAndData.fileDto().getChecksum() )
                .verifyComplete();
    }

    @Test
    @DisplayName("cp throws InsertFailureException when no treeObj copied")
    void cpThrowsWhenSourceNotFound() {
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf( "file0" ) )
                .objectType( FILE )
                .build();
        TreeDto fileObj1 = TreeDto.builder()
                .path( Ltree.valueOf( "file1" ) )
                .build();
        // source invalid as it was never inserted
        Mono<Void> cpReq = documentService.cp( fileObj0, fileObj1.getPath(), true, user0 );
        cpReq.as( StepVerifier::create )
                .verifyError( InsertFailureException.class );
    }

    @Test
    @DisplayName("cp copies an empty dir")
    void cpCopiesAnEmptyDir(@Autowired TreeRepository treeRepository) {
        TreeDto dir0 = TreeDto.builder().path( Ltree.valueOf( "dir0" ) )
                .objectType( DIR )
                .build();
        TreeDto dir1 = TreeDto.builder()
                .path( Ltree.valueOf( "dir1" ) )
                .build();
        Mono<TreeDto> insertReq = documentService.createDir( dir0, user0 );
        Mono<Void> cpReq = documentService.cp( dir0, dir1.getPath(), true, user0 );
        //create dir0 then, cp dir0 to dir1
        insertReq.then( cpReq ).as( StepVerifier::create )
                .verifyComplete();
        Flux<Ltree> foundDirs = Flux.concat( treeRepository.ls( dir0, user0 ),
                        treeRepository.ls( dir1, user0 ) )
                .map( TreeDto::getPath );
        foundDirs.as( StepVerifier::create )
                .expectNext( dir0.getPath(), dir1.getPath() )
                .verifyComplete();
    }

    @Test
    @DisplayName("cp copies a non-empty dir")
    void cpCopiesNonEmptyDir(@Autowired TreeRepository treeRepository,
                             @Autowired FileRepository fileRepository) {
        TreeDto dir0 = TreeDto.builder().path( Ltree.valueOf( "dir0" ) )
                .objectType( DIR )
                .build();
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf( "dir0.file0" ) )
                .objectType( FILE )
                .build();
        FileDtoAndData fileAndData = randomFileGenerator.generate();
        var doInsert = Flux.concat( documentService.createDir( dir0, user0 ),
                documentService.createFile( fileObj0, fileAndData.fileDto(), fileAndData.data(), user0 ) );
        // create dir and file
        doInsert.as( StepVerifier::create )
                .expectNextCount( 2 )
                .verifyComplete();
        TreeDto destDir = TreeDto.builder().path( Ltree.valueOf( ( "dir1" ) ) )
                .build();
        // copy to destination
        var doCopy = documentService.cp( dir0, destDir.getPath(), false, user0 );
        doCopy.as( StepVerifier::create )
                .verifyComplete();
        // ls source and dest, should be 2 + 2 tree records
        Flux<TreeDto> foundDtos = Flux.fromIterable( List.of( dir0, destDir ) )
                .flatMap( treeDto -> treeRepository.ls( treeDto, user0 ) )
                .cache();
        foundDtos.as( StepVerifier::create )
                .expectNextCount( 4 )
                .verifyComplete();
        // filter only files, and use the full treeRecord (with objectId) to get fileViews
        Flux<String> foundChecksums = foundDtos.filter( treeDto -> treeDto.getObjectType() == FILE )
                .flatMap( treeDto -> fileRepository.lsNewestFilesFor( treeDto, Integer.MAX_VALUE, user0 ) )
                .map( FileViewDto::getChecksum );
        // verify checksums
        foundChecksums.as( StepVerifier::create )
                .expectNext( fileAndData.fileDto().getChecksum(), fileAndData.fileDto().getChecksum() )
                .verifyComplete();
    }

    @Test
    @DisplayName("ls of a root")
    void lsOfRoot(@Autowired TreeRepository treeRepository,
                  @Autowired FileRepository fileRepository) {
        TreeDto root = TreeDto.builder()
                .path( Ltree.valueOf( "" ) )
                .objectType( ROOT )
                .build();
        TreeDto dir0 = TreeDto.builder().path( Ltree.valueOf( "dir0" ) )
                .objectType( DIR )
                .build();
        TreeDto fileObj0 = TreeDto.builder().path( Ltree.valueOf( "file0" ) )
                .objectType( FILE )
                .build();
        FileDtoAndData fileAndData = randomFileGenerator.generate();
        // add dir0 and file0 to root
        var doInsert = Flux.concat( documentService.createDir( dir0, user0 ),
                documentService.createFile( fileObj0, fileAndData.fileDto(), fileAndData.data(), user0 ) );
        // create dir and file
        doInsert.as( StepVerifier::create )
                .expectNextCount( 2 )
                .verifyComplete();
        // fetch expected records
        TreeDto expectedDirObj = treeRepository.ls( dir0, user0 ).blockFirst();
        TreeDto expectedFileObj = treeRepository.ls( fileObj0, user0 ).blockFirst();
        FileViewDto expectedFileView = fileRepository.lsNewestFileFor( expectedFileObj, user0 )
                .block();
        // Generate expected TreeAndFileViews
        List<TreeAndFileView> expectedSequence = Stream.of( new TreeAndFileView( expectedDirObj, null ),
                        new TreeAndFileView( expectedFileObj, expectedFileView ) )
                .sorted()
                .toList();
        Flux<TreeAndFileView> foundTreeAndFileView = documentService.ls( root, user0 )
                .sort();
        // verify
        foundTreeAndFileView.as( StepVerifier::create )
                .expectNextSequence( expectedSequence )
                .verifyComplete();
    }

    @Test
    @DisplayName("ls throws IllegalObjectType exception when Source ObjectType is FILE")
    void lsThrowsWhenSourceObjectTypeIsFile() {
        TreeDto source = TreeDto.builder()
                .path( Ltree.valueOf( "file0" ) )
                .objectType( FILE ).build();
        assertThrows( IllegalObjectTypeException.class, () -> documentService.ls( source, user0 ) );
    }

    @Test
    @DisplayName("ls throws IllegalObjectType exception if the actual source is a file (spoofed)")
    void lsThrowsWhenActualSourceIsAFileSpoofed(@Autowired TreeRepository treeRepository) {
        TreeDto source = TreeDto.builder()
                .path( Ltree.valueOf( "file0" ) )
                .objectType( FILE ).build();
        TreeDto spoofedSource = TreeDto.builder()
                .path( source.getPath() )
                .objectType( DIR ) // allows source to bypass initial validation
                .build();
        var insThenLs = treeRepository.create( source, user0 )
                .thenMany( documentService.ls( spoofedSource, user0 ) );
        insThenLs.as( StepVerifier::create ).verifyError( IllegalObjectTypeException.class );
    }

    @Test
    @DisplayName("ls throws RecordNotFoundException when no record returned")
    void lsThrowsWhenNoRecordIsReturned() {
        TreeDto source = TreeDto.builder()
                .path( Ltree.valueOf( "dir0" ) )
                .objectType( DIR ).build();
        documentService.ls( source, user0 ).as( StepVerifier::create )
                .verifyError( RecordNotFoundException.class );
    }

    @Nested
    @DisplayName("File Version tests")
    @EnablePostgresTestContainer
    @EnableMinioTestContainer
    @ActiveProfiles(value = {"test", "s3", "dev"})
    class VersionTests {

        private static final int NUM_VERSIONS = 15;
        private TreeDto file0;
        private final CloudUser user = user0;
        private final LinkedList<FileViewDto> fileViews = new LinkedList<>();  // need a slice-able queue

        @BeforeEach
        void before() {
            TreeDto file0 = TreeDto.builder()
                    .path( Ltree.valueOf( "file0" ) )
                    .objectType( FILE ).build();
            Mono<Void> add15 = this.createFile( file0 ).map( TreeAndFileView::treeDto )
                    .then( Mono.defer( () -> addFileVersions( NUM_VERSIONS - 1 ) ) );
            add15.as( StepVerifier::create )
                    .verifyComplete();
        }

        private Mono<TreeAndFileView> createFile(TreeDto toCreate) {
            // this is pretty fragile, in theory could create files from many treeDtos and add to list
            // flagging but not fixing as this is a test class
            final FileDtoAndData fileAndData = randomFileGenerator.generate();
            return documentService.createFile( toCreate, fileAndData.fileDto(), fileAndData.data(), user )
                    .doOnNext( n -> {
                        fileViews.addFirst( n.fileViewDto() );
                        this.file0 = n.treeDto();
                    } );
        }

        private Mono<TreeAndFileView> addVersion() {
            final FileDtoAndData fileAndData = randomFileGenerator.generate();
            return documentService.addFileVersion(
                            file0, fileAndData.fileDto(), fileAndData.data(), user )
                    .doOnNext( newVersion -> fileViews.addFirst( newVersion.fileViewDto() ) );
        }

        private Mono<Void> addFileVersions(int n) {
            return Flux.concat( IntStream.range( 0, n ).boxed()
                            .map( x -> addVersion() )
                            .toArray( Mono[]::new )
                    )
                    .then();
        }

        @Test
        @DisplayName("fetchFirstPageFileVersions returns first page of records")
        void fetchFirstPageFileVersionsReturnsExpected() {
            final int LIMIT = 7;
            var firstPage = documentService.fetchFirstPageFileVersions( file0, LIMIT, user )
                    .block();
            Iterable<FileViewDto> expectedFiles = fileViews.subList( 0, 7 );
            firstPage.numVersions().as( StepVerifier::create )
                    .expectNext( (long) fileViews.size() )
                    .verifyComplete();
            firstPage.firstPage().as( StepVerifier::create )
                    .expectNextSequence( expectedFiles )
                    .verifyComplete();
        }

        @Test
        @DisplayName("fetch next page fetches expected records")
        void fetchNextPageReturnsExpected() {
            final int LIMIT = 7;
            FileViewDto lastRecordOnFirstPage = fileViews.get( 6 );
            Iterable<FileViewDto> expectedNextPage = fileViews.subList( 7, 7 + LIMIT );
            Flux<FileViewDto> secondPage = documentService.fetchNextPage( lastRecordOnFirstPage, LIMIT, user );
            secondPage.as( StepVerifier::create )
                    .expectNextSequence( expectedNextPage )
                    .verifyComplete();
        }

        @Test
        @DisplayName("rmVersion removes expected file and data")
        void rmVersionRemovesExpected(@Autowired FileTestQueries fileTestQueries) {
            FileViewDto toRemove = fileViews.get( 2 );
            Mono<Void> rm = documentService.rmVersion( file0, toRemove, user );
            rm.as( StepVerifier::create )
                    .verifyComplete();
            // deleted from fileStore
            Flux<ByteBuffer> fetchData = documentService.getFileData( toRemove, user );
            fetchData.as( StepVerifier::create )
                    .verifyError( NoSuchKeyException.class );
            // deleted from FileView
            Mono<FileViewDto> ls = fileTestQueries.fetchFileViewDto( toRemove );
            ls.as( StepVerifier::create )
                    .expectNextCount( 0 )
                    .verifyComplete();
        }

        @Test
        @DisplayName("rmTreeObject deletes FileObject from Tree Files from FileView and data from FileStore")
        void rmTreeObjectRemovesExpected(@Autowired FileTestQueries fileTestQueries,
                                         @Autowired TreeTestQueries treeTestQueries,
                                         @Autowired FileViewDtoToFileDto fileViewToFile) {
            Mono<Void> rm = documentService.rmTreeObject( file0, false, user );
            rm.as( StepVerifier::create ).verifyComplete();
            assertNull( treeTestQueries.getByObjectId( file0.getObjectId() ) );
            fileTestQueries.fetchRecordsByObjectId( file0.getObjectId() ).as( StepVerifier::create )
                    .expectNextCount( 0 )
                    .verifyComplete();
            fileViews.stream().map( fileViewToFile::convert )
                    .forEach( file -> documentService.getFileData( file, user )
                            .as( StepVerifier::create )
                            .verifyError( NoSuchKeyException.class ) );
        }

        @Test
        @DisplayName("rmTreeObject rolls back when document service returns exception")
        void rmTreeObjectRollsBack(@Autowired TreeRepository treeRepository,
                                   @Autowired FileRepository fileRepository) {
            FileStore fileStoreMock = Mockito.mock( FileStore.class );
            // fileStore throws exception on delete
            when( fileStoreMock.deleteFiles( any(), any() ) ).thenReturn( Mono.error( new DeleteFailureException() ) );
            // instantiate a document service using mock
            DocumentService documentService = new DocumentService( fileStoreMock, fileRepository, treeRepository, jooqTx );
            Mono<Void> rm = documentService.rmTreeObject( file0, false, user );
            // will fail in terminal step, deleting from fileStore
            rm.as( StepVerifier::create )
                    .verifyError( DeleteFailureException.class );
            // delete from tree rolled back
            treeRepository.ls( file0, user )
                    .as( StepVerifier::create )
                    .expectNext( file0 )
                    .verifyComplete();
            // delete from fileRepository rolled back
            fileRepository.lsNewestFilesFor( file0, fileViews.size(), user )
                    .as( StepVerifier::create )
                    .expectNextSequence( fileViews )
                    .verifyComplete();
        }
    }
}