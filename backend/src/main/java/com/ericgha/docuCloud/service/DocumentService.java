package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.SeekInitResponse;
import com.ericgha.docuCloud.dto.TreeAndFileView;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.dto.TreeJoinFileDto;
import com.ericgha.docuCloud.exceptions.DeleteFailureException;
import com.ericgha.docuCloud.exceptions.IllegalObjectTypeException;
import com.ericgha.docuCloud.exceptions.InsertFailureException;
import com.ericgha.docuCloud.exceptions.RecordNotFoundException;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.repository.FileRepository;
import com.ericgha.docuCloud.repository.TreeRepository;
import com.ericgha.docuCloud.repository.testtool.file.UpdateFailureException;
import com.ericgha.docuCloud.util.PublisherUtil;
import com.ericgha.docuCloud.util.validator.TreeDtoValidator;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Record3;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import static com.ericgha.docuCloud.jooq.enums.ObjectType.*;
import static com.ericgha.docuCloud.jooq.tables.Tree.TREE;

@Service
@Profile("(test & s3) | !test")
@RequiredArgsConstructor
@Slf4j
public class DocumentService {

    private final FileStore fileStore;
    private final FileRepository fileRepository;
    private final TreeRepository treeRepository;

    private final JooqTransaction jooqTrans;

    /**
     * Lists files and directories in a ROOT or DIR. Fetches FileViewDtos for all FILE objects, if an object is not a
     * FILE then a null fileViewDto is returned.  Returns a {@link TreeAndFileView} The TreeDto will never be null. The
     * FileViewDto must be null when the TreeDto is a DIR; The fileViewDto may be null for a FILE when no file versions
     * are linked to the TreeDto.
     *
     * @param source
     * @param cloudUser
     * @return {@link TreeAndFileView}
     * @throws IllegalObjectTypeException if source is a FILE or if a TreeDto with ObjectType = ROOT is returned (should
     *                                    never happen).
     * @throws RecordNotFoundException    if the source record could not be located
     */

    public Flux<TreeAndFileView> ls(TreeDto source, CloudUser cloudUser)
            throws IllegalObjectTypeException, RecordNotFoundException {
        TreeDtoValidator.mustBeOneOfObjectTypes( source, ROOT, DIR );
        Flux<TreeDto> hotStream = treeRepository.ls( source, cloudUser ).cache( 0 );
        Mono<Void> takeParent = this.firstMustBeRootOrFile( hotStream );
        Flux<TreeAndFileView> dirsAndFiles = this.zipWithFileViewDtos( hotStream, cloudUser );
        return takeParent.thenMany( dirsAndFiles );
    }


    public Mono<SeekInitResponse> fetchFirstPageFileVersions(TreeDto treeDto, int limit, CloudUser cloudUser) {
        var count = fileRepository.countFilesFor( treeDto, cloudUser );
        var fileList = fileRepository.lsNewestFilesFor( treeDto, limit, cloudUser );
        return Mono.just( new SeekInitResponse( fileList, count ) );
    }


    public Flux<FileViewDto> fetchNextPage(FileViewDto lastFileView, int limit, CloudUser cloudUser) {
        return fileRepository.lsNextFilesFor( lastFileView, limit, cloudUser );
    }

    public Mono<Void> rmTreeObject(TreeDto record, @NonNull Boolean recursive, CloudUser cloudUser) {
        Flux<TreeDto> rmTree;
        if (recursive) {
            rmTree = treeRepository.rmDirRecursive( record, cloudUser );
        } else {
            rmTree = treeRepository.rmNormal( record, cloudUser )
                    .flux();
        }
        Mono<List<UUID>> versionsToDelete = rmTree.map( TreeDto::getObjectId )
                .flatMap( objectId -> fileRepository.rmEdgesFrom( objectId, cloudUser ) )
                .filter( record2 -> record2.get( "orphan", Boolean.class ) )
                .map( record2 -> record2.get( "file_id", UUID.class ) )
                .collectList();
        return fileStore.deleteFiles( versionsToDelete, cloudUser ).as( jooqTrans::inTransaction );
    }


    public <T extends FileDto> Mono<Void> rmVersion(TreeDto treeDto, T fileDto, CloudUser cloudUser) {
        TreeJoinFileDto record = TreeJoinFileDto.builder().objectId( treeDto.getObjectId() )
                .fileId( fileDto.getFileId() )
                .build();
        // This will always flat map to 1 UUID, however delete files is designed to take requests of multiple files
        Mono<List<UUID>> versionsToDelete = PublisherUtil.requireNext( fileRepository.rmEdge( record, cloudUser ),
                        e -> new DeleteFailureException( "FileRepository", e ) )
                .filter( record2 -> record2.get( "orphan", Boolean.class ) )
                .map( record2 -> record2.get( "file_id", UUID.class ) )
                .flux()
                .collectList();
        return fileStore.deleteFiles( versionsToDelete, cloudUser );
    }

    /**
     * treeDto only requires: path field and ObjectType.  ObjectType must be FILE fileDto only requires: checksum and
     * size. Extra fields for both will be ignored.
     *
     * @param treeDto   required fields {@code path} and {@code objectType}
     * @param fileDto   required fields {@code checksum} and {@code size}
     * @param data      data for first file version
     * @param cloudUser user credentials
     * @return {@code TreeAndFileView}
     * @throws IllegalObjectTypeException if the treeDto objectType is FILE
     * @throws InsertFailureException     if no records are inserted into either the Tree or File tables
     */

    public <T extends FileDto> Mono<TreeAndFileView> createFile(@NonNull TreeDto treeDto,
                                                                @NonNull T fileDto,
                                                                @NonNull Flux<ByteBuffer> data,
                                                                @NonNull CloudUser cloudUser) throws IllegalObjectTypeException, InsertFailureException {
        TreeDtoValidator.mustBeObjectType( treeDto, FILE );

        return PublisherUtil.requireNext(
                        treeRepository.create( treeDto, cloudUser ),
                        e -> new InsertFailureException( "TreeRepository", e ) )
                .zipWhen( fullTreeDto ->
                                PublisherUtil.requireNext(
                                        fileRepository.createFileFor( fullTreeDto, fileDto, cloudUser ),
                                        e -> new InsertFailureException( "FileRepository", e ) ),
                        TreeAndFileView::new )
                .flatMap( treeAndFileView -> this.putDocumentIfFile( treeAndFileView, data, cloudUser ) )
                .as( jooqTrans::inTransaction );
    }

    // The only Information used from treeDto is objectId.  The only
    // information used from fileDto is checksum and size

    public <T extends FileDto> Mono<TreeAndFileView> addFileVersion(@NonNull TreeDto treeDto,
                                                                    @NonNull T fileDto,
                                                                    @NonNull Flux<ByteBuffer> data,
                                                                    @NonNull CloudUser cloudUser) throws InsertFailureException, IllegalObjectTypeException {
        TreeDtoValidator.mustBeObjectType( treeDto, FILE );
        return PublisherUtil.requireNext(
                        fileRepository.createFileFor( treeDto, fileDto, cloudUser ),
                        e -> new InsertFailureException( "FileRepository", e ) )
                .map( fileViewDto -> new TreeAndFileView( treeDto, fileViewDto ) )
                .flatMap( treeAndFileView -> this.putDocumentIfFile( treeAndFileView, data, cloudUser ) );
    }

    public <T extends FileDto> Flux<ByteBuffer> getFileData(T fileDto, CloudUser cloudUser) throws NoSuchKeyException {
        return fileStore.getFile( fileDto, cloudUser );
    }

    public Mono<TreeDto> createDir(TreeDto treeDto, CloudUser cloudUser) throws NullPointerException, IllegalArgumentException {
        TreeDtoValidator.mustBeObjectType( treeDto, DIR );
        return PublisherUtil.requireNext( treeRepository.create( treeDto, cloudUser ),
                e -> new InsertFailureException( "TreeRepository - DIR", e ) );
    }

    public Mono<TreeDto> createRoot(CloudUser cloudUser) throws InsertFailureException {
        TreeDto root = TreeDto.builder().objectType( ROOT )
                .path( Ltree.valueOf( "" ) )
                .build();
        return PublisherUtil.requireNext( treeRepository.create( root, cloudUser ),
                e -> new InsertFailureException( "TreeRepository - ROOT", e ) );
    }


    public Mono<Long> mv(TreeDto source, Ltree destination, CloudUser cloudUser) {
        var objectType = TreeDtoValidator.getOrThrow( source.getObjectType(), "objectType" );
        String exceptionMsg;
        Mono<Long> mvMono;
        if (objectType == FILE) {
            mvMono = treeRepository.mvFile( source, destination, cloudUser );
            exceptionMsg = "Update failure exception: mv file";
        } else if (objectType == DIR) {
            mvMono = treeRepository.mvDir( source, destination, cloudUser );
            exceptionMsg = "Update failure exception: mv dir";
        } else {
            return Mono.error( new IllegalArgumentException( String.format( "Cannot move source type: %s", objectType ) ) );
        }
        return mvMono.as( mono -> PublisherUtil.requireNext( mono,
                        e -> new UpdateFailureException( exceptionMsg, e ) ) )
                .as( mono -> PublisherUtil.requireNonZero( mono,
                        () -> new UpdateFailureException( exceptionMsg ) ) );
    }


    public Mono<Void> cp(TreeDto source, Ltree destination, boolean onlyNewestVer, CloudUser cloudUser) {
        return this.cpTreeOperations( source, destination, cloudUser )
                .flatMap( record3 -> this.cpFileOperations( record3, onlyNewestVer, cloudUser ) )
                .as( jooqTrans::inTransaction )
                .then();
    }

    private Flux<Record3<UUID, UUID, ObjectType>> cpTreeOperations(TreeDto source, Ltree destination,
                                                                   CloudUser cloudUser) {
        var objectType = TreeDtoValidator.getOrThrow( source.getObjectType(), "objectType" );
        Flux<Record3<UUID, UUID, ObjectType>> copied;
        if (objectType == FILE) {
            copied = treeRepository.cpFile( source, destination, cloudUser ).flux();
        } else if (objectType == DIR) {
            copied = treeRepository.cpDir( source, destination, cloudUser );
        } else {
            throw new IllegalArgumentException( String.format( "Cannot copy source type: %s", objectType ) );
        }
        return copied
                .as( flux -> PublisherUtil.requireNonEmpty( flux, e -> new InsertFailureException( "Unable to copy treeObject", e ) ) )
                .filter( record3 -> record3.get( TREE.OBJECT_TYPE ) == FILE );
    }

    // record3 probably should have a dto...
    // source_id, destination_id, objectType (should only receive file objectTypes)
    private Mono<Long> cpFileOperations(Record3<UUID, UUID, ObjectType> record3, boolean onlyNewestVer,
                                        CloudUser cloudUser) {
        var sourceId = record3.get( "source_id", UUID.class );
        var destId = record3.get( "destination_id", TREE.OBJECT_ID.getType() );
        if (onlyNewestVer) {
            return fileRepository.cpNewestFile( sourceId, destId, cloudUser );
        }
        return fileRepository.cpAllFiles( sourceId, destId, cloudUser );
    }

    /**
     * Takes first element, if it's not a dir or file throw.  If the flux emits complete without providing an element
     * this throws.
     *
     * @param flux
     * @return
     * @throws IllegalObjectTypeException if not a ROOT or DIR
     * @throws RecordNotFoundException    if complete emitted without a next signal.
     */
    private Mono<Void> firstMustBeRootOrFile(Flux<TreeDto> flux) throws IllegalObjectTypeException, RecordNotFoundException {
        return flux.take( 1 )
                .doOnNext( treeDto ->
                        TreeDtoValidator.mustBeOneOfObjectTypes( treeDto, ROOT, DIR )
                )
                .hasElements().doOnNext( b -> {
                    if (!b) {
                        throw new RecordNotFoundException();
                    }
                } )
                .then();
    }

    /**
     * Fetches FileViewDtos for all FILE objects, if an object is not a FILE then a null fileViewDto is returned.
     * Returns a {@link TreeAndFileView} The TreeDto will never be null. The FileViewDto must be null when the TreeDto
     * is a DIR; The fileViewDto may be null for a FILE when no file versions are linked to the TreeDto.
     *
     * @param flux
     * @param cloudUser
     * @return
     */
    private Flux<TreeAndFileView> zipWithFileViewDtos(Flux<TreeDto> flux, CloudUser cloudUser) {
        return flux.flatMap( treeDto -> {
            ObjectType objectType = TreeDtoValidator.mustBeOneOfObjectTypes( treeDto, FILE, DIR );
            if (objectType == DIR) {
                return Mono.just( new TreeAndFileView( treeDto, null ) );
            } else {
                return fileRepository.lsNewestFileFor( treeDto, cloudUser )
                        .map( fileViewDto -> new TreeAndFileView( treeDto, fileViewDto ) )
                        // while uncommon a fileObject without any files is possible
                        .switchIfEmpty( Mono.just( new TreeAndFileView( treeDto, null ) ) );
            }
        } );
    }

    private Mono<TreeAndFileView> putDocumentIfFile(TreeAndFileView treeAndFileView, Flux<ByteBuffer> data, CloudUser cloudUser) {
        FileViewDto fileView = treeAndFileView.fileViewDto();
        TreeDto treeDto = treeAndFileView.treeDto();
        Supplier<Mono<TreeAndFileView>> returnVal = () -> Mono.defer( () -> Mono.just( treeAndFileView ) );
        if (Objects.isNull( fileView )) {  // no op on ROOT and DIR
            TreeDtoValidator.mustBeOneOfObjectTypes( treeDto, ROOT, DIR );
            return returnVal.get();
        }
        TreeDtoValidator.mustBeObjectType( treeDto, FILE );
        return fileStore.putFile( data, fileView, cloudUser ).then( returnVal.get() );
    }
}
