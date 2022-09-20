package com.ericgha.docuCloud.controller;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.SeekInitResponse;
import com.ericgha.docuCloud.dto.TreeAndFileView;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.exceptions.DeleteFailureException;
import com.ericgha.docuCloud.exceptions.IllegalObjectTypeException;
import com.ericgha.docuCloud.exceptions.InsertFailureException;
import com.ericgha.docuCloud.exceptions.RecordNotFoundException;
import com.ericgha.docuCloud.repository.testtool.file.UpdateFailureException;
import com.ericgha.docuCloud.service.DocumentService;
import com.ericgha.docuCloud.util.StatusCodeMapper;
import lombok.RequiredArgsConstructor;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.context.annotation.Profile;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.enums.ObjectType.FILE;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@RequiredArgsConstructor
@Profile("(test & s3) | !test")
@RequestMapping("api/document")
public class DocumentController {

    private final DocumentService documentService;

     /*
    ---------------------------------------- Resources used -------------------------------------
    # Key - : not used, * : referenced through fKey, X: used
    HTTP Method     Method                          TreeRepository     FileRepository   FileStore
    GET             ls                                   X                   X              -
    GET             fetchFirstPageFileVersions           *                   X              -
    GET             fetchNextPage                        *                   X              -
    GET             getFileData                          -                   -              X
    DELETE          rmTreeObject                         X                   X              X
    DELETE          rmVersion                            *                   X              X
    POST            createRoot                           X                   -              -
    POST            createDir                            X                   -              -
    POST            addFileVersion                       *                   X              X
    POST            createFile                           X                   X              X
    PATCH           mv                                   X                   -              -
    POST            cp                                   X                   X              -
     */

    @GetMapping("ls")
    public Flux<TreeAndFileView> ls(TreeDto source, @AuthenticationPrincipal CloudUser cloudUser, ServerHttpResponse response) {
        return documentService.ls( source, cloudUser )
                .doOnError( e -> response.setStatusCode(
                        StatusCodeMapper.mapThrowable( e,
                                Map.of( RecordNotFoundException.class, NOT_FOUND,
                                        IllegalObjectTypeException.class, BAD_REQUEST ) ) ) );
    }

    @GetMapping("versionsFirst")
    public Mono<SeekInitResponse> versionsFirst(TreeDto source, @RequestParam(defaultValue = "25") Integer limit, @AuthenticationPrincipal CloudUser cloudUser) {
        return documentService.fetchFirstPageFileVersions( source, limit, cloudUser );
    }

    @GetMapping("versionNext")
    public Flux<FileViewDto> versionsNext(FileViewDto last, int limit, @AuthenticationPrincipal CloudUser cloudUser) {
        return documentService.fetchNextPage( last, limit, cloudUser );
    }

    @GetMapping("version")
    public Flux<ByteBuffer> getVersion(FileViewDto fileViewDto, @AuthenticationPrincipal CloudUser cloudUser, ServerHttpResponse response) {
        return documentService.getFileData( fileViewDto, cloudUser )
                .doOnError( e -> response.setStatusCode(
                        StatusCodeMapper.mapThrowable( e, NoSuchKeyException.class, NOT_FOUND ) ) );
    }

    @DeleteMapping("tree")
    public Mono<Void> deleteTreeObject(TreeDto target, @RequestHeader boolean recursive, @AuthenticationPrincipal CloudUser cloudUser) {
        return documentService.rmTreeObject( target, recursive, cloudUser );
    }

    @DeleteMapping("version")
    public Mono<Void> rmVersion(FileViewDto target, @AuthenticationPrincipal CloudUser cloudUser, ServerHttpResponse response) {
        return documentService.rmVersion( target, cloudUser )
                .doOnError( e -> response.setStatusCode( StatusCodeMapper.mapThrowable( e, DeleteFailureException.class, NOT_FOUND ) ) );
    }

    @PostMapping("root")
    public Mono<TreeDto> createRoot(@AuthenticationPrincipal CloudUser cloudUser, ServerHttpResponse response) {
        return documentService.createRoot( cloudUser )
                .doOnError( e -> response.setStatusCode( StatusCodeMapper.mapThrowable( e, InsertFailureException.class, BAD_REQUEST ) ) );
    }

    @PostMapping("dir")
    public Mono<TreeDto> createDir(TreeDto treeDto, @AuthenticationPrincipal CloudUser cloudUser, ServerHttpResponse response) {
        return documentService.createDir( treeDto, cloudUser )
                .doOnError( e -> response.setStatusCode( StatusCodeMapper.mapThrowable( e, InsertFailureException.class, BAD_REQUEST ) ) );
    }

    @PostMapping("file")
    public Mono<TreeAndFileView> createFile(Flux<ByteBuffer> data, @RequestHeader Ltree path,
                                            @RequestHeader String checksum, @RequestHeader Long size,
                                            @AuthenticationPrincipal CloudUser cloudUser, ServerHttpResponse response) {
        TreeDto treeDto = TreeDto.builder()
                .path( path ).objectType( FILE ).build();
        FileDto fileDto = FileDto.builder()
                .checksum( checksum ).size( size ).build();
        return documentService.createFile( treeDto, fileDto, data, cloudUser )
                .doOnError( e -> response.setStatusCode( StatusCodeMapper.mapThrowable( e, IllegalObjectTypeException.class, BAD_REQUEST,
                        InsertFailureException.class, BAD_REQUEST ) ) );
    }

    @PostMapping("version")
    public Mono<TreeAndFileView> addFileVersion(Flux<ByteBuffer> data, @RequestHeader UUID objectId,
                                                @RequestHeader String checksum, @RequestHeader Long size,
                                                @AuthenticationPrincipal CloudUser cloudUser, ServerHttpResponse response) {
        TreeDto treeDto = TreeDto.builder()
                .objectId( objectId ).objectType( FILE ).build();
        FileDto fileDto = FileDto.builder()
                .checksum( checksum ).size( size ).build();
        return documentService.addFileVersion( treeDto, fileDto, data, cloudUser )
                .doOnError( e -> response.setStatusCode( StatusCodeMapper.mapThrowable( e,
                        IllegalObjectTypeException.class, BAD_REQUEST,
                        InsertFailureException.class, BAD_REQUEST ) ) );
    }

    @PatchMapping("mv")
    public Mono<Long> mv(TreeDto source, @RequestHeader Ltree destination,
                         @AuthenticationPrincipal CloudUser cloudUser, ServerHttpResponse response) {
        return documentService.mv( source, destination, cloudUser )
                .doOnError( e -> response.setStatusCode( StatusCodeMapper.mapThrowable( e,
                        NullPointerException.class, BAD_REQUEST,
                        UpdateFailureException.class, BAD_REQUEST,
                        IllegalObjectTypeException.class, BAD_REQUEST ) ) );
    }

    @PostMapping("cp")
    public Mono<Void> cp(TreeDto source, @RequestHeader Ltree destination,
                         @RequestHeader boolean onlyNewest, @AuthenticationPrincipal CloudUser cloudUser,
                         ServerHttpResponse response) {
        return documentService.cp( source, destination, onlyNewest, cloudUser )
                .doOnError( e -> response.setStatusCode( StatusCodeMapper.mapThrowable( e,
                        NullPointerException.class, BAD_REQUEST,
                        InsertFailureException.class, BAD_REQUEST ) ) );
    }
}
