package com.ericgha.docuCloud.repository;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.dto.TreeJoinFileDto;
import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.ResultQuery;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static com.ericgha.docuCloud.jooq.Routines.fileViewDel;
import static com.ericgha.docuCloud.jooq.Tables.FILE_VIEW;
import static com.ericgha.docuCloud.jooq.Tables.TREE_JOIN_FILE;
import static org.jooq.impl.DSL.*;

@Repository
@RequiredArgsConstructor
public class FileRepository {

    // TODO ensure no ref to non file objects
    // TODO ensure each file has object degree >= 1
    // TODO tests of table constraints

    public Mono<Long> createEdge(FileDto fileDto, TreeDto treeDto, CloudUser cloudUser, DSLContext dsl) {
        // linking of other user's fileObject or non file objects is prevented by table constraints
        return Mono.from( dsl.insertInto( FILE_VIEW )
                        .set( FILE_VIEW.OBJECT_ID, treeDto.getObjectId() )
                        .set( FILE_VIEW.FILE_ID, fileDto.getFileId() )
                        .set( FILE_VIEW.USER_ID, cloudUser.getUserId() )
                        .set( FILE_VIEW.LINKED_AT, currentOffsetDateTime() ) )
                .map( (Number num) -> (long) num );
    }

    // fileId, and uploadedAt fields are generated and will be ignored, only fields used are checksum and size
    public Mono<FileViewDto> createFileFor(TreeDto treeObject, FileDto file, CloudUser cloudUser, DSLContext dsl) {
        // linking of other user's fileObject or non file objects is prevented by table constraints
        return Mono.from( dsl.insertInto( FILE_VIEW )
                        .set( FILE_VIEW.OBJECT_ID, treeObject.getObjectId() )
                        .set( FILE_VIEW.FILE_ID, UUID.randomUUID() )
                        .set( FILE_VIEW.USER_ID, cloudUser.getUserId() )
                        .set( FILE_VIEW.UPLOADED_AT, currentOffsetDateTime() )
                        .set( FILE_VIEW.LINKED_AT, currentOffsetDateTime() )
                        .set( FILE_VIEW.CHECKSUM, file.getChecksum() )
                        .set( FILE_VIEW.SIZE, file.getSize() )
                        .returning( asterisk() ) )
                .mapNotNull( FileViewDto::fromRecord );
    }

    // This is intended for when a file version is being deleted
    // returns file edge pointed to and if the file was deleted (creating an orphan fileResource)
    // throws if record not found or not user's
    public Mono<Record2<UUID, Boolean>> rmEdge(TreeJoinFileDto link, CloudUser cloudUser, DSLContext dsl) {
        return Mono.from( dsl.select( FILE_VIEW.FILE_ID,
                        fileViewDel( FILE_VIEW.OBJECT_ID, FILE_VIEW.FILE_ID, FILE_VIEW.USER_ID ).as( "orphan" ) )
                .from( FILE_VIEW )
                .where( FILE_VIEW.OBJECT_ID.eq( link.getObjectId() )
                        .and( FILE_VIEW.FILE_ID.eq( link.getFileId() ) )
                        .and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) ) );
    }

    // This is intended for when an object is deleted from treeRepository
    // returns file edge pointed to and if the file was deleted (creating an orphan fileResource)
    public Flux<Record2<UUID, Boolean>> rmEdgesFrom(UUID objectId, CloudUser cloudUser, DSLContext dsl) {
        return Flux.from( dsl.select( FILE_VIEW.FILE_ID,
                        fileViewDel( FILE_VIEW.OBJECT_ID, FILE_VIEW.FILE_ID, FILE_VIEW.USER_ID ).as( "orphan" ) )
                .from( FILE_VIEW )
                .where( FILE_VIEW.OBJECT_ID.eq( objectId ) ) );
    }

    public Mono<Long> cpNewestFile(UUID sourceObjectId, UUID destinationObjectId, CloudUser cloudUser, DSLContext dsl) {
        ResultQuery<FileViewRecord> newestFile = this.selectNewestFilesLinkedTo( sourceObjectId, cloudUser, 1, dsl );
        return this.cpCommon( destinationObjectId, newestFile, dsl );
    }

    public Mono<Long> cpAllFiles(UUID sourceObjectId, UUID destinationObjectId, CloudUser cloudUser, DSLContext dsl) {
        ResultQuery<FileViewRecord> allFIles = this.selectAllFilesLinkedTo( sourceObjectId, cloudUser, dsl );
        return this.cpCommon( destinationObjectId, allFIles, dsl );
    }

    public Mono<Long> cpCommon(UUID destinationObjectId, ResultQuery<FileViewRecord> edgesToCopy, DSLContext dsl) {
        var toCopy = name( "source" ).as( edgesToCopy );
        return Mono.from( dsl.with( toCopy )
                        .insertInto( TREE_JOIN_FILE )
                        .select( dsl.select( val( destinationObjectId, UUID.class ), toCopy.field( FILE_VIEW.FILE_ID ), currentOffsetDateTime() ).from( toCopy ) ) )
                // this actually returns a long at runtime, but at compile time expects an Integer
                .map( (Number c) -> (long) c );
    }

    public Mono<Long> countFilesFor(TreeDto treeDto, CloudUser cloudUser, DSLContext dsl) {
        return Mono.from( dsl.select( count( asterisk() ).cast(Long.class) )
                        .from( FILE_VIEW )
                        .where( FILE_VIEW.OBJECT_ID.eq( treeDto.getObjectId() )
                                .and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) ) )
                .map( Record1::value1 );
    }

    public Mono<FileViewDto> lsNewestFileFor(TreeDto treeDto, CloudUser cloudUser, DSLContext dsl) {
        return lsNewestFilesFor( treeDto, 1, cloudUser, dsl )
                .next();
    }

    public Flux<FileViewDto> lsNewestFilesFor(TreeDto treeDto, int limit, CloudUser cloudUser, DSLContext dsl) {
        return Flux.from( selectNewestFilesLinkedTo( treeDto.getObjectId(), cloudUser, limit, dsl ) )
                .mapNotNull( FileViewDto::fromRecord );
    }

    public Flux<FileViewDto> lsNextFilesFor(FileViewDto lastRecord, int limit, CloudUser cloudUser, DSLContext dsl) {
        return Flux.from( dsl.select( asterisk() )
                        .from( FILE_VIEW )
                        .where( FILE_VIEW.OBJECT_ID.eq( lastRecord.getObjectId() ).and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) )
                        .orderBy( FILE_VIEW.LINKED_AT.desc(), FILE_VIEW.UPLOADED_AT.desc(), FILE_VIEW.FILE_ID.desc() )
                        .seek( lastRecord.getLinkedAt(), lastRecord.getUploadedAt(), lastRecord.getFileId() )
                        .limit( limit )
                        .coerce( FILE_VIEW ) )
                .mapNotNull( FileViewDto::fromRecord );
    }

    ResultQuery<FileViewRecord> selectNewestFilesLinkedTo(UUID objectId, CloudUser cloudUser, int limit, DSLContext dsl) {
        return dsl.select( asterisk() )
                .from( FILE_VIEW )
                .where( FILE_VIEW.OBJECT_ID.eq( objectId ).and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) )
                .orderBy( FILE_VIEW.LINKED_AT.desc(), FILE_VIEW.UPLOADED_AT.desc(), FILE_VIEW.FILE_ID.desc() )
                .limit( limit )
                .coerce( FILE_VIEW );
    }

    ResultQuery<FileViewRecord> selectAllFilesLinkedTo(UUID objectId, CloudUser cloudUser, DSLContext dsl) {
        return dsl.select( asterisk() )
                .from( FILE_VIEW )
                .where( FILE_VIEW.OBJECT_ID.eq( objectId ).and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) )
                .coerce( FILE_VIEW );
    }


}
