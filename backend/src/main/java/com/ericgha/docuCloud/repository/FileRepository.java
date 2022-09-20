package com.ericgha.docuCloud.repository;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.dto.TreeJoinFileDto;
import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import com.ericgha.docuCloud.service.JooqTransaction;
import lombok.RequiredArgsConstructor;
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

    private final JooqTransaction jooqTx;

    // TODO ensure no ref to non file objects
    // TODO ensure each file has object degree >= 1
    // TODO tests of table constraints

    public <T extends FileDto> Mono<Long> createEdge(T fileDto, TreeDto treeDto, CloudUser cloudUser) {
        // linking of other user's fileObject or non file objects is prevented by table constraints
        return jooqTx.withConnection( dsl -> dsl.insertInto( FILE_VIEW )
                        .set( FILE_VIEW.OBJECT_ID, treeDto.getObjectId() )
                        .set( FILE_VIEW.FILE_ID, fileDto.getFileId() )
                        .set( FILE_VIEW.USER_ID, cloudUser.getUserId() )
                        .set( FILE_VIEW.LINKED_AT, currentOffsetDateTime() ) )
                .map( (Number num) -> (long) num );
    }

    // fileId, and uploadedAt fields are generated and will be ignored, only fields used are checksum and size
    public <T extends FileDto> Mono<FileViewDto> createFileFor(TreeDto treeObject, T file, CloudUser cloudUser) {
        // linking of other user's fileObject or non file objects is prevented by table constraints
        return jooqTx.withConnection( dsl -> dsl.insertInto( FILE_VIEW )
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
    public Mono<Record2<UUID, Boolean>> rmEdge(TreeJoinFileDto link, CloudUser cloudUser) {
        return jooqTx.withConnection( dsl -> dsl.select( FILE_VIEW.FILE_ID,
                        fileViewDel( FILE_VIEW.OBJECT_ID, FILE_VIEW.FILE_ID, FILE_VIEW.USER_ID ).as( "orphan" ) )
                .from( FILE_VIEW )
                .where( FILE_VIEW.OBJECT_ID.eq( link.getObjectId() )
                        .and( FILE_VIEW.FILE_ID.eq( link.getFileId() ) )
                        .and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) ) );
    }

    // This is intended for when an object is deleted from treeRepository
    // returns file edge pointed to and if the file was deleted (creating an orphan fileResource)
    public Flux<Record2<UUID, Boolean>> rmEdgesFrom(UUID objectId, CloudUser cloudUser) {
        return jooqTx.withConnectionMany( dsl -> dsl.select( FILE_VIEW.FILE_ID,
                        // fileViewDel is custom a table function to perform deletes, returns true if version
                        // became an orphan (no other TreeObjects link to it) and was deleted
                        fileViewDel( FILE_VIEW.OBJECT_ID, FILE_VIEW.FILE_ID, FILE_VIEW.USER_ID ).as( "orphan" ) )
                .from( FILE_VIEW )
                .where( FILE_VIEW.OBJECT_ID.eq( objectId ).and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) ) );
    }

    public Mono<Long> cpNewestFile(UUID sourceObjectId, UUID destinationObjectId, CloudUser cloudUser) {
        Mono<ResultQuery<FileViewRecord>> newestFile = this.selectNewestFilesLinkedTo( sourceObjectId, cloudUser, 1 );
        return newestFile.flatMap( query -> this.cpCommon( destinationObjectId, query ) );
    }

    public Mono<Long> cpAllFiles(UUID sourceObjectId, UUID destinationObjectId, CloudUser cloudUser) {
        Mono<ResultQuery<FileViewRecord>> allFiles = this.selectAllFilesLinkedTo( sourceObjectId, cloudUser );
        return allFiles.flatMap( query -> this.cpCommon( destinationObjectId, query ) );
    }

    Mono<Long> cpCommon(UUID destinationObjectId, ResultQuery<FileViewRecord> edgesToCopy) {
        var toCopy = name( "source" ).as( edgesToCopy );
        return jooqTx.withConnection( dsl -> dsl.with( toCopy )
                        .insertInto( TREE_JOIN_FILE )
                        .select( dsl.select( val( destinationObjectId, UUID.class ), toCopy.field( FILE_VIEW.FILE_ID ), currentOffsetDateTime() ).from( toCopy ) ) )
                // this actually returns a long at runtime, but at compile time expects an Integer
                .map( (Number c) -> (long) c );
    }

    public Mono<Long> countFilesFor(TreeDto treeDto, CloudUser cloudUser) {
        return jooqTx.withConnection( dsl -> dsl.select( count( asterisk() ).cast( Long.class ) )
                        .from( FILE_VIEW )
                        .where( FILE_VIEW.OBJECT_ID.eq( treeDto.getObjectId() )
                                .and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) ) )
                .map( Record1::value1 );
    }

    public Mono<FileViewDto> lsNewestFileFor(TreeDto treeDto, CloudUser cloudUser) {
        return lsNewestFilesFor( treeDto, 1, cloudUser )
                .next();
    }

    public Flux<FileViewDto> lsNewestFilesFor(TreeDto treeDto, int limit, CloudUser cloudUser) {
        return selectNewestFilesLinkedTo( treeDto.getObjectId(), cloudUser, limit )
                .flatMapMany( Flux::from )
                .mapNotNull( FileViewDto::fromRecord );
    }

    public Flux<FileViewDto> lsNextFilesFor(FileViewDto lastRecord, int limit, CloudUser cloudUser) {
        return jooqTx.withConnectionMany( dsl -> dsl.select( asterisk() )
                        .from( FILE_VIEW )
                        .where( FILE_VIEW.OBJECT_ID.eq( lastRecord.getObjectId() ).and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) )
                        .orderBy( FILE_VIEW.LINKED_AT.desc(), FILE_VIEW.UPLOADED_AT.desc(), FILE_VIEW.FILE_ID.desc() )
                        .seek( lastRecord.getLinkedAt(), lastRecord.getUploadedAt(), lastRecord.getFileId() )
                        .limit( limit )
                        .coerce( FILE_VIEW ) )
                .mapNotNull( FileViewDto::fromRecord );
    }

    Mono<ResultQuery<FileViewRecord>> selectNewestFilesLinkedTo(UUID objectId, CloudUser cloudUser, int limit) {
        return jooqTx.get().map( dsl ->
                dsl.select( asterisk() )
                        .from( FILE_VIEW )
                        .where( FILE_VIEW.OBJECT_ID.eq( objectId ).and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) )
                        .orderBy( FILE_VIEW.LINKED_AT.desc(), FILE_VIEW.UPLOADED_AT.desc(), FILE_VIEW.FILE_ID.desc() )
                        .limit( limit )
                        .coerce( FILE_VIEW ) );
    }

    Mono<ResultQuery<FileViewRecord>> selectAllFilesLinkedTo(UUID objectId, CloudUser cloudUser) {
        return jooqTx.get().map( dsl ->
                dsl.select( asterisk() )
                        .from( FILE_VIEW )
                        .where( FILE_VIEW.OBJECT_ID.eq( objectId ).and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) )
                        .coerce( FILE_VIEW ) );
    }


}
