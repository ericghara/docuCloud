package com.ericgha.docuCloud.repository.testutil.file;

import com.ericgha.docuCloud.converter.FileViewRecordToTreeJoinFileRecord;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import com.ericgha.docuCloud.jooq.tables.records.TreeJoinFileRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.tables.FileView.FILE_VIEW;
import static org.jooq.impl.DSL.*;

/**
 * File, tree_join_file (tables) and file_view (view) queries useful for testing are grouped here. While this class on
 * the surface has much of the functionality of FileService, the methods in this class do not have any checks or
 * safeguards to enforce table constraints.
 */
@RequiredArgsConstructor
@Profile("test")
@Component
public class FileTestQueries {

    private final DSLContext dsl;
    private final FileViewRecordToTreeJoinFileRecord fvrToTjfrConverter = new FileViewRecordToTreeJoinFileRecord();

    public Flux<FileViewRecord> fetchRecordsByUserId(CloudUser cloudUser, Comparator<FileViewRecord> comparator) {
        return fetchRecordsByUserId( cloudUser.getUserId(), comparator );
    }

    public Flux<FileViewRecord> fetchRecordsByUserId(UUID userId, Comparator<FileViewRecord> comparator) {
        return Flux.from( dsl.selectFrom( FILE_VIEW )
                        .where( FILE_VIEW.USER_ID.eq( userId ) ) )
                .sort( comparator );
    }

    public Mono<FileViewRecord> fetchFileViewRecord(UUID objectId, UUID fileId) {
        return Mono.from( dsl.selectFrom( FILE_VIEW ).where(
                FILE_VIEW.OBJECT_ID.eq( objectId )
                        .and( FILE_VIEW.FILE_ID.eq( fileId ) ) ) );
    }

    public Flux<FileViewRecord> fetchRecordsByFileId(UUID fileId, UUID userId) {
        return Flux.from( dsl.selectFrom( FILE_VIEW )
                .where( FILE_VIEW.FILE_ID.eq( fileId )
                        .and( FILE_VIEW.USER_ID.eq( userId ) ) ) );
    }

    public Flux<FileViewRecord> fetchRecordsByChecksum(String checksum, UUID userId) {
        return Flux.from( dsl.selectFrom( FILE_VIEW )
                .where( FILE_VIEW.CHECKSUM.eq( checksum )
                        .and( FILE_VIEW.USER_ID.eq( userId ) ) ) );
    }

    public Flux<FileViewRecord> fetchRecordsByObjectId(UUID objectId) {
        return Flux.from( dsl.selectFrom(FILE_VIEW)
                .where(FILE_VIEW.OBJECT_ID.eq(objectId) ) );
    }

    // from existing file view record fetches current
    public Mono<FileViewRecord> fetchFileViewRecord(FileViewRecord fileViewRecord) {
        return fetchFileViewRecord( fileViewRecord.getObjectId(),
                fileViewRecord.getFileId() );
    }

    // can only create a link not a file
    // Allow links b/t different users data if the table allows...
    public Flux<TreeJoinFileRecord> createLinks(Collection<FileViewRecord> newLinks, Comparator<TreeJoinFileRecord> comparator) {
        List<Mono<TreeJoinFileRecord>> queries = newLinks.stream()
                .map( this::createLink )
                .toList();
        return Flux.concat( queries ).map( fvr -> new TreeJoinFileRecord()
                        .setObjectId( fvr.getObjectId() )
                        .setFileId( fvr.getFileId() )
                        .setLinkedAt( fvr.getLinkedAt() ) )
                .sort( comparator );
    }

    public Mono<TreeJoinFileRecord> createLink(FileViewRecord rec) {
        return Mono.from( dsl.insertInto( FILE_VIEW ).set( FILE_VIEW.OBJECT_ID, rec.getObjectId() )
                        .set( FILE_VIEW.FILE_ID, rec.getFileId() )
                        // required due to table constraints
                        .set( FILE_VIEW.USER_ID, rec.getUserId() )
                        .set( FILE_VIEW.LINKED_AT, currentOffsetDateTime() )
                        .returning( asterisk() ) )
                // Converts result to treeJoinFileRecord because many fields are null in fileViewRecord
                .map( fvrToTjfrConverter::convert );
    }

    // must provide a full FileViewRecord aside from timestamps (uploaded_at, linked_at
    // allows linking to another users object if the table allows...

    public Flux<FileViewRecord> createFilesWithLinks(Collection<FileViewRecord> fileViewRecords, Comparator<FileViewRecord> comparator) {
        List<Mono<FileViewRecord>> queries = fileViewRecords.stream()
                .map( this::createFileWithLinks )
                .toList();
        return Flux.fromIterable( queries )
                .flatMap( Mono::from )
                .sort( comparator );
    }

    public Mono<FileViewRecord> createFileWithLinks(FileViewRecord fileViewRecord) {
        return Mono.from( dsl.insertInto( FILE_VIEW )
                .set( FILE_VIEW.OBJECT_ID, fileViewRecord.getObjectId() )
                .set( FILE_VIEW.FILE_ID, fileViewRecord.getFileId() )
                .set( FILE_VIEW.USER_ID, fileViewRecord.getUserId() )
                .set( FILE_VIEW.UPLOADED_AT, currentOffsetDateTime() )
                .set( FILE_VIEW.LINKED_AT, currentOffsetDateTime() )
                .set( FILE_VIEW.CHECKSUM, fileViewRecord.getChecksum() )
                .set( FILE_VIEW.SIZE, fileViewRecord.getSize() )
                .returning( asterisk() ) );
    }

    public Flux<Record2<UUID, Long>> fetchFileLinkingDegreeByFileId(CloudUser cloudUser) {
        return Flux.from(
                dsl.select( FILE_VIEW.FILE_ID, count( FILE_VIEW.FILE_ID ).cast( Long.class ).as( "count" ) )
                        .from( FILE_VIEW )
                        .where( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) )
                        .groupBy( FILE_VIEW.FILE_ID )
                        .orderBy( FILE_VIEW.FILE_ID.asc() )
        );
    }

    public Flux<Record2<UUID, Long>> fetchObjectLinkingDegreeByObjectId(CloudUser cloudUser) {
        return Flux.from(
                dsl.select( FILE_VIEW.OBJECT_ID, count( FILE_VIEW.OBJECT_ID ).cast( Long.class ).as( "count" ) )
                        .from( FILE_VIEW )
                        .where( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) )
                        .groupBy( FILE_VIEW.OBJECT_ID )
                        .orderBy( FILE_VIEW.OBJECT_ID.asc() )
        );
    }

}
