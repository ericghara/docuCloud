package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import com.ericgha.docuCloud.jooq.tables.records.TreeJoinFileRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.InsertResultStep;
import org.jooq.Record2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.tables.FileView.FILE_VIEW;
import static org.jooq.impl.DSL.*;

/**
 * File, tree_join_file (tables) and file_view (view) queries useful for testing are grouped here.
 * While this class on the surface has much of the functionality of FileService, the methods in
 * this class do not have any checks or safeguards to enforce table constraints.
 */
@RequiredArgsConstructor
public class FileTestQueries {

    private final DSLContext dsl;

    Flux<FileViewRecord> recordsByUser(CloudUser cloudUser, Comparator<FileViewRecord> comparator) {
        return Flux.from( dsl.selectFrom( FILE_VIEW )
                .where( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) )
                .sort(comparator);
    }

    Mono<FileViewRecord> fetchLink(UUID objectId, UUID fileId) {
        return Mono.from( dsl.selectFrom( FILE_VIEW ).where(
                FILE_VIEW.OBJECT_ID.eq( objectId )
                .and( FILE_VIEW.FILE_ID.eq( fileId ) ) ) );
    }

    Mono<FileViewRecord> fetchLink(FileViewRecord fileViewRecord) {
        return fetchLink(fileViewRecord.getObjectId(),
                fileViewRecord.getFileId() );
    }

    // can only create a link not a file
    // Allow links b/t different users data if the table allows...
    // Be careful with comparators here
    Flux<TreeJoinFileRecord> createLinks(Collection<FileViewRecord> newLinks, Comparator<TreeJoinFileRecord> comparator) {
        List<Mono<FileViewRecord>> queries = newLinks.stream().map( rec -> dsl.insertInto(FILE_VIEW).set(FILE_VIEW.OBJECT_ID, rec.getObjectId() )
                .set(FILE_VIEW.FILE_ID, rec.getFileId() )
                // required due to table constraints
                .set(FILE_VIEW.USER_ID, rec.getUserId() )
                .set( FILE_VIEW.LINKED_AT, currentOffsetDateTime() )
                .returning( asterisk() ) )
                .map(Mono::from)
                .toList();
        return Flux.concat(queries).map( fvr -> new TreeJoinFileRecord()
                .setObjectId( fvr.getObjectId() )
                .setFileId( fvr.getFileId() )
                .setLinkedAt( fvr.getLinkedAt() ) )
                .sort(comparator);
    }

    Flux<Record2<UUID, Long>> fetchFileLinkingDegree(CloudUser cloudUser) {
        return Flux.from(
                dsl.select( FILE_VIEW.FILE_ID, count(FILE_VIEW.FILE_ID).cast(Long.class).as("count") )
                        .from(FILE_VIEW)
                        .where(FILE_VIEW.USER_ID.eq(cloudUser.getUserId()))
                        .groupBy(FILE_VIEW.FILE_ID)
        );
    }

    // must provide a full FileViewRecord aside from timestamps (uploaded_at, linked_at
    // allows linking to another users object if the table allows...
    Flux<FileViewRecord> createFilesWithLinks(Collection<FileViewRecord> fileViewRecords, Comparator<FileViewRecord> comparator) {
        List<InsertResultStep<FileViewRecord>> queries = fileViewRecords.stream().map( rec -> dsl.insertInto(FILE_VIEW)
                        .set(FILE_VIEW.OBJECT_ID, rec.getObjectId() )
                        .set(FILE_VIEW.FILE_ID, rec.getFileId() )
                        .set(FILE_VIEW.USER_ID, rec.getUserId() )
                        .set(FILE_VIEW.UPLOADED_AT, currentOffsetDateTime() )
                        .set( FILE_VIEW.LINKED_AT, currentOffsetDateTime() )
                        .set( FILE_VIEW.CHECKSUM, rec.getChecksum() )
                        .set( FILE_VIEW.SIZE, rec.getSize() )
                        .returning( asterisk() ) )
                .toList();
        return Flux.fromIterable(queries)
                .flatMap( Mono::from )
                .sort(comparator);
    }

    Flux<Record2<UUID, Long>> fetchObjectLinkingDegree(CloudUser cloudUser) {
        return Flux.from(
                dsl.select( FILE_VIEW.OBJECT_ID, count(FILE_VIEW.OBJECT_ID).cast(Long.class).as("count") )
                        .from(FILE_VIEW)
                        .where(FILE_VIEW.USER_ID.eq(cloudUser.getUserId()))
                        .groupBy(FILE_VIEW.OBJECT_ID)
        );
    }

}
