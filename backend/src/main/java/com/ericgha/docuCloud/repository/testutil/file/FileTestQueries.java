package com.ericgha.docuCloud.repository.testutil.file;

import com.ericgha.docuCloud.converter.FileViewDtoToTreeJoinFileDto;
import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeJoinFileDto;
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
    private final FileViewDtoToTreeJoinFileDto fileViewToTreeJoinFile;

    public Flux<FileViewDto> fetchRecordsByUserId(CloudUser cloudUser, Comparator<FileViewDto> comparator) {
        return fetchRecordsByUserId( cloudUser.getUserId(), comparator );
    }

    public Flux<FileViewDto> fetchRecordsByUserId(UUID userId, Comparator<FileViewDto> comparator) {
        return Flux.from( dsl.selectFrom( FILE_VIEW )
                        .where( FILE_VIEW.USER_ID.eq( userId ) ) )
                .map(FileViewDto::fromRecord)
                .sort( comparator );
    }

    public Mono<FileViewDto> fetchFileViewDto(UUID objectId, UUID fileId) {
        return Mono.from( dsl.selectFrom( FILE_VIEW ).where(
                FILE_VIEW.OBJECT_ID.eq( objectId )
                        .and( FILE_VIEW.FILE_ID.eq( fileId ) ) ) )
                .map(FileViewDto::fromRecord);
    }

    public Flux<FileViewDto> fetchRecordsByFileId(UUID fileId, UUID userId) {
        return Flux.from( dsl.selectFrom( FILE_VIEW )
                .where( FILE_VIEW.FILE_ID.eq( fileId )
                        .and( FILE_VIEW.USER_ID.eq( userId ) ) ) )
                .map(FileViewDto::fromRecord);
    }

    public Flux<FileViewDto> fetchRecordsByChecksum(String checksum, UUID userId) {
        return Flux.from( dsl.selectFrom( FILE_VIEW )
                .where( FILE_VIEW.CHECKSUM.eq( checksum )
                        .and( FILE_VIEW.USER_ID.eq( userId ) ) ) )
                .map(FileViewDto::fromRecord);
    }

    public Flux<FileViewDto> fetchRecordsByObjectId(UUID objectId) {
        return Flux.from( dsl.selectFrom(FILE_VIEW)
                .where(FILE_VIEW.OBJECT_ID.eq(objectId) ) )
                .map(FileViewDto::fromRecord);
    }

    // from existing file view record fetches current
    public Mono<FileViewDto> fetchFileViewDto(FileViewDto fileViewRecord) {
        return fetchFileViewDto( fileViewRecord.getObjectId(),
                fileViewRecord.getFileId() );
    }

    // can only create a link not a file
    // Allow links b/t different users data if the table allows...
    public Flux<TreeJoinFileDto> createLinks(Collection<FileViewDto> newLinks, Comparator<TreeJoinFileDto> comparator) {
        List<Mono<TreeJoinFileDto>> queries = newLinks.stream()
                .map( this::createLink )
                .toList();
        return Flux.concat( queries )
                .sort( comparator );
    }

    public Mono<TreeJoinFileDto> createLink(FileViewDto rec) {
        return Mono.from( dsl.insertInto( FILE_VIEW ).set( FILE_VIEW.OBJECT_ID, rec.getObjectId() )
                        .set( FILE_VIEW.FILE_ID, rec.getFileId() )
                        // required due to table constraints
                        .set( FILE_VIEW.USER_ID, rec.getUserId() )
                        .set( FILE_VIEW.LINKED_AT, currentOffsetDateTime() )
                        .returning( asterisk() ) )
                // Converts result to treeJoinFileRecord because many fields are null in fileViewRecord
                .map(FileViewDto::fromRecord)
                .map( fileViewToTreeJoinFile::convert );
    }

    // must provide a full FileViewDto aside from timestamps (uploaded_at, linked_at
    // allows linking to another users object if the table allows...

    public Flux<FileViewDto> createFilesWithLinks(Collection<FileViewDto> fileViewRecords, Comparator<FileViewDto> comparator) {
        List<Mono<FileViewDto>> queries = fileViewRecords.stream()
                .map( this::createFileWithLinks )
                .toList();
        return Flux.fromIterable( queries )
                .flatMap( Mono::from )
                .sort( comparator );
    }

    public Mono<FileViewDto> createFileWithLinks(FileViewDto fileViewDto) {
        return Mono.from( dsl.insertInto( FILE_VIEW )
                .set( FILE_VIEW.OBJECT_ID, fileViewDto.getObjectId() )
                .set( FILE_VIEW.FILE_ID, fileViewDto.getFileId() )
                .set( FILE_VIEW.USER_ID, fileViewDto.getUserId() )
                .set( FILE_VIEW.UPLOADED_AT, currentOffsetDateTime() )
                .set( FILE_VIEW.LINKED_AT, currentOffsetDateTime() )
                .set( FILE_VIEW.CHECKSUM, fileViewDto.getChecksum() )
                .set( FILE_VIEW.SIZE, fileViewDto.getSize() )
                .returning( asterisk() ) )
                .map(FileViewDto::fromRecord);
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
