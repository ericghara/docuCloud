package com.ericgha.docuCloud.repository;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.FileRecord;
import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import com.ericgha.docuCloud.jooq.tables.records.TreeJoinFileRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.DeleteResultStep;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectWindowStep;
import org.jooq.Table;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static com.ericgha.docuCloud.jooq.Tables.FILE_VIEW;
import static com.ericgha.docuCloud.jooq.Tables.TREE_JOIN_FILE;
import static com.ericgha.docuCloud.jooq.tables.File.FILE;
import static org.jooq.impl.DSL.*;

@Service
@RequiredArgsConstructor
public class FileRepository {

    private final DSLContext dsl;
    private final TreeRepository treeRepository;

    // TODO ensure no ref to non file objects
    // TODO ensure each file has object degree >= 1

    // TODO

    // TODO isObjectType test
    public Mono<Long> linkExistingFile(FileRecord fileRecord, TreeDto treeDto, CloudUser cloudUser) {
        SelectConditionStep<Record1<Boolean>> isFile = treeRepository.isObjectType( ObjectType.FILE, treeDto, cloudUser );

        return Mono.from( dsl.insertInto( TREE_JOIN_FILE )
                        .select( dsl.select( val( treeDto.getObjectId(), UUID.class ), val( fileRecord.getFileId(), UUID.class ), currentOffsetDateTime() )
                                .where( val( true, Boolean.class ).eq( isFile ) ) ) )
                .map( (Number num) -> (long) num );
    }

    public Mono<FileRecord> unLinkExistingFile(TreeJoinFileRecord link, CloudUser cloudUser) {
        var rmLink = unLink( link, cloudUser );
        var selectCountAfterRm = selectFileLinkDegree( rmLink );
        return Mono.from( dsl.deleteFrom( FILE ).using( selectCountAfterRm )
                .where( selectCountAfterRm.field( "count", Long.class )
                        .eq( 0L ).and( selectCountAfterRm.field( FILE_VIEW.FILE_ID )
                                .eq( FILE.FILE_ID ) ) )
                .returning( asterisk() ) );
    }

    DeleteResultStep<FileViewRecord> unLink(TreeJoinFileRecord link, CloudUser cloudUser) {
        return deleteFrom( FILE_VIEW )
                .where( FILE_VIEW.OBJECT_ID.eq( link.getObjectId() )
                        .and( FILE_VIEW.FILE_ID.eq( link.getFileId() )
                                .and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) ) )
                .returning( asterisk() );
    }

    SelectWindowStep<Record2<UUID, Long>> selectFileLinkDegree(DeleteResultStep<FileViewRecord> delStep) {
        // designed to accept one or many records
        var records = name( "fvrs" ).as( delStep );
        return dsl.with( records )
                .selectDistinct( FILE_VIEW.FILE_ID, count( FILE_VIEW.FILE_ID ).cast( Long.class ).as( "count" ) )
                .from( records, FILE_VIEW )
                .where( FILE_VIEW.FILE_ID.in( records.field( FILE_VIEW.FILE_ID ) ) )
                .groupBy( FILE_VIEW.FILE_ID );
    }

    SelectJoinStep<Record1<Boolean>> isUsersFile(FileRecord fileRecord, CloudUser cloudUser) {
        Table<Record2<UUID, UUID>> fileLinks = selectLinksToFile( fileRecord, cloudUser ).limit( 1 ).asTable();

        return dsl.select( when( count().eq( 0 ), false )
                        .otherwise( true ) )
                .from( fileLinks );
    }

    // if user does not own file will always return null, this will therefore trigger a delete request
    // but the delete will never be successful b/c the file is in a prefix that the user does not own
    SelectConditionStep<Record2<UUID, UUID>> selectLinksToFile(FileRecord fileRecord, CloudUser cloudUser) {
        return dsl.select( FILE_VIEW.OBJECT_ID, FILE_VIEW.FILE_ID )
                .from( FILE_VIEW )
                .where( FILE_VIEW.FILE_ID.eq( fileRecord.getFileId() )
                        .and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) );
    }

}
