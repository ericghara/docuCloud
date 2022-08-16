package com.ericgha.docuCloud.repository;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.dto.TreeJoinFileDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeJoinFileRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.CommonTableExpression;
import org.jooq.DSLContext;
import org.jooq.DeleteResultStep;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectWindowStep;
import org.jooq.SelectWithTiesStep;
import org.jooq.Table;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Objects;
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
    public Mono<Long> createEdge(FileDto fileDto, TreeDto treeDto, CloudUser cloudUser) {
        SelectConditionStep<Record1<Boolean>> isFile = treeRepository.isObjectType( ObjectType.FILE, treeDto, cloudUser );

        return Mono.from( dsl.insertInto( TREE_JOIN_FILE )
                        .select( dsl.select( val( treeDto.getObjectId(), UUID.class ), val( fileDto.getFileId(), UUID.class ), currentOffsetDateTime() )
                                .where( val( true, Boolean.class ).eq( isFile ) ) ) )
                .map( (Number num) -> (long) num );
    }

    public Mono<FileDto> rmEdge(TreeJoinFileDto link, CloudUser cloudUser) {
        CommonTableExpression<TreeJoinFileRecord> rmLink = name("rm_link").as(deleteEdgeStep( link, cloudUser ) );
        var selectCountAfterRm = name("select_count").as(selectFileNodeDegreeFrom( rmLink ) );
        var rmFiles = name("rm_files").as(dsl.deleteFrom( FILE ).using( selectCountAfterRm )
                .where( selectCountAfterRm.field( "count", Long.class )
                        .eq( 0L ).and( selectCountAfterRm.field( FILE_VIEW.FILE_ID )
                                .eq( FILE.FILE_ID ) ) )
                .returning( asterisk() ) );
        Field<Long> countField =  field( "count", Long.class );
        Field<UUID> fieldField = field(FILE.FILE_ID);
        return Mono.from(dsl.deleteFrom(FILE).using(selectCountAfterRm).where( selectCountAfterRm.field( "count", Long.class )
                        .eq( 0L ).and( selectCountAfterRm.field( FILE_VIEW.FILE_ID )
                                .eq( FILE.FILE_ID ) ) )
                .returning( asterisk() ) )
                .map( FileDto::fromRecord );
//        return Mono.from( dsl.with(selectCountAfterRm )
//                        .select( rmFiles.asterisk() )
//                        .from( rmFiles )
//                        .coerce( FILE ) )
//                .map( FileDto::fromRecord );
    }

    DeleteResultStep<TreeJoinFileRecord> deleteEdgeStep(TreeJoinFileDto edge, CloudUser cloudUser) {
        SelectWithTiesStep<Record1<UUID>> fileIdIfUserOwns = this.selectFileId( edge.getFileId(), cloudUser ) ;
        return dsl.deleteFrom( TREE_JOIN_FILE )
                .where( TREE_JOIN_FILE.OBJECT_ID.eq( edge.getObjectId() )
                        .and( TREE_JOIN_FILE.FILE_ID.eq( fileIdIfUserOwns ) ) )
                .returning( asterisk() );
    }

    // Accepts any result query with a FILE_ID record
    SelectWindowStep<Record2<UUID, Long>> selectFileNodeDegreeFrom(CommonTableExpression<?> query) {
        Field<UUID> fileIdField = query.fieldsRow()
                .field( FILE_VIEW.FILE_ID.getName(), FILE_VIEW.FILE_ID.getDataType() );
        if (Objects.isNull( fileIdField ) )  {
            throw new IllegalArgumentException( "file_id field not found in query" );
        }
        // designed to accept one or many records
        var allNodeDegrees = selectAllFileNodeDegrees();
        return dsl.with(query)
                .selectDistinct( query.field( fileIdField ), allNodeDegrees.field("count", Long.class) )
                .from( ( query ) )
                .leftJoin( allNodeDegrees ).on(query.field( fileIdField ).eq(allNodeDegrees.field(fileIdField) ) );
    }

    // always combine with something that will filter results, or this will return a result from all records
    private SelectWindowStep<Record2<UUID, Long>> selectAllFileNodeDegrees() {
        return dsl.select(FILE_VIEW.FILE_ID, count(FILE_VIEW.FILE_ID).cast(Long.class).as("count") )
                .from(FILE_VIEW)
                .groupBy( FILE_VIEW.FILE_ID );
    }

    SelectJoinStep<Record1<Boolean>> isUsersFile(FileDto fileDto, CloudUser cloudUser) {
        Table<Record2<UUID, UUID>> fileLinks = selectAdjacenciesOfFile( fileDto, cloudUser ).limit( 1 ).asTable();

        return dsl.select( when( count().eq( 0 ), false )
                        .otherwise( true ) )
                .from( fileLinks );
    }

    // A user permission safe way to select a file
    SelectWithTiesStep<Record1<UUID>> selectFileId(UUID fileId, CloudUser cloudUser) {
        return dsl.select( FILE_VIEW.FILE_ID )
                .from( FILE_VIEW )
                .where( FILE_VIEW.FILE_ID.eq( fileId )
                        .and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) )
                .limit( 1 );
    }

    // if user does not own file will always return null, this will therefore trigger a delete request
    // but the delete will never be successful b/c the file is in a prefix that the user does not own
    SelectConditionStep<Record2<UUID, UUID>> selectAdjacenciesOfFile(FileDto fileDto, CloudUser cloudUser) {
        return dsl.select( FILE_VIEW.OBJECT_ID, FILE_VIEW.FILE_ID )
                .from( FILE_VIEW )
                .where( FILE_VIEW.FILE_ID.eq( fileDto.getFileId() )
                        .and( FILE_VIEW.USER_ID.eq( cloudUser.getUserId() ) ) );
    }

}
