package com.ericgha.docuCloud.repository;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.Tree;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.CommonTableExpression;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Record6;
import org.jooq.ResultQuery;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.postgres.extensions.types.Ltree;
import org.reactivestreams.Publisher;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.Routines.*;
import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static org.jooq.impl.DSL.*;

@Repository
@RequiredArgsConstructor
public class TreeRepository {

    // TODO lsDir(Ltree path, CloudUser clouduser)


    public Mono<TreeDto> create(TreeDto treeDto, CloudUser cloudUser, DSLContext dsl) {
        return Mono.from( dsl.insertInto( TREE )
                        .set( TREE.OBJECT_ID, UUID.randomUUID() )
                        .set( TREE.OBJECT_TYPE, treeDto.getObjectType() )
                        .set( TREE.PATH, treeDto.getPath() )
                        .set( TREE.USER_ID, cloudUser.getUserId() )
                        .set( TREE.CREATED_AT, defaultValue( OffsetDateTime.class ) )
                        .returning( asterisk() ) )
                .map( treeRecord -> treeRecord.into( TreeDto.class ) );
    }


    public Mono<Long> mvFile(Ltree newPath, TreeDto treeDto, CloudUser cloudUser, DSLContext dsl) {
        if (treeDto.getObjectType() != ObjectType.FILE) {
            return Mono.empty();
        }
        return Mono.from( dsl.update( TREE )
                        .set( TREE.PATH, newPath )
                        .where( TREE.OBJECT_ID.eq( treeDto.getObjectId() )
                                .and( TREE.USER_ID.eq( cloudUser.getUserId() ) )
                                .and( TREE.OBJECT_TYPE.eq( ObjectType.FILE ) ) )
                )
                // This is a workaround for a jOOQ bug, Method signature is Integer but actually returns a Long at runtime
                .map( (Number o) -> o.longValue() );
    }


    public Mono<Long> mvDir(Ltree newPath, TreeDto curRecord, CloudUser cloudUser, DSLContext dsl) {
        if (curRecord.getObjectType() != ObjectType.DIR) {
            return Mono.empty();
        }
        var movePathCte = name( "new" ).fields( "object_id", "path" )
                .as( createMovePath( newPath, curRecord, cloudUser, dsl ) );
        return Mono.from( dsl.with( movePathCte )
                        .update( TREE )
                        .set( TREE.PATH, movePathCte.field( "path", Ltree.class ) )
                        .from( movePathCte )
                        .where( TREE.OBJECT_ID.eq( movePathCte.field( "object_id", UUID.class ) ) ) )
                // This is a workaround for a jOOQ bug, Method signature is Integer but actually returns a Long at runtime
                .map( (Number o) -> o.longValue() );
    }


    public Flux<TreeDto> rmDirRecursive(TreeDto record, CloudUser cloudUser, DSLContext dsl) {
        if (record.getObjectType() != ObjectType.DIR) {
            return Flux.empty();
        }
        // Doesn't perform internal check of proper objectType as a creating a subtree from a spoofed file is not expensive
        SelectConditionStep<Record1<UUID>> delObjectIds = selectDescendents( record, cloudUser, dsl );
        return Flux.from( dsl.delete( TREE )
                        .where( TREE.OBJECT_ID.in( delObjectIds ) )
                        .returning( asterisk() ) )
                .map( TreeDto::fromRecord );
    }


    public Mono<TreeDto> rmNormal(TreeDto record, CloudUser cloudUser, DSLContext dsl) {
        // Doesn't perform internal check of proper objectType as a creating a subtree from a spoofed file is not expensive
        SelectJoinStep<Record1<Integer>> doDel = hasDescendents( record, cloudUser, dsl );
        return Mono.from(
                        dsl.delete( TREE )
                                .where( TREE.OBJECT_ID.eq( record.getObjectId() )
                                        .and( val( 1 ).eq( doDel ) ) )
                                .returning( asterisk() ) )
                .map( TreeDto::fromRecord );
    }

    // returning source_id, destination_id, object_type
    public Flux<Record3<UUID, UUID, ObjectType>> cpDir(Ltree destination, TreeDto sourceRecord, CloudUser cloudUser, DSLContext dsl) {
        if (sourceRecord.getObjectType() != ObjectType.DIR) {
            return Flux.empty();
        }
        var selectRecordCopies = fetchDirCopyRecords( destination, sourceRecord, cloudUser, dsl );
        return Flux.from( cpCommon( selectRecordCopies, dsl ) );
    }


    // Returning ObjectType is a compromise for code usability with cpDir, it will of course always be ObjectType.FILE
    public Mono<Record3<UUID, UUID, ObjectType>> cpFile(Ltree destination, TreeDto sourceRecord, CloudUser cloudUser, DSLContext dsl) {
        if (sourceRecord.getObjectType() != ObjectType.FILE) {
            return Mono.empty();
        }
        var selectRecordCopies = fetchFileCopyRecords( destination, sourceRecord, cloudUser, dsl );
        return Mono.from( cpCommon( selectRecordCopies, dsl ) );
    }

    // Query designed such that one or both of Object_id and path are used
    // If a ObjectType = FILE will always return empty
    public Flux<TreeDto> ls(TreeDto source, CloudUser cloudUser, DSLContext dsl) {
        CommonTableExpression<TreeRecord> parent = name( "parent" ).as(
                this.flexibleSelect( source, cloudUser, dsl ) );
        return Flux.from( dsl.with( parent )
                .select(asterisk() )
                .from(TREE, parent)
                .where( ltreeIsparent( parent.field( TREE.PATH ), TREE.PATH ) )
                .and( nlevel( parent.field( TREE.PATH ) ).plus( 1 ).eq( nlevel( TREE.PATH ) ) )
                .and( TREE.USER_ID.eq(cloudUser.getUserId() ) )
                .coerce( TREE ) )
                .map( TreeDto::fromRecord );
    }

    // Query designed such that one or both of Object_id and path are used, therefore one of these fields may be null
    ResultQuery<TreeRecord> flexibleSelect(TreeDto treeDto, CloudUser cloudUser, DSLContext dsl) {
        UUID objectId = treeDto.getObjectId();
        Ltree path = treeDto.getPath();
        if (Objects.isNull( objectId ) && Objects.isNull( path )) {
            throw new IllegalArgumentException( "One or both of objectId and path must be non-null.  Instead both were null" );
        }
        var conditions = TREE.USER_ID.eq( Objects.requireNonNull( cloudUser.getUserId(), "userId was null" ) )
                .and( TREE.OBJECT_TYPE.ne( ObjectType.FILE ) );
        if (Objects.nonNull( objectId )) {
            conditions = conditions.and( val( objectId ).eq( TREE.OBJECT_ID ) );
        }
        if (Objects.nonNull( path )) {
            conditions = conditions.and( val( path ).eq( TREE.PATH ) );
        }
        return dsl.select( asterisk() )
                .from( TREE ).where( conditions ).limit( 1 )
                .coerce( TREE );
    }

    // the purpose of this query is to check that the source is owned by the user
    SelectConditionStep<Record2<Ltree, Integer>> selectDirPathAndLevel(TreeDto curRecord, CloudUser cloudUser, DSLContext dsl) {
        return dsl.select( TREE.PATH.as( "path" ),
                        nlevel( TREE.PATH ).as( "level" ) )
                .from( TREE )
                .where( TREE.OBJECT_ID.eq( curRecord.getObjectId() ).and( TREE.USER_ID.eq( cloudUser.getUserId() ) )
                        .and( TREE.OBJECT_TYPE.eq( ObjectType.DIR ) ) );
    }

    ResultQuery<Record2<UUID, Ltree>> createMovePath(Ltree newPath, TreeDto curRecord, CloudUser cloudUser, DSLContext dsl) {
        CommonTableExpression<Record2<Ltree, Integer>> oldPathCte = name( "oldPath" ).fields( "path", "level" )
                .as( selectDirPathAndLevel( curRecord, cloudUser, dsl ) );
        return dsl.with( oldPathCte ).select( TREE.OBJECT_ID,
                        when( oldPathCte.field( "level", Integer.class ).eq( nlevel( TREE.PATH ) ), newPath )
                                .otherwise( ltreeAddltree( val( newPath ),
                                        subpath2( TREE.PATH, nlevel( oldPathCte.field( "path", Ltree.class ) ) ) ) )
                                .as( "path" ) )
                .from( TREE, oldPathCte )
                .where( TREE.USER_ID.eq( cloudUser.getUserId() ).and(
                        ltreeIsparent( oldPathCte.field( "path", Ltree.class ), TREE.PATH ) ) );
    }

    SelectConditionStep<Record1<UUID>> selectDescendents(TreeDto record, CloudUser cloudUser, DSLContext dsl) {
        return dsl.select( TREE.OBJECT_ID )
                .from( TREE )
                .where( TREE.USER_ID.eq( cloudUser.getUserId() )
                        .and( ltreeIsparent(
                                field( selectPath( record, cloudUser, dsl ) ), TREE.PATH ) ) );
    }

    /* Return values:
       0 - no matches including query record,
       1 - found object that has no descendents (except self),
       2 - object has 1 or more descendents (in addition to self)
     */
    SelectJoinStep<Record1<Integer>> hasDescendents(TreeDto record, CloudUser cloudUser, DSLContext dsl) {
        // went to CTE because in jOOQ seemed a little easier
        var selectDescCte = name( "descendents" )
                .fields( "count" )
                .as( select( TREE.OBJECT_ID )
                        .from( TREE )
                        .where( TREE.USER_ID.eq( cloudUser.getUserId() )
                                .and( ltreeIsparent(
                                        field( selectPath( record, cloudUser, dsl ) ), TREE.PATH ) ) )
                        .limit( 2 ) );
        return dsl.with( selectDescCte ).select( count() )
                .from( selectDescCte );
    }

    SelectConditionStep<Record1<Ltree>> selectPath(TreeDto record, CloudUser cloudUser, DSLContext dsl) {
        return dsl.select( TREE.PATH )
                .from( TREE )
                .where( TREE.OBJECT_ID.eq( record.getObjectId() )
                        .and( TREE.USER_ID.eq( cloudUser.getUserId() ) )
                        .and( TREE.OBJECT_TYPE.ne( ObjectType.ROOT ) ) );
    }

    Publisher<Record3<UUID, UUID, ObjectType>> cpCommon(
            SelectConditionStep<Record6<UUID, UUID, ObjectType, Ltree, UUID, OffsetDateTime>> selectRecordCopies, DSLContext dsl) {
        var copyCte = name( "copy_records" ).fields( "source_id", "object_id", "object_type",
                "path", "user_id", "created_at" ).as( selectRecordCopies );
        var insertCte = name( "insert_res" ).as(
                dsl.insertInto( TREE )
                        .select( dsl.select( copyCte.fields( "object_id", "object_type",
                                "path", "user_id", "created_at" ) ).from( copyCte ) )
                        .returning( TREE.OBJECT_ID ) );
        return dsl.with( copyCte ).with( insertCte ).select( copyCte.field( "source_id", UUID.class ),
                        copyCte.field( "object_id", UUID.class ).as( "destination_id" ),
                        copyCte.field( "object_type", ObjectType.class ) )
                .from( copyCte );
    }


    // Does not includes self (parent)
    // Creates new uuid and new timestamp, and converts path, other fields remain the same;
    SelectConditionStep<Record6<UUID, UUID, ObjectType, Ltree, UUID, OffsetDateTime>> fetchDirCopyRecords(
            Ltree destination, TreeDto sourceRecord, CloudUser cloudUser, DSLContext dsl) {
        CommonTableExpression<Record2<Ltree, Integer>> oldPathCte = name( "oldPath" ).fields( "path", "level" )
                .as( selectDirPathAndLevel( sourceRecord, cloudUser, dsl ) );
        return dsl.with( oldPathCte ).select( TREE.OBJECT_ID.as( "source_id" ),
                        uuidGenerateV4().as( TREE.OBJECT_ID ), TREE.OBJECT_TYPE,
                        when( oldPathCte.field( "level", Integer.class ).eq( nlevel( TREE.PATH ) ), destination )
                                .otherwise( ltreeAddltree( val( destination ),
                                        subpath2( TREE.PATH, nlevel( oldPathCte.field( "path", Ltree.class ) ) ) ) )
                                .as( TREE.PATH ),
                        TREE.USER_ID, currentOffsetDateTime().as( TREE.CREATED_AT ) )
                .from( TREE, oldPathCte )
                .where( TREE.USER_ID.eq( cloudUser.getUserId() ).and(
                        ltreeIsparent( oldPathCte.field( "path", Ltree.class ), TREE.PATH ) ) );
    }

    SelectConditionStep<Record6<UUID, UUID, ObjectType, Ltree, UUID, OffsetDateTime>> fetchFileCopyRecords(
            Ltree destination, TreeDto sourceRecord, CloudUser cloudUser, DSLContext dsl) {
        return dsl.select(
                        TREE.OBJECT_ID.as( "source_id" ),
                        uuidGenerateV4().as( TREE.OBJECT_ID ), TREE.OBJECT_TYPE,
                        val( destination ).as( TREE.PATH ), TREE.USER_ID, currentOffsetDateTime().as( TREE.CREATED_AT ) )
                .from( TREE )
                .where( TREE.OBJECT_ID.eq( sourceRecord.getObjectId() )
                        .and( TREE.USER_ID.eq( cloudUser.getUserId() ) )
                        .and( TREE.OBJECT_TYPE.eq( ObjectType.FILE ) ) );
    }

    SelectConditionStep<Record1<Boolean>> isObjectType(ObjectType objectType, TreeDto treeDto, CloudUser cloudUser, DSLContext dsl) {
        return dsl.select( when( count( Tree.TREE.OBJECT_ID ).eq( 0 ), false ).otherwise( true ) )
                .from( Tree.TREE )
                .where( Tree.TREE.OBJECT_ID.eq( treeDto.getObjectId() )
                        .and( Tree.TREE.OBJECT_TYPE.eq( objectType ) )
                        .and( Tree.TREE.USER_ID.eq( cloudUser.getUserId() ) ) );
    }
}
