package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.CommonTableExpression;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Record6;
import org.jooq.ResultQuery;
import org.jooq.SelectConditionStep;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.Flow;

import static com.ericgha.docuCloud.jooq.Routines.*;
import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static org.jooq.impl.DSL.*;

@Service
@RequiredArgsConstructor
public class TreeService {

    private final DSLContext dsl;

    // todo add an object mapper to the constructor and include it for all public functions
    // for testing the objectmapper will be NO-OP but for production will conver to Tree pojo

    @Transactional
    public Mono<TreeRecord> create(TreeRecord treeRecord, CloudUser cloudUser) {
        return Mono.from( dsl.insertInto( TREE )
                .set( TREE.OBJECT_ID, UUID.randomUUID() )
                .set( TREE.OBJECT_TYPE, treeRecord.getObjectType() )
                .set( TREE.PATH, treeRecord.getPath() )
                .set( TREE.USER_ID, cloudUser.getUserId() )
                .set( TREE.CREATED_AT, defaultValue( OffsetDateTime.class ) )
                .returning( asterisk() )
        );
    }

    @Transactional
    public Mono<Long> mvFile(Ltree newPath, TreeRecord treeRecord, CloudUser cloudUser) {
        if (treeRecord.getObjectType() != ObjectType.FILE) {
            return Mono.just( 0L );
        }
        return Mono.from( dsl.update( TREE )
                        .set( TREE.PATH, newPath )
                        .where( TREE.OBJECT_ID.eq( treeRecord.getObjectId() )
                                .and( TREE.USER_ID.eq( cloudUser.getUserId() ) )
                                .and( TREE.OBJECT_TYPE.eq( ObjectType.FILE ) ) ) )
                // This is a workaround for a jOOQ bug, Method signature is Integer but actually returns a Long at runtime
                .map( (Number o) -> o.longValue() );
    }

    // the purpose of this query is to check that the source is owned by the user
    SelectConditionStep<Record2<Ltree, Integer>> selectDirPathAndLevel(TreeRecord curRecord, CloudUser cloudUser) {
        return dsl.select( TREE.PATH.as( "path" ),
                        nlevel( TREE.PATH ).as( "level" ) )
                .from( TREE )
                .where( TREE.OBJECT_ID.eq( curRecord.getObjectId() ).and( TREE.USER_ID.eq( cloudUser.getUserId() ) )
                        .and( TREE.OBJECT_TYPE.eq( ObjectType.DIR ) ) );
    }

    ResultQuery<Record2<UUID, Ltree>> createMovePath(Ltree newPath, TreeRecord curRecord, CloudUser cloudUser) {
        CommonTableExpression<Record2<Ltree, Integer>> oldPathCte = name( "oldPath" ).fields( "path", "level" )
                .as( selectDirPathAndLevel( curRecord, cloudUser ) );
        return dsl.with( oldPathCte ).select( TREE.OBJECT_ID,
                        when( oldPathCte.field( "level", Integer.class ).eq( nlevel( TREE.PATH ) ), newPath )
                                .otherwise( ltreeAddltree( val( newPath ),
                                        subpath2( TREE.PATH, nlevel( oldPathCte.field( "path", Ltree.class ) ) ) ) )
                                .as( "path" ) )
                .from( TREE, oldPathCte )
                .where( TREE.USER_ID.eq( cloudUser.getUserId() ).and(
                        ltreeIsparent( oldPathCte.field( "path", Ltree.class ), TREE.PATH ) ) );
    }

    @Transactional
    public Mono<Long> mvDir(Ltree newPath, TreeRecord curRecord, CloudUser cloudUser) {
        if (curRecord.getObjectType() != ObjectType.DIR) {
            return Mono.just( 0L );
        }
        var movePathCte = name( "new" ).fields( "object_id", "path" )
                .as( createMovePath( newPath, curRecord, cloudUser ) );
        return Mono.from( dsl.with( movePathCte )
                        .update( TREE )
                        .set( TREE.PATH, movePathCte.field( "path", Ltree.class ) )
                        .from( movePathCte )
                        .where( TREE.OBJECT_ID.eq( movePathCte.field( "object_id", UUID.class ) ) ) )
                // This is a workaround for a jOOQ bug, Method signature is Integer but actually returns a Long at runtime
                .map( (Number o) -> o.longValue() );
    }

    @Transactional
    public Flux<Record3<UUID, UUID, ObjectType>> cpDir(Ltree destination, TreeRecord sourceRecord, CloudUser cloudUser) {
        if (sourceRecord.getObjectType() != ObjectType.DIR) {
            return Flux.empty();
        }
        var copyCte = name( "copy_records" ).fields("source_id", "object_id", "object_type",
                "path", "user_id", "created_at").as( copySubTreeForInsert( destination, sourceRecord, cloudUser ) );
        var insertCte = name( "insert_res" ).as(
                dsl.insertInto( TREE )
                        .select( dsl.select( copyCte.fields( "object_id", "object_type",
                                "path", "user_id", "created_at" ) ).from( copyCte ) )
                        .returning(TREE.OBJECT_ID));
        return Flux.from( dsl.with(copyCte).with(insertCte).select( copyCte.field( "source_id", UUID.class ),
                    copyCte.field( "object_id", UUID.class ).as("destination_id"),
                    copyCte.field( "object_type", ObjectType.class ) )
                .from( copyCte ) );
    }

    public Flow.Publisher<Record3<UUID,UUID,ObjectType>> cpCommon(
            SelectConditionStep<Record6<UUID, UUID, ObjectType, Ltree, UUID, OffsetDateTime>> selectRecordCopies) {
        var copyCte = name( "copy_records" ).fields("source_id", "object_id", "object_type",
                "path", "user_id", "created_at").as( selectRecordCopies );
        var insertCte = name( "insert_res" ).as(
                dsl.insertInto( TREE )
                        .select( dsl.select( copyCte.fields( "object_id", "object_type",
                                "path", "user_id", "created_at" ) ).from( copyCte ) )
                        .returning(TREE.OBJECT_ID));
        return dsl.with(copyCte).with(insertCte).select( copyCte.field( "source_id", UUID.class ),
                        copyCte.field( "object_id", UUID.class ).as("destination_id"),
                        copyCte.field( "object_type", ObjectType.class ) )
                .from( copyCte );
    }

    // Does not includes self (parent)
    // Creates new uuid and new timestamp, and converts path, other fields remain the same;
SelectConditionStep<Record6<UUID, UUID, ObjectType, Ltree, UUID, OffsetDateTime>> copySubTreeForInsert(
            Ltree destination, TreeRecord sourceRecord, CloudUser cloudUser) {
        CommonTableExpression<Record2<Ltree, Integer>> oldPathCte = name( "oldPath" ).fields( "path", "level" )
                .as( selectDirPathAndLevel( sourceRecord, cloudUser ) );
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

    SelectConditionStep<Record6<UUID, UUID, ObjectType, Ltree, UUID, OffsetDateTime>> copyFileForInsert(
            Ltree destination, TreeRecord sourceRecord, CloudUser cloudUser) {
        return dsl.select(
                        TREE.OBJECT_ID.as( "source_id" ),
                        uuidGenerateV4().as( TREE.OBJECT_ID ), TREE.OBJECT_TYPE,
                        val( destination ).as( TREE.PATH ), TREE.USER_ID, currentOffsetDateTime().as( TREE.CREATED_AT ) )
                .from( TREE )
                .where( TREE.OBJECT_ID.eq( sourceRecord.getObjectId() )
                        .and( TREE.USER_ID.eq( cloudUser.getUserId() ) )
                        .and(TREE.OBJECT_TYPE.eq(ObjectType.FILE) ) );
    }

        @Transactional
    public Mono<Record2<UUID,UUID>> cpFile(Ltree destination, TreeRecord sourceRecord, CloudUser cloudUser) {
        if (sourceRecord.getObjectType() != ObjectType.FILE) {
            return Mono.empty();
        }
        var copyCte = name( "copy_record" )
                .fields("source_id", "object_id", "object_type",
                        "path", "user_id", "created_at" )
                .as( copyFileForInsert( destination, sourceRecord, cloudUser ) );
        var insertCte = name( "insert_res" ).as(
                dsl.insertInto( TREE )
                        .select( dsl.select( copyCte.fields( "object_id", "object_type",
                                "path", "user_id", "created_at" ) ).from( copyCte ) )
                        .returning(TREE.OBJECT_ID));
        return Mono.from( dsl.with( copyCte ).with(insertCte).insertInto( TREE ).select( copyCte ) )
                .map( Number::longValue );
    }


//    // Does not include self (parent) in results
//   SelectConditionStep<TreeRecord> selectChildren(TreeRecord treeRecord, CloudUser cloudUser) {
//        return selectChildrenAndParent( treeRecord, cloudUser )
//                .and( DSL.val( treeRecord.getPath() ).notEqual( TREE.PATH ) );
//    }
}
