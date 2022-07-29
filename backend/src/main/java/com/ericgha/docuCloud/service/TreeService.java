package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.CommonTableExpression;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.ResultQuery;
import org.jooq.SelectConditionStep;
import org.jooq.impl.DSL;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.Routines.*;
import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static org.jooq.impl.DSL.*;

@Service
@RequiredArgsConstructor
public class TreeService {

    private final DSLContext dsl;
    private final Environment env;

    @Transactional
    public Mono<TreeRecord> create(TreeRecord treeRecord, CloudUser cloudUser) {
        return Mono.from( dsl.insertInto( TREE )
                .set( TREE.OBJECT_ID, UUID.randomUUID().toString() )
                .set( TREE.OBJECT_TYPE, treeRecord.getObjectType() )
                .set( TREE.PATH, treeRecord.getPath() )
                .set( TREE.USER_ID, cloudUser.getUserId() )
                .set( TREE.CREATED_AT, defaultValue( LocalDateTime.class ) )
                .returning( asterisk() )
        );
    }

    @Transactional
    public Mono<Integer> mvFile(Ltree newPath, TreeRecord treeRecord, CloudUser cloudUser) {
        return Mono.from( dsl.update( TREE )
                .set( TREE.PATH, treeRecord.getPath() )
                .where( TREE.OBJECT_ID.eq( treeRecord.getObjectId() )
                        .and(TREE.USER_ID.eq(cloudUser.getUserId() ) )
                        .and(TREE.OBJECT_TYPE.eq(ObjectType.FILE) ) ) );
    }

    SelectConditionStep<Record2<Ltree, Integer>> selectPathAndLevel(TreeRecord curRecord, CloudUser cloudUser) {
        return dsl.select( TREE.PATH.as( "path" ),
                        nlevel( TREE.PATH ).as( "level" ) )
                .from( TREE )
                .where( TREE.OBJECT_ID.eq( curRecord.getObjectId() ).and(TREE.USER_ID.eq(cloudUser.getUserId() ) ) );
    }

    ResultQuery<Record2<String, Ltree>> createMovePath(Ltree newPath, TreeRecord curRecord, CloudUser cloudUser) {
        CommonTableExpression<Record2<Ltree, Integer>> oldPathCte = name( "oldPath" ).fields( "path", "level" )
                .as( selectPathAndLevel( curRecord, cloudUser ) );
        return dsl.with( oldPathCte ).select( TREE.OBJECT_ID,
                        when( oldPathCte.field( "level", Integer.class ).eq( nlevel( TREE.PATH ) ), newPath )
                                .otherwise( ltreeAddltree( val( newPath ),
                                        subpath2( TREE.PATH, nlevel( oldPathCte.field( "path", Ltree.class ) ) ) ) )
                                .as( "path" ) )
                .from( TREE, oldPathCte )
                .where( TREE.USER_ID.eq(cloudUser.getUserId()).and(
                        ltreeIsparent( oldPathCte.field( "path", Ltree.class ), TREE.PATH ) ) );
    }

    @Transactional
    public Mono<Long> mvPath(TreeRecord curRecord, Ltree newPath, CloudUser cloudUser) {
        var movePathCte = name( "new" ).fields( "object_id", "path" )
                .as( createMovePath( newPath, curRecord, cloudUser ) );
        return Mono.from( dsl.with( movePathCte )
                        .update( TREE )
                        .set( TREE.PATH, movePathCte.field( "path", Ltree.class ) )
                        .from( movePathCte )
                        .where( TREE.OBJECT_ID.eq( movePathCte.field( "object_id", String.class ) ) ) )
                // This is a workaround for a jOOQ bug, Method signature is Integer but actually returns a Long at runtime
                .map( (Number o) -> o.longValue() );
    }

    @Transactional
    public Mono<TreeRecord> select(Ltree queryPath) {
        return Mono.from( dsl.selectFrom( TREE ).where( TREE.PATH.eq( queryPath ) ) );
    }

    @Transactional
        // Does not include self (parent) in results
    Flux<TreeRecord> selectChildren(Ltree parent) {
        return Flux.from( dsl.selectFrom( TREE )
                .where( ltreeIsparent( DSL.val( parent ), TREE.PATH ) )
                .and( DSL.val( parent ).notEqual( TREE.PATH ) )
                .orderBy( nlevel( TREE.PATH ).asc(), TREE.PATH.asc() ) );
    }

    @Transactional
        // Does not includes self (parent)
    Flux<TreeRecord> selectChildrenAndParent(Ltree parent) {
        return Flux.from( dsl.selectFrom( TREE )
                .where( ltreeIsparent( DSL.val( parent ), TREE.PATH ) )
                .orderBy( nlevel( TREE.PATH ).asc(), TREE.PATH.asc() ) );
    }
}
