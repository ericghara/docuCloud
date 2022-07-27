package com.ericgha.docuCloud.service;

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
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.stream.Stream;

import static com.ericgha.docuCloud.jooq.Routines.*;
import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static org.jooq.impl.DSL.*;

@Service
@RequiredArgsConstructor
public class TreeService {

    private final DSLContext dsl;
    private final Environment env;

    @Transactional
    public Mono<TreeRecord> create(String path, ObjectType type) {
        return Mono.from( dsl.insertInto( TREE )
                .set( TREE.OBJECT_ID, UUID.randomUUID().toString() )
                .set( TREE.OBJECT_TYPE, type )
                .set( TREE.PATH, Ltree.valueOf( path ) )
                .returning( TREE.OBJECT_ID, TREE.OBJECT_TYPE, TREE.PATH )
        );
    }

    @Transactional
    public Mono<TreeRecord> updatePath(String objectId, String path) {
        return Mono.from( dsl.update( TREE )
                .set( TREE.PATH, Ltree.valueOf( path ) )
                .where( TREE.OBJECT_ID.eq( objectId ) )
                .returning( TREE.OBJECT_ID, TREE.OBJECT_TYPE, TREE.PATH ) );
    }

    SelectConditionStep<Record2<Ltree, Integer>> selectPathAndLevel(String objectId) {
        return dsl.select( TREE.PATH.as("path"),
                nlevel( TREE.PATH ).as("level") )
                .from( TREE )
                .where( TREE.OBJECT_ID.eq( objectId ) );
    }

    ResultQuery<Record2<String, Ltree>> createMovePath(String objectId, Ltree newPath) {
        CommonTableExpression<Record2<Ltree, Integer>> oldPathCte = name( "oldPath" ).fields( "path", "level" )
                .as( selectPathAndLevel( objectId ) );
        return dsl.with( oldPathCte ).select( TREE.OBJECT_ID,
                        when( oldPathCte.field( "level", Integer.class ).eq( nlevel( TREE.PATH ) ), newPath )
                                .otherwise( ltreeAddltree( val( newPath ),
                                        subpath2( TREE.PATH, nlevel( oldPathCte.field( "path", Ltree.class ) ) ) ) )
                                .as( "path" ) )
                .from( TREE, oldPathCte )
                .where( ltreeIsparent( oldPathCte.field( "path", Ltree.class ), TREE.PATH ) );
    }

    @Transactional
    public Mono<Long> mvPath(String objectId, Ltree newPath) {
        var movePathCte = name( "new" ).fields( "object_id", "path" )
                .as( createMovePath( objectId, newPath ) );
        return Mono.from( dsl.with( movePathCte )
                        .update( TREE )
                        .set( TREE.PATH, movePathCte.field( "path", Ltree.class ) )
                        .from( movePathCte )
                        .where( TREE.OBJECT_ID.eq( movePathCte.field( "object_id", String.class ) ) ) )
                // This is a workaround for a jOOQ bug, Method signature is Integer but actually returns a Long at runtime
                .map( (Number o) -> o.longValue() );
    }

    @Transactional
    public Mono<TreeRecord> select(String queryPath) {
        return Mono.from( dsl.selectFrom( TREE ).where( TREE.PATH.eq( Ltree.valueOf( queryPath ) ) ) );
    }

    @Transactional
    // Does not include self (parent) in results
    Flux<TreeRecord> selectChildren(Ltree parent) {
        return Flux.from( dsl.selectFrom(TREE)
                .where(ltreeIsparent( DSL.val(parent), TREE.PATH ) )
                    .and( DSL.val(parent).notEqual( TREE.PATH ) )
                .orderBy( nlevel(TREE.PATH).asc(), TREE.PATH.asc() ) );
    }

    @Transactional
    // Does not includes self (parent)
    Flux<TreeRecord> selectChildrenAndParent(Ltree parent) {
        return Flux.from( dsl.selectFrom(TREE)
                .where(ltreeIsparent( DSL.val(parent), TREE.PATH ) )
                .orderBy( nlevel(TREE.PATH).asc(), TREE.PATH.asc() ) );
    }

    @Transactional
    // for testing
    Mono<TreeRecord> create(TreeRecord record) {
        if(record.getObjectId().length() > 36) {
            throw new IllegalArgumentException("object_id must be <= 36 characters");
        }
        if (Stream.of(env.getActiveProfiles()).anyMatch(  prof -> prof.matches("(?i)prod.*")  ) ) {
            throw new IllegalStateException( "Do not use this method in production" );
        }
        return Mono.from( dsl.insertInto( TREE )
                .set( TREE.OBJECT_ID, record.getObjectId() )
                .set( TREE.OBJECT_TYPE, record.getObjectType() )
                .set( TREE.PATH, record.getPath() )
                .returning( TREE.OBJECT_ID, TREE.OBJECT_TYPE, TREE.PATH )
        );
    }
}
