package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.defaultValue;

@RequiredArgsConstructor
public class TreeTestQueries {

    private final DSLContext dsl;

    @Transactional
    public Mono<TreeRecord> create(ObjectType objectType, Ltree path, String userId) {
        return Mono.from( dsl.insertInto( TREE )
                .set( TREE.OBJECT_ID, defaultValue( String.class ) )
                .set( TREE.OBJECT_TYPE, objectType )
                .set( TREE.PATH, path )
                .set( TREE.USER_ID, userId )
                .set( TREE.CREATED_AT, defaultValue( LocalDateTime.class ) )
                .returning( asterisk() )
        );
    }

    @Transactional(readOnly = true)
    public List<TreeRecord> getAllUserObjects(String userId) {
        return Flux.from( dsl.selectFrom( TREE )
                        .where( TREE.USER_ID.eq( userId ) )
                        .orderBy( TREE.OBJECT_ID.asc() ) )
                .collectList()
                .block();
    }

    @Transactional(readOnly = true)
    public TreeRecord getByObjectId(String objectId) {
        return Mono.from( dsl.selectFrom( TREE )
            .where( TREE.OBJECT_ID.eq( objectId ) ) )
           .block();
    }

}
