package com.ericgha.docuCloud.repository.testutil.tree;

import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.defaultValue;

@RequiredArgsConstructor
@Repository
@Profile("test")
public class TreeTestQueries {

    private final DSLContext dsl;

    public Mono<TreeDto> create(ObjectType objectType, Ltree path, UUID userId) {
        return Mono.from( dsl.insertInto( TREE )
                .set( TREE.OBJECT_ID, defaultValue( UUID.class ) )
                .set( TREE.OBJECT_TYPE, objectType )
                .set( TREE.PATH, path )
                .set( TREE.USER_ID, userId )
                .set( TREE.CREATED_AT, defaultValue( OffsetDateTime.class ) )
                .returning( asterisk() )
        )
        .map(treeRecord -> treeRecord.into( TreeDto.class) );
    }

    public Mono<TreeDto> update(TreeDto dto) {
        return Mono.from( dsl.update( TREE )
                .set( TREE.OBJECT_TYPE, dto.getObjectType() )
                .set( TREE.PATH, dto.getPath() )
                .set( TREE.USER_ID, dto.getUserId())
                .set( TREE.CREATED_AT,  dto.getCreatedAt() )
                .where( TREE.OBJECT_ID.eq(DSL.val(dto.getObjectId() ) ) )
                .returning( asterisk() )
        )
        .map(treeRecord -> treeRecord.into( TreeDto.class) );
    }

    public List<TreeDto> getAllUserObjects(UUID userId) {
        return Flux.from( dsl.selectFrom( TREE )
                        .where( TREE.USER_ID.eq( userId ) )
                        .orderBy( TREE.OBJECT_ID.asc() ) )
                .map(treeRecord -> treeRecord.into( TreeDto.class) )
                .collectList()
                .block();
    }

    public TreeDto getByObjectId(UUID objectId) {
        return Mono.from( dsl.selectFrom( TREE )
            .where( TREE.OBJECT_ID.eq( objectId ) ) )
            .map(treeRecord -> treeRecord.into( TreeDto.class) )
           .block();
    }

    public TreeDto getByObjectPath(String pathStr, UUID userId) {
        return Mono.from(dsl.selectFrom(TREE)
                .where(TREE.USER_ID.eq(userId)
                        .and( TREE.PATH.eq(Ltree.valueOf( pathStr )))))
                .map(treeRecord -> treeRecord.into( TreeDto.class) )
                .block();
    }
}
