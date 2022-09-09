package com.ericgha.docuCloud.repository.testtool.tree;

import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.service.JooqTransaction;
import lombok.RequiredArgsConstructor;
import org.jooq.impl.DSL;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static com.ericgha.docuCloud.jooq.Tables.TREE;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.defaultValue;

@RequiredArgsConstructor
@Component
public class TreeTestQueries {

    private final JooqTransaction jooqTx;

    public Mono<TreeDto> create(ObjectType objectType, Ltree path, UUID userId) {
        return jooqTx.withConnection( dsl -> dsl.insertInto( TREE )
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
        return jooqTx.withConnection( dsl -> dsl.update( TREE )
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
        return jooqTx.withConnectionMany( dsl -> dsl.selectFrom( TREE )
                        .where( TREE.USER_ID.eq( userId ) )
                        .orderBy( TREE.OBJECT_ID.asc() ) )
                .map(treeRecord -> treeRecord.into( TreeDto.class) )
                .collectList()
                .block();
    }

    public TreeDto getByObjectId(UUID objectId) {
        return jooqTx.withConnection( dsl -> dsl.selectFrom( TREE )
            .where( TREE.OBJECT_ID.eq( objectId ) ) )
            .mapNotNull(treeRecord -> treeRecord.into( TreeDto.class) )
           .block();
    }

    public TreeDto getByObjectPath(String pathStr, UUID userId) {
        return jooqTx.withConnection( dsl -> dsl.selectFrom(TREE)
                .where(TREE.USER_ID.eq(userId)
                        .and( TREE.PATH.eq(Ltree.valueOf( pathStr )))))
                .map(treeRecord -> treeRecord.into( TreeDto.class) )
                .block();
    }
}
