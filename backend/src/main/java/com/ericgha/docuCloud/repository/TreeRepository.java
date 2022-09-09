package com.ericgha.docuCloud.repository;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.exceptions.IllegalObjectTypeException;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.Tree;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import com.ericgha.docuCloud.service.JooqTransaction;
import com.ericgha.docuCloud.util.validator.TreeDtoValidator;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.jooq.CommonTableExpression;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Record6;
import org.jooq.ResultQuery;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.TableLike;
import org.jooq.impl.DSL;
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
import static com.ericgha.docuCloud.jooq.enums.ObjectType.DIR;
import static com.ericgha.docuCloud.jooq.enums.ObjectType.FILE;
import static org.jooq.impl.DSL.*;

@Repository
@RequiredArgsConstructor
public class TreeRepository {

    // TODO lsDir(Ltree path, CloudUser clouduser)
    private final JooqTransaction jooqTx;


    // required treeDto fields: objectType, path

    public Mono<TreeDto> create(TreeDto treeDto, CloudUser cloudUser) {
        return jooqTx.withConnection( dsl -> dsl.insertInto( TREE )
                        .set( TREE.OBJECT_ID, UUID.randomUUID() )
                        .set( TREE.OBJECT_TYPE, treeDto.getObjectType() )
                        .set( TREE.PATH, treeDto.getPath() )
                        .set( TREE.USER_ID, cloudUser.getUserId() )
                        .set( TREE.CREATED_AT, defaultValue( OffsetDateTime.class ) )
                        .returning( asterisk() ) )
                .map( treeRecord -> treeRecord.into( TreeDto.class ) );
    }


    public Mono<Long> mvFile(TreeDto source, Ltree destination, CloudUser cloudUser) {
        if (source.getObjectType() != FILE) {
            return Mono.empty();
        }
        return jooqTx.withConnection( dsl -> dsl.update( TREE )
                        .set( TREE.PATH, destination )
                        .where( TREE.OBJECT_ID.eq( source.getObjectId() )
                                .and( TREE.USER_ID.eq( cloudUser.getUserId() ) )
                                .and( TREE.OBJECT_TYPE.eq( FILE ) ) )
                )
                // This is a workaround for a jOOQ bug, Method signature is Integer but actually returns a Long at runtime
                .map( (Number o) -> o.longValue() );
    }


    public Mono<Long> mvDir(TreeDto source, Ltree destination, CloudUser cloudUser) throws IllegalObjectTypeException {
        TreeDtoValidator.mustBeObjectType( source, DIR );
        return jooqTx.withConnection( dsl -> {
                    var movePathCte = name( "new" ).fields( "object_id", "path" )
                            .as( createMovePath( destination, source, cloudUser, dsl ) );
                    return dsl.with( movePathCte )
                            .update( TREE )
                            .set( TREE.PATH, movePathCte.field( "path", Ltree.class ) )
                            .from( movePathCte )
                            .where( TREE.OBJECT_ID.eq( movePathCte.field( "object_id", UUID.class ) ) );
                } )
                // This is a workaround for a jOOQ bug, Method signature is Integer but actually returns a Long at runtime
                .map( (Number o) -> o.longValue() );
    }


    public Flux<TreeDto> rmDirRecursive(TreeDto record, CloudUser cloudUser) throws IllegalObjectTypeException {
        TreeDtoValidator.mustBeObjectType( record, DIR );
        return jooqTx.withConnectionMany( dsl -> {
                    // Doesn't perform internal check of proper objectType as a creating a subtree from a spoofed file is not expensive
                    SelectConditionStep<Record1<UUID>> delObjectIds = selectDescendents( record, cloudUser, dsl );
                    return dsl.delete( TREE )
                            .where( TREE.OBJECT_ID.in( delObjectIds ) )
                            .returning( asterisk() );
                } )
                .map( TreeDto::fromRecord );
    }


    public Mono<TreeDto> rmNormal(TreeDto record, CloudUser cloudUser) {
        return jooqTx.withConnection( dsl -> {
                    // Doesn't perform internal check of proper objectType as a creating a subtree from a spoofed file is not expensive
                    SelectJoinStep<Record1<Integer>> doDel = hasDescendents( record, cloudUser, dsl );
                    return dsl.delete( TREE )
                            .where( TREE.OBJECT_ID.eq( record.getObjectId() )
                                    .and( val( 1 ).eq( doDel ) ) )
                            .returning( asterisk() );
                } )
                .map( TreeDto::fromRecord );
    }

    // returning source_id, destination_id, object_type

    public Flux<Record3<UUID, UUID, ObjectType>> cpDir(TreeDto source, Ltree destination, CloudUser cloudUser) {
        TreeDtoValidator.mustBeObjectType( source, DIR );
        return jooqTx.withConnectionMany( dsl -> {
            var selectRecordCopies = fetchDirCopyRecords( destination, source, cloudUser, dsl );
            return cpCommon( selectRecordCopies, dsl );
        } );
    }


    // Value of objectType field in records will always be ObjectType.FILE; returning ObjectType is a compromise for code usability with cpDir.
    // uses flexibleSelect so source must have path and/or object_id specified.
    public Mono<Record3<UUID, UUID, ObjectType>> cpFile(TreeDto source, Ltree destination, CloudUser cloudUser) {
        TreeDtoValidator.mustBeObjectType( source, FILE );
        return jooqTx.withConnection( dsl -> {
            var selectRecordCopies = fetchFileCopyRecords( destination, source, cloudUser, dsl );
            return cpCommon( selectRecordCopies, dsl );
        } );
    }

    /**
     * This works a little differently than a traditional UNIX ls.  The direct descendents of the source are returned
     * along WITH the source.  This allows differentiation between <ol><li>The parent has no children</li>
     * <li>The parent does not exist.</li></ol>  (1) will return only the source
     * and (2) will return a null set.
     * <br><br>
     * This query is designed to be flexible, and allows TreeDtos that define one or both of, {@code objectId},
     * {@code path}.  If both are specified a match based on both is used.
     *
     * @param source
     * @param cloudUser
     * @return records (if any) ordered by path ascending.  Source, if found, is guaranteed to be the first record
     * returned.  If source is NOT found will always return null set.
     */
    @SneakyThrows
    public Flux<TreeDto> ls(TreeDto source, CloudUser cloudUser) {
        CommonTableExpression<TreeRecord> parent = name( "parent" ).as(
                this.flexibleSelect( source, cloudUser ) );
        return jooqTx.withConnectionMany( dsl ->
                        dsl.with( parent )
                                .select( TREE.asterisk() )
                                .from( TREE, parent )
                                .where( ltreeIsparent( parent.field( TREE.PATH ), TREE.PATH ) )
                                .and( nlevel( TREE.PATH ).le( nlevel( parent.field( TREE.PATH ) ).plus( 1 ) ) )
                                .and( TREE.USER_ID.eq( cloudUser.getUserId() ) )
                                .orderBy( TREE.PATH.asc() )
                                .coerce( TREE )
                )
                .map( TreeDto::fromRecord );
    }

    // Query designed such that one or both of Object_id and path are used, therefore one of these fields may be null
    ResultQuery<TreeRecord> flexibleSelect(TreeDto treeDto, CloudUser cloudUser) {
        UUID objectId = treeDto.getObjectId();
        Ltree path = treeDto.getPath();
        if (Objects.isNull( objectId ) && Objects.isNull( path )) {
            throw new IllegalArgumentException( "One or both of objectId and path must be non-null.  Instead both were null" );
        }
        var conditions = TREE.USER_ID.eq( Objects.requireNonNull( cloudUser.getUserId(), "userId was null" ) );
        if (Objects.nonNull( objectId )) {
            conditions = conditions.and( val( objectId ).eq( TREE.OBJECT_ID ) );
        }
        if (Objects.nonNull( path )) {
            conditions = conditions.and( val( path ).eq( TREE.PATH ) );
        }
        return DSL.select( asterisk() )
                .from( TREE ).where( conditions ).limit( 1 )
                .coerce( TREE );
    }

    // the purpose of this query is to check that the source is owned by the user
    SelectConditionStep<Record2<Ltree, Integer>> selectDirPathAndLevel(TreeDto sourceRecord, CloudUser cloudUser, DSLContext dsl) {
        CommonTableExpression<TreeRecord> source = name( "source" ).as(
                this.flexibleSelect( sourceRecord, cloudUser ) );
        // hack to use cte, but can't figure out how to convert ResultQuery to TableLike
        Field<Ltree> treePath = source.field( TREE.PATH );
        return dsl.with( source )
                .select( treePath.as( "path" ),
                        nlevel( treePath ).as( "level" ) )
                .from( source )
                .where( source.field( TREE.OBJECT_TYPE ).eq( DIR ) );
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
                .as( dsl.select( TREE.OBJECT_ID )
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

    // returns fields source_id, destination_id, object_type
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
        CommonTableExpression<TreeRecord> source = name( "source" ).as(
                this.flexibleSelect( sourceRecord, cloudUser ) );
        return dsl.with( source )
                .select( source.field( TREE.OBJECT_ID ).as( "source_id" ),
                        uuidGenerateV4().as( TREE.OBJECT_ID ),
                        source.field( TREE.OBJECT_TYPE ),
                        val( destination ).as( TREE.PATH ),
                        source.field( TREE.USER_ID ),
                        currentOffsetDateTime().as( TREE.CREATED_AT ) )
                .from( source )
                .where( source.field( TREE.OBJECT_TYPE ).eq( FILE ) );
    }

    SelectConditionStep<Record1<Boolean>> isObjectType(ObjectType objectType, TreeDto treeDto, CloudUser cloudUser, DSLContext dsl) {
        return dsl.select( when( count( Tree.TREE.OBJECT_ID ).eq( 0 ), false ).otherwise( true ) )
                .from( Tree.TREE )
                .where( Tree.TREE.OBJECT_ID.eq( treeDto.getObjectId() )
                        .and( Tree.TREE.OBJECT_TYPE.eq( objectType ) )
                        .and( Tree.TREE.USER_ID.eq( cloudUser.getUserId() ) ) );
    }

    //todo delete me
    SelectConditionStep<TreeRecord> selectAll(CloudUser cloudUser, DSLContext dsl) {
        return dsl.selectFrom( TREE ).where( TREE.USER_ID.eq( cloudUser.getUserId() ) );
    }
}
