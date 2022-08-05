package com.ericgha.docuCloud.service;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.FileRecord;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static com.ericgha.docuCloud.jooq.Tables.FILE_VIEW;
import static org.jooq.impl.DSL.*;

@Service
@RequiredArgsConstructor
public class FileService {

    private final DSLContext dsl;
    private final TreeService treeService;

    // TODO ensure no ref to non file objects
    // TODO ensure each file has object degree >= 1
    // TODO

    // TODO isObjectType test

    public Mono<Long> linkExistingFile(FileRecord fileRecord, TreeRecord treeRecord, CloudUser cloudUser) {
        SelectConditionStep<Record1<Boolean>> isFile = treeService.isObjectType( ObjectType.FILE, treeRecord, cloudUser );

        return Mono.from(dsl.insertInto(FILE_VIEW)
                .select( dsl.select(val(fileRecord.getFileId() ), val(treeRecord.getObjectId() ), val(currentOffsetDateTime()) )
                        .where( val(true).eq(isFile) ) ) )
                .map( Long::valueOf );
    }

    SelectJoinStep<Record1<Boolean>> isUsersFile(FileRecord fileRecord, CloudUser cloudUser) {
        Table<Record2<UUID, UUID>> fileLinks = selectLinksToFile(fileRecord, cloudUser).limit(1).asTable();

        return dsl.select( when(count().eq( 0 ), false )
                        .otherwise( true ))
                .from(fileLinks);
    }

    // if user does not own file will always return null, this will therefore trigger a delete request
    // but the delete will never be successful b/c the file is in a prefix that the user does not own
    SelectConditionStep<Record2<UUID,UUID>> selectLinksToFile(FileRecord fileRecord, CloudUser cloudUser) {
        return dsl.select( FILE_VIEW.OBJECT_ID, FILE_VIEW.FILE_ID )
                .from(FILE_VIEW)
                .where(FILE_VIEW.FILE_ID.eq(fileRecord.getFileId())
                    .and(FILE_VIEW.USER_ID.eq(cloudUser.getUserId())) );
    }

}
