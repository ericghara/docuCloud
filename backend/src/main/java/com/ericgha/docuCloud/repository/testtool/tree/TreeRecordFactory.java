package com.ericgha.docuCloud.repository.testtool.tree;

import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import org.jooq.postgres.extensions.types.Ltree;

import java.util.UUID;

final public class TreeRecordFactory {

    public static TreeRecord createFromPathStr(UUID objectId, String path, ObjectType objectType) {
        return new TreeRecord().setObjectId( objectId )
                .setPath( Ltree.valueOf( path ) )
                .setObjectType( objectType );
    }
}
