package com.ericgha.docuCloud.factory;

import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import org.jooq.postgres.extensions.types.Ltree;

final public class TreeRecordFactory {

    public static TreeRecord createFromPathStr(String objectId, String path, ObjectType objectType) {
        return new TreeRecord().setObjectId( objectId )
                .setPath( Ltree.valueOf( path ) )
                .setObjectType( objectType );
    }
}
