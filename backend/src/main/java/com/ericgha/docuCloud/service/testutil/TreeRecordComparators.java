package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;

public class TreeRecordComparators {

    static public int compareByLtree(TreeRecord a, TreeRecord b) {
        String pathA = a.getPath().data();
        String pathB = b.getPath().data();
        return pathA.compareTo(pathB);
    }

    static public int compareByObjectId(TreeRecord a, TreeRecord b) {
        String idA = a.getObjectId();
        String idB = b.getObjectId();
        return idA.compareTo(idB);
    }
}
