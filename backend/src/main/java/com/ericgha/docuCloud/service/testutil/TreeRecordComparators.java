package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;

import java.util.UUID;

public class TreeRecordComparators {

    static public int compareByLtree(TreeRecord a, TreeRecord b) {
        String pathA = a.getPath().data();
        String pathB = b.getPath().data();
        return pathA.compareTo(pathB);
    }

    static public int compareByObjectId(TreeRecord a, TreeRecord b) {
        UUID idA = a.getObjectId();
        UUID idB = b.getObjectId();
        return idA.compareTo(idB);
    }
}
