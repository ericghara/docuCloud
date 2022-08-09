package com.ericgha.docuCloud.util.comparators;

import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;

import java.util.Comparator;

public class TreeRecordComparators {

    static Comparator<TreeRecord> compareByLtree() {
        return Comparator.comparing( tr -> tr.getPath().data() );
    }

    static public Comparator<TreeRecord> compareByObjectId() {
        return Comparator.comparing(TreeRecord::getObjectId);
    }
}
