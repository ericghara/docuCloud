package com.ericgha.docuCloud.util.comparators;

import com.ericgha.docuCloud.jooq.tables.records.TreeJoinFileRecord;

import java.util.Comparator;

public class TreeJoinFileRecordComparators {

    public static Comparator<TreeJoinFileRecord> sortByObjectIdFileIdLinkedAt() {
        return Comparator.comparing(TreeJoinFileRecord::getObjectId)
                .thenComparing( TreeJoinFileRecord::getFileId )
                .thenComparing( TreeJoinFileRecord::getLinkedAt);
    }
}
