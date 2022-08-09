package com.ericgha.docuCloud.util.comparators;

import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;

import java.util.Comparator;

public class FileViewRecordComparators {

    static public Comparator<FileViewRecord> compareByObjectIdFileId() {
        return Comparator.comparing(  FileViewRecord::getObjectId )
                .thenComparing( FileViewRecord::getFileId );
    }

    static public Comparator<FileViewRecord> compareBySizeObjectId() {
        return Comparator.comparingLong( FileViewRecord::getSize )
                .thenComparing( FileViewRecord::getObjectId );

    }

}
