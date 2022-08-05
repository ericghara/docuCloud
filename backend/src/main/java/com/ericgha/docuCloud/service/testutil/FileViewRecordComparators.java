package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;

import java.util.Comparator;

public class FileViewRecordComparators {

    static public Comparator<FileViewRecord> objectIdFileIdTime() {
        return Comparator.comparing(  FileViewRecord::getObjectId )
                .thenComparing( FileViewRecord::getFileId )
                .thenComparing( FileViewRecord::getLinkedAt );
    }

    static public Comparator<FileViewRecord> sizeObjectId() {
        return Comparator.comparingLong( FileViewRecord::getSize )
                .thenComparing( FileViewRecord::getObjectId );

    }

}
