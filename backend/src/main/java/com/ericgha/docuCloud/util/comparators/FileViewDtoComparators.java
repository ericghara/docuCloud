package com.ericgha.docuCloud.util.comparators;

import com.ericgha.docuCloud.dto.FileViewDto;

import java.util.Comparator;

public class FileViewDtoComparators {

    static public Comparator<FileViewDto> compareByObjectIdFileId() {
        return Comparator.comparing(  FileViewDto::getObjectId )
                .thenComparing( FileViewDto::getFileId );
    }

    static public Comparator<FileViewDto> compareBySizeObjectId() {
        return Comparator.comparingLong( FileViewDto::getSize )
                .thenComparing( FileViewDto::getObjectId );

    }

}
