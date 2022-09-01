package com.ericgha.docuCloud.util.comparator;

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

    static public Comparator<FileViewDto> compareByLinkedAtUploadedAtFileId() {
        return Comparator.comparing( FileViewDto::getLinkedAt )
                .thenComparing( FileViewDto::getUploadedAt )
                .thenComparing( FileViewDto::getFileId );
    }
}
