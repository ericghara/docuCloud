package com.ericgha.docuCloud.util.comparators;


import com.ericgha.docuCloud.dto.TreeJoinFileDto;

import java.util.Comparator;

public class TreeJoinFileDtoComparators {

    public static Comparator<TreeJoinFileDto> sortByObjectIdFileIdLinkedAt() {
        return Comparator.comparing(TreeJoinFileDto::getObjectId)
                .thenComparing( TreeJoinFileDto::getFileId )
                .thenComparing( TreeJoinFileDto::getLinkedAt);
    }
}
