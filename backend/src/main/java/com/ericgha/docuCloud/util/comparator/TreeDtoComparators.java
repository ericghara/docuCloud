package com.ericgha.docuCloud.util.comparator;

import com.ericgha.docuCloud.dto.TreeDto;

import java.util.Comparator;

public class TreeDtoComparators {

    static Comparator<TreeDto> compareByLtree() {
        return Comparator.comparing( TreeDto::getPathStr )
                .thenComparing( TreeDto::getObjectId );
    }

    static public Comparator<TreeDto> compareByObjectId() {
        return Comparator.comparing(TreeDto::getObjectId);
    }
}
