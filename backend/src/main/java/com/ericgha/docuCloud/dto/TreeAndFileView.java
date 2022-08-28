package com.ericgha.docuCloud.dto;

import lombok.NonNull;


public record TreeAndFileView(@NonNull TreeDto treeDto, FileViewDto fileViewDto) implements Comparable<TreeAndFileView> {

    /**
     * Compares {@code TreeDto} then {@code FileViewDto} using
     * natural ordering.  Since {@code FileViewDto} is nullable
     * it is compared in a nulls first manner.
     * @param other the object to be compared.
     * @return
     */
    @Override
    public int compareTo(TreeAndFileView other) {
        int comp = this.treeDto.compareTo( other.treeDto );
        if (comp != 0) {
            return comp;
        }
        var thisFile = this.fileViewDto;
        var otherFile = other.fileViewDto;
        if (thisFile != null && otherFile != null) {
            return this.fileViewDto.compareTo( other.fileViewDto );
        }
        if (thisFile == null && otherFile == null) {
            return 0;
        }
        if (otherFile == null) {
            return 1;
        }
        else {
            return -1;
        }
    }
}
