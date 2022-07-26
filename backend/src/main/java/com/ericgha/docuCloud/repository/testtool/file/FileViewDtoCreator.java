package com.ericgha.docuCloud.repository.testtool.file;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/**
 * Creates "Fake" FILE_VIEW objects which link a list of file objects to a list of files via tree_join_file table (ie
 * create a full FILE_VIEW record) file_id fields are auto generated by this, while uploaded_at and linked_at fields are
 * left null to be generated by the database.  Checksum is FILE# where # is the index of the Object being linked in the
 * input array.  Ie OBJECT[0] -> file0, therefore file object relationships can be tracked by checksum.  Size is the
 * index in the array ie Object[0] -> 0.
 */
public class FileViewDtoCreator {

    private FileViewDtoCreator() throws IllegalAccessException {
        throw new IllegalAccessException( "Utility class, do not instantiate." );
    }

    private static final String CHECKSUM_PREFIX = "file";

    public static List<FileViewDto> create(List<TreeDto> treeDtos, CloudUser cloudUser) {
        List<FileViewDto> fvRecords = new ArrayList<>();
        for (int i = 0; i < treeDtos.size(); i++) {
            var tr = treeDtos.get( i );
            fvRecords.add( create( treeDtos.get( i ),
                    cloudUser, i ) );
        }
        fvRecords.sort(Comparator.naturalOrder() );
        return fvRecords;
    }

    // Index is used to set checksum and size values.  See class description
    public static FileViewDto create(@NonNull TreeDto treeDto, @NonNull CloudUser cloudUser, long index) {
        return FileViewDto.builder()
                .objectId( treeDto.getObjectId() )
                .fileId( UUID.randomUUID() )
                .userId( cloudUser.getUserId() )
                .checksum( CHECKSUM_PREFIX + index )
                .size( index )
                .build();
    }

}
