package com.ericgha.docuCloud.converter;

import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeJoinFileDto;
import lombok.NonNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

@Component
public class FileViewDtoToTreeJoinFileDto implements Converter<FileViewDto, TreeJoinFileDto> {

    @Override
    // Throws if any tree_join_file field is null
    public TreeJoinFileDto convert(@NonNull FileViewDto source) {
        UUID objectId = Objects.requireNonNull(source.getObjectId(), "objectId was null");
        UUID fileId = Objects.requireNonNull(source.getFileId(), "fileId was null");
        OffsetDateTime linkedAt = Objects.requireNonNull( source.getLinkedAt(), "linkedAt was null" );
        return TreeJoinFileDto.builder()
                .objectId( objectId )
                .fileId( fileId )
                .linkedAt( linkedAt )
                .build();
    }
}
