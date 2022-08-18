package com.ericgha.docuCloud.converter;

import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import lombok.NonNull;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
public class FileViewDtoToFileDto implements Converter<FileViewDto, FileDto> {

    @Override
    public FileDto convert(@NonNull FileViewDto source) {
        return FileDto.builder().fileId( source.getFileId() )
                .checksum( source.getChecksum() )
                .size( source.getSize() )
                .userId( source.getUserId() )
                .uploadedAt( source.getUploadedAt() )
                .build();
    }
}
