package com.ericgha.docuCloud.converter;

import com.ericgha.docuCloud.jooq.tables.records.FileRecord;
import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import lombok.NonNull;
import org.springframework.core.convert.converter.Converter;

public class FileViewRecordToFileRecord implements Converter<FileViewRecord, FileRecord> {

    @Override
    public FileRecord convert(@NonNull FileViewRecord source) {
        return new FileRecord().setFileId( source.getFileId() )
                .setChecksum( source.getChecksum() )
                .setSize( source.getSize() )
                .setUploadedAt( source.getUploadedAt() );
    }
}
