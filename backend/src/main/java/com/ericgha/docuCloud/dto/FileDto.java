package com.ericgha.docuCloud.dto;

import com.ericgha.docuCloud.jooq.tables.records.FileRecord;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.UUID;

@RequiredArgsConstructor
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class FileDto implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID fileId;
    private final String checksum;
    private final Long size;
    private final OffsetDateTime uploadedAt;

    public FileRecord intoRecord() {
        return new FileRecord().setFileId( fileId )
                .setChecksum( checksum )
                .setSize( size )
                .setUploadedAt( uploadedAt );
    }

    public static FileDto fromRecord(@NonNull FileRecord record) {
        return record.into(FileDto.class);
    }
}
