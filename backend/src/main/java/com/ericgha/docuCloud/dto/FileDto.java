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
import java.util.Comparator;
import java.util.UUID;

@RequiredArgsConstructor
@Builder
@Getter
@EqualsAndHashCode
@ToString
public final class FileDto implements Serializable, Comparable<FileDto> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID fileId;
    private final String checksum;
    private final Long size;

    private final UUID userId;
    private final OffsetDateTime uploadedAt;

    public FileRecord intoRecord() {
        return new FileRecord().setFileId( fileId )
                .setChecksum( checksum )
                .setSize( size )
                .setUserId( userId )
                .setUploadedAt( uploadedAt );
    }

    public static FileDto fromRecord(@NonNull FileRecord record) {
        return record.into( FileDto.class );
    }

    private static final Comparator<FileDto> COMPARATOR =
            Comparator.comparing( FileDto::getFileId )
                    .thenComparing( FileDto::getChecksum )
                    .thenComparing( FileDto::getSize )
                    .thenComparing( FileDto::getUserId )
                    .thenComparing( FileDto::getUploadedAt );

    public int compareTo(@NonNull FileDto other) {
        return COMPARATOR.compare( this, other );
    }
}
