package com.ericgha.docuCloud.dto;

import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
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
public final class FileViewDto  extends FileDto implements Serializable, Comparable<FileViewDto> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID objectId;
    private final OffsetDateTime linkedAt;

    public FileViewDto(UUID fileId, String checksum, Long size, UUID userId, OffsetDateTime uploadedAt, UUID objectId, OffsetDateTime linkedAt) {
        super( fileId, checksum, size, userId, uploadedAt );
        this.objectId = objectId;
        this.linkedAt = linkedAt;
    }

    public FileViewRecord intoRecord() {
        return new FileViewRecord().setObjectId( objectId )
                .setFileId( super.getFileId() )
                .setUserId( super.getUserId() )
                .setUploadedAt( super.getUploadedAt() )
                .setLinkedAt( linkedAt )
                .setChecksum( super.getChecksum() )
                .setSize( super.getSize() );
    }

    public static FileViewDto fromRecord(@NonNull FileViewRecord fileViewRecord) {
        return fileViewRecord.into( FileViewDto.class);
    }

    private static final Comparator<FileViewDto> COMPARATOR =
            Comparator.comparing(FileViewDto::getObjectId)
                    .thenComparing(FileViewDto::getFileId  )
                    .thenComparing( FileViewDto::getUploadedAt )
                    .thenComparing( FileViewDto::getLinkedAt )
                    .thenComparing(FileViewDto::getChecksum)
                    .thenComparing(FileViewDto::getSize);
    public int compareTo(@NonNull FileViewDto other) {
        return COMPARATOR.compare( this, other );
    }
}
