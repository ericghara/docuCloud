package com.ericgha.docuCloud.dto;

import com.ericgha.docuCloud.jooq.tables.records.FileRecord;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.io.Serial;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.UUID;

@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class FileDto implements Serializable, Comparable<FileDto> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID fileId;
    private final String checksum;
    private final Long size;

    private final UUID userId;
    private final OffsetDateTime uploadedAt;

    public static FileDtoBuilder builder() {
        return new FileDtoBuilder();
    }

    public FileRecord intoFileRecord() {
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

    @Getter
    @Accessors(fluent = true)
    public static class FileDtoBuilder {
        private UUID fileId;
        private String checksum;
        private Long size;
        private UUID userId;
        private OffsetDateTime uploadedAt;

        FileDtoBuilder() {
        }

        public FileDtoBuilder fileId(UUID fileId) {
            this.fileId = fileId;
            return this;
        }

        public FileDtoBuilder checksum(String checksum) {
            this.checksum = checksum;
            return this;
        }

        public FileDtoBuilder size(Long size) {
            this.size = size;
            return this;
        }

        public FileDtoBuilder userId(UUID userId) {
            this.userId = userId;
            return this;
        }

        public FileDtoBuilder uploadedAt(OffsetDateTime uploadedAt) {
            this.uploadedAt = uploadedAt;
            return this;
        }

        public FileDto build() {
            return new FileDto( fileId, checksum, size, userId, uploadedAt );
        }

        public String toString() {
            return "FileDto.FileDtoBuilder(fileId=" + this.fileId + ", checksum=" + this.checksum + ", size=" + this.size + ", userId=" + this.userId + ", uploadedAt=" + this.uploadedAt + ")";
        }
    }
}
