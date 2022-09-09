package com.ericgha.docuCloud.dto;

import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;

import java.io.Serial;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.UUID;

@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public final class FileViewDto extends FileDto implements Serializable, Comparable<FileDto> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID objectId;
    private final OffsetDateTime linkedAt;

    public FileViewDto(UUID objectId, UUID fileId, UUID userId, OffsetDateTime uploadedAt, OffsetDateTime linkedAt, String checksum, Long size) {
        super( fileId, checksum, size, userId, uploadedAt );
        this.objectId = objectId;
        this.linkedAt = linkedAt;
    }

    public FileViewRecord intoFileViewRecord() {
        return new FileViewRecord().setObjectId( objectId )
                .setFileId( super.getFileId() )
                .setUserId( super.getUserId() )
                .setUploadedAt( super.getUploadedAt() )
                .setLinkedAt( linkedAt )
                .setChecksum( super.getChecksum() )
                .setSize( super.getSize() );
    }


    public static FileViewDto fromRecord(@NonNull FileViewRecord fileViewRecord) {
        return fileViewRecord.into( FileViewDto.class );
    }

    private static final Comparator<FileViewDto> COMPARATOR =
            Comparator.comparing( FileViewDto::getObjectId )
                    .thenComparing( FileViewDto::getFileId )
                    .thenComparing( FileViewDto::getUploadedAt )
                    .thenComparing( FileViewDto::getLinkedAt )
                    .thenComparing( FileViewDto::getChecksum )
                    .thenComparing( FileViewDto::getSize );

    @Override
    public int compareTo(@NonNull FileDto other) {
        if (other instanceof FileViewDto otherFvr) {
            return COMPARATOR.compare( this, otherFvr );
        }
        return super.compareTo( other );
    }

    public static FileViewDtoBuilder builder() {
        return new FileViewDtoBuilder();
    }

    @Accessors(fluent = true)
    public static class FileViewDtoBuilder extends FileDtoBuilder {

        // Not a fan of Lombok's @SuperBuilder

        @Getter
        private UUID objectId;

        @Getter
        private OffsetDateTime linkedAt;

        private FileViewDtoBuilder() {
            super();
        }

        public FileViewDto build() {
            return new FileViewDto( this.objectId, this.fileId(), this.userId(),
                    this.uploadedAt(), this.linkedAt, this.checksum(), this.size() );
        }

        public FileViewDtoBuilder objectId(UUID ObjectId) {
            this.objectId = ObjectId;
            return this;
        }

        @Override
        public FileViewDtoBuilder fileId(UUID fileId) {
            super.fileId( fileId );
            return this;
        }

        @Override
        public FileViewDtoBuilder userId(UUID userId) {
            super.userId( userId );
            return this;
        }

        @Override
        public FileViewDtoBuilder uploadedAt(OffsetDateTime uploadedAt) {
            super.uploadedAt( uploadedAt );
            return this;
        }

        public FileViewDtoBuilder linkedAt(OffsetDateTime linkedAt) {
            this.linkedAt = linkedAt;
            return this;
        }

        @Override
        public FileViewDtoBuilder checksum(String checksum) {
            super.checksum( checksum );
            return this;
        }

        @Override
        public FileViewDtoBuilder size(Long size) {
            super.size( size );
            return this;
        }

        @Override
        public String toString() {
            return String.format(  "objectId=%1$s, fileId=%2$s, userId=%3$s, uploadedAt=%4$s, " +
                            "linkedAt=%5$s, checksum=%6$s, size=%7$s",  this.objectId, this.fileId(), this.userId(),
                    this.uploadedAt(), this.linkedAt, this.checksum(), this.size() );
        }
    }
}
