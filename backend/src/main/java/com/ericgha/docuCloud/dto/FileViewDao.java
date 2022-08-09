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
import java.util.UUID;

@RequiredArgsConstructor
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class FileViewDao implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID objectId;
    private final UUID fileId;
    private final UUID userId;
    private final OffsetDateTime uploadedAt;
    private final OffsetDateTime linkedAt;
    private final String checksum;
    private final Long size;

    public FileViewRecord intoRecord() {
        return new FileViewRecord().setObjectId( objectId )
                .setFileId( fileId )
                .setUserId( userId )
                .setUploadedAt( uploadedAt )
                .setLinkedAt( linkedAt )
                .setChecksum( checksum )
                .setSize( size );
    }

    static FileViewDao fromRecord(@NonNull FileViewRecord fileViewRecord) {
        return fileViewRecord.into(FileViewDao.class);
    }

}
