package com.ericgha.docuCloud.converter;

import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import com.ericgha.docuCloud.jooq.tables.records.TreeJoinFileRecord;
import lombok.NonNull;
import org.springframework.core.convert.converter.Converter;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

public class FileViewRecordToTreeJoinFileRecord implements Converter<FileViewRecord, TreeJoinFileRecord> {

    @Override
    // Throws if any tree_join_file field is null
    public TreeJoinFileRecord convert(@NonNull FileViewRecord source) {
        UUID objectId = Objects.requireNonNull(source.getObjectId(), "objectId was null");
        UUID fileId = Objects.requireNonNull(source.getFileId(), "fileId was null");
        OffsetDateTime linkedAt = Objects.requireNonNull( source.getLinkedAt(), "linkedAt was null" );
        return new TreeJoinFileRecord().setObjectId( objectId )
                .setFileId( fileId )
                .setLinkedAt( linkedAt );
    }
}
