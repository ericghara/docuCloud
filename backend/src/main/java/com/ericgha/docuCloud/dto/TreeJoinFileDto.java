package com.ericgha.docuCloud.dto;

import com.ericgha.docuCloud.jooq.tables.records.TreeJoinFileRecord;
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
public class TreeJoinFileDto implements Serializable, Comparable<TreeJoinFileDto> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID objectId;
    private final UUID fileId;
    private final OffsetDateTime linkedAt;

    public static TreeJoinFileDto fromRecord(@NonNull TreeJoinFileRecord record) {
        return record.into(TreeJoinFileDto.class);
    }

    public TreeJoinFileRecord intoRecord() {
        return new TreeJoinFileRecord()
                .setObjectId( objectId )
                .setFileId( fileId )
                .setLinkedAt( linkedAt );
    }

    private static final Comparator<TreeJoinFileDto> COMPARATOR = Comparator.comparing( TreeJoinFileDto::getObjectId )
            .thenComparing( TreeJoinFileDto::getFileId )
            .thenComparing( TreeJoinFileDto::getLinkedAt );

    public int compareTo(@NonNull TreeJoinFileDto other ) {
        return COMPARATOR.compare( this, other );
    }

}
