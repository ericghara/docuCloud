package com.ericgha.docuCloud.dto;


import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.jooq.postgres.extensions.types.Ltree;

import java.io.Serial;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;

@RequiredArgsConstructor
@Builder
@Getter
@EqualsAndHashCode
@ToString
public final class TreeDto implements Serializable, Comparable<TreeDto> {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID objectId;
    private final ObjectType objectType;
    private final Ltree path;
    private final UUID userId;
    private final OffsetDateTime createdAt;

    public TreeRecord intoRecord() {
        return new TreeRecord().setObjectId( objectId )
                .setObjectType( objectType )
                .setPath(path)
                .setUserId( userId )
                .setCreatedAt( createdAt );
    }

    public static TreeDto fromRecord(@NonNull TreeRecord treeRecord) {
        return treeRecord.into( TreeDto.class );
    }

    public String getPathStr() {
        if (Objects.isNull(this.path) ) {
            return null;
        }
        return path.data();
    }

    private static final Comparator<TreeDto> COMPARATOR = Comparator.comparing( TreeDto::getObjectId )
            .thenComparing(TreeDto::getObjectType)
            // jOOQ's Ltree class doesn't implement comparable...
            .thenComparing(TreeDto::getPathStr)
            .thenComparing( TreeDto::getUserId )
            .thenComparing( TreeDto::getCreatedAt );

    public int compareTo(@NonNull TreeDto other) {
        return COMPARATOR.compare(this, other);
    }
}
