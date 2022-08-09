package com.ericgha.docuCloud.dto;


import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.jooq.postgres.extensions.types.Ltree;

import java.io.Serial;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.UUID;

@RequiredArgsConstructor
@Builder
@Getter
@EqualsAndHashCode
@ToString
public final class TreeDTO implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID objectId;
    private final ObjectType objectType;
    private final Ltree path;
    private final UUID userId;
    private final OffsetDateTime createdAt;

    public TreeRecord intoTreeRecord() {
        return new TreeRecord().setObjectId( objectId )
                .setObjectType( objectType )
                .setPath(path)
                .setUserId( userId )
                .setCreatedAt( createdAt );
    }

    public String getPathStr() {
        return path.data();
    }
}
