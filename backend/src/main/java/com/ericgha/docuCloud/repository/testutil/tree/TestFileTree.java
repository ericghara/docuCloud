package com.ericgha.docuCloud.repository.testutil.tree;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.repository.TreeRepository;
import lombok.Getter;
import lombok.NonNull;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A class and collection of methods useful for testing {@link TreeRepository} queries.
 * Test user's file trees can be easily created with stored state
 * representing the expected state of the tree in absence of mutating operations.  Additionally,
 * methods to fetch the current state of the tree are provided.  While queries are performed
 * referencing the {@code Ltree} or path {@code String} this is for convenience, the {@code ObjectId}
 * is used internally to connect original and current objects.
 **/
public class TestFileTree {

    private final Map<Ltree, TreeDto> dtoByPath;
    @Getter
    private final UUID userId;
    private final TreeTestQueries testQueries;

    private final TestFileTreeCsvParser testFileTreeCsvParser = new TestFileTreeCsvParser();


    TestFileTree(@NonNull UUID userId, @NonNull TreeTestQueries testQueries) {
        this.userId = userId;
        this.testQueries = testQueries;
        this.dtoByPath = new HashMap<>();
    }

    public TestFileTree(CloudUser user, TreeTestQueries testQueries) {
        this( user.getUserId(), testQueries);
    }

    @Nullable
    public TreeDto getOrigRecord(Ltree path) throws IllegalArgumentException {
        TreeDto dto = dtoByPath.get( path );
        if (Objects.isNull( dto ) ) {
            throw new IllegalArgumentException("Instance has not created an instance with specified path");
        }
        // copy doesn't copy objectId...
        return dto;
    }

    @Nullable
    public TreeDto getOrigRecord(@NonNull String pathStr) {
        var record = getOrigRecord( Ltree.valueOf( pathStr ) );
        if (Objects.isNull(record)) {
            throw new IllegalArgumentException("No object with specified path was created by this instance.");
        }
        return record;
    }

    @Nullable
    public TreeDto fetchCurRecord(UUID objectId) {
        return testQueries.getByObjectId( objectId );
    }
    // Parses objectId from tree dto, ignores all other info

    public TreeDto fetchCurRecord(TreeDto origRecord) {
        if (Objects.isNull( origRecord.getObjectId() ) ) {
            throw new IllegalArgumentException("Received a TreeDto with a null objectId");
        }
        return fetchCurRecord(origRecord.getObjectId() );
    }

    public List<TreeDto> fetchAllUserObjects( ) {
        List<TreeDto> dtos = testQueries.getAllUserObjects(userId);
        dtos.sort(Comparator.naturalOrder());
        return dtos;
    }

    public TreeDto fetchByObjectPath(@NonNull String pathStr) {
        return testQueries.getByObjectPath( pathStr, userId );
    }

    // returns objects in unspecified order;

    public List<TreeDto> getTrackedObjectsOfType(ObjectType objectType) {
        return this.getTrackedObjects().stream()
                .filter(rec -> rec.getObjectType() == objectType)
                .toList();
    }

    // returns objects in order specified by comparator
    public List<TreeDto> getTrackedObjects() {
        List<TreeDto> dtos = new ArrayList<>( this.dtoByPath.values() );
        dtos.sort(Comparator.naturalOrder());
        return dtos;
    }

    public TreeDto add(@NonNull ObjectType objectType, @NonNull Ltree path) {
        return testQueries.create(objectType, path, userId)
                .doOnNext(dto -> {
                    if (!Objects.isNull(dtoByPath.put(dto.getPath(), dto) ) )  {
                    throw new IllegalStateException("Duplicate insert");
                }})
                .block();
    }
    public TreeDto add(ObjectType objectType, @NonNull String pathStr) {
        return add(objectType, Ltree.valueOf( pathStr ) );
    }

    public List<TreeDto> addFromCsv(String csv) {
        return testFileTreeCsvParser.parse( csv )
                .map( csvRecord -> this.add( csvRecord.objectType(), csvRecord.path() ) )
                .toList();
    }

}
