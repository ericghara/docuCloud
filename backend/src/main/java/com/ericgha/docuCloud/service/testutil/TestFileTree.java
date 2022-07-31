package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import com.ericgha.docuCloud.service.TreeService;
import lombok.NonNull;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A class and collection of methods useful for testing {@link TreeService} queries.
 * Test user's file trees can be easily created with stored state
 * representing the expected state of the tree in absence of mutating operations.  Additionally,
 * methods to fetch the current state of the tree are provided.  While queries are performed
 * referencing the {@code Ltree} or path {@code String} this is for convenience, the {@code ObjectId}
 * is used internally to connect original and current objects.
 **/
public class TestFileTree {

    private final Map<Ltree, TreeRecord> recordByPath;
    private final String userId;
    private final TreeTestQueries testQueries;

    private final TestFileTreeCsvParser testFileTreeCsvParser = new TestFileTreeCsvParser();


    public TestFileTree(@NonNull String userId, @NonNull TreeTestQueries testQueries) {
        this.userId = userId;
        this.testQueries = testQueries;
        this.recordByPath = new HashMap<>();
    }

    public TestFileTree(CloudUser user, TreeTestQueries testQueries) {
        this( user.getUserId(), testQueries);
    }

    @Nullable
    public TreeRecord getOrigRecord(Ltree path) {
        var orig = recordByPath.get( path );
        if (Objects.isNull( orig ) ) {
            return null;
        }
        // copy doesn't copy objectId...
        var copy = orig.copy();
        copy.setObjectId( orig.getObjectId() );
        return copy;
    }

    @Nullable
    public TreeRecord getOrigRecord(@NonNull String pathStr) {
        return getOrigRecord( Ltree.valueOf( pathStr ) );
    }

    @Nullable
    public TreeRecord fetchCurRecord(String objectId) {
        return testQueries.getByObjectId( objectId );
    }
    // Parses objectId from tree record, ignores all other info
    public TreeRecord fetchCurRecord(TreeRecord origRecord) {
        if (Objects.isNull( origRecord.getObjectId() ) ) {
            throw new IllegalArgumentException("Received a TreeRecord with a null objectId");
        }
        return fetchCurRecord(origRecord.getObjectId() );
    }

    public List<TreeRecord> fetchAllUserObjects() {
        return testQueries.getAllUserObjects(userId);
    }

    public List<TreeRecord> fetchAllUserObjects(@NonNull Comparator<TreeRecord> comparator) {
        List<TreeRecord> records = testQueries.getAllUserObjects(userId);
        records.sort(comparator);
        return records;
    }

    // returns objects in unspecified order;
    public List<TreeRecord> getTrackedObjects() {
        return this.recordByPath.values().stream()
                .map( r -> {
                    var copy = r.copy();
                    copy.setObjectId( r.getObjectId() );
                    return copy;
                } ).collect( Collectors.toCollection( ArrayList::new ) );
    }

    // returns objects in order specified by comparator
    public List<TreeRecord> getTrackedObjects(@NonNull Comparator<TreeRecord> comparator) {
        List<TreeRecord> records = this.getTrackedObjects();
        records.sort(comparator);
        return records;
    }

    public TreeRecord add(@NonNull ObjectType objectType, @NonNull Ltree path) {
        return testQueries.create(objectType, path, userId)
                .doOnNext(record -> {
                    if (!Objects.isNull(recordByPath.put(record.getPath(), record) ) )  {
                    throw new IllegalStateException("Duplicate insert");
                }})
                .block();
    }
    public TreeRecord add(ObjectType objectType, @NonNull String pathStr) {
        return add(objectType, Ltree.valueOf( pathStr ) );
    }

    public List<TreeRecord> addFromCsv(String csv) {
        return testFileTreeCsvParser.parse( csv )
                .map( csvRecord -> this.add( csvRecord.objectType(), csvRecord.path() ) )
                .toList();
    }

}
