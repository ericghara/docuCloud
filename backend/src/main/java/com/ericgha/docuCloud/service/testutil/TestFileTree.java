package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import org.jooq.postgres.extensions.types.Ltree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TestFileTree {

    private final Map<Ltree, TreeRecord> recordByPath;
    private final String userId;
    private final TreeTestQueries testQueries;

    private final TestFileTreeCsvParser testFileTreeCsvParser = new TestFileTreeCsvParser();

    public TestFileTree(String userId, TreeTestQueries testQueries) {
        this.userId = userId;
        this.testQueries = testQueries;
        this.recordByPath = new HashMap<>();
    }

    public TestFileTree(CloudUser user, TreeTestQueries testQueries) {
        this( user.getUserId(), testQueries);
    }

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

    public TreeRecord getOrigRecord(String pathStr) {
        return getOrigRecord( Ltree.valueOf( pathStr ) );
    }

    public TreeRecord fetchCurRecord(String objectId) {
        return testQueries.getByObjectId( objectId );
    }
    // Parses objectId from tree record, ignores all other info
    public TreeRecord fetchCurRecord(TreeRecord origRecord) {
        return fetchCurRecord(origRecord.getObjectId() );
    }

    public List<TreeRecord> fetchAllUserObjects() {
        return testQueries.getAllUserObjects(userId);
    }

    public List<TreeRecord> fetchAllUserObjects(Comparator<TreeRecord> comparator) {
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
    public List<TreeRecord> getTrackedObjects(Comparator<TreeRecord> comparator) {
        List<TreeRecord> records = this.getTrackedObjects();
        records.sort(comparator);
        return records;
    }

    public TreeRecord add(ObjectType objectType, Ltree path) {
        return testQueries.create(objectType, path, userId)
                .doOnNext(record -> {
                    if (!Objects.isNull(recordByPath.put(record.getPath(), record) ) )  {
                    throw new IllegalStateException("Duplicate insert");
                }})
                .block();
    }
    public TreeRecord add(ObjectType objectType, String pathStr) {
        return add(objectType, Ltree.valueOf( pathStr ) );
    }

    public List<TreeRecord> addFromCsv(String csv) {
        return testFileTreeCsvParser.parse( csv )
                .map( csvRecord -> this.add( csvRecord.objectType(), csvRecord.path() ) )
                .toList();
    }

}
