package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import org.jooq.postgres.extensions.types.Ltree;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

class TestFileTree {

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

    public TreeRecord getRecordByPath(Ltree path) {
        return recordByPath.get( path );
    }

    public TreeRecord getRecordByPath(String pathStr) {
        return getRecordByPath( Ltree.valueOf( pathStr ) );
    }

    public void assertNoChanges() {
        List<TreeRecord> found = testQueries.getAllUserObjects(userId);
        List<TreeRecord> expected = recordByPath.values()
                .stream().sorted( Comparator.comparing( TreeRecord::getObjectId ) ).toList();
        assertIterableEquals( found, expected );
    }

    public void assertNoChangesFor(Ltree... paths) {
        Map<Ltree, TreeRecord> curObjects = testQueries.getAllUserObjects( userId )
                .stream()
                .collect( Collectors.toMap(
                        TreeRecord::getPath,
                        record -> record ) );
        Stream.of( paths )
                .forEach( p -> assertEquals( recordByPath.get( p ), curObjects.get( p ),
                        "Records didn't match for path " + p ) );
    }

    public void assertNoChangesFor(String... pathStrs) {
        assertNoChangesFor( Stream.of( pathStrs )
                .map( Ltree::valueOf ).toArray( Ltree[]::new ) );
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
