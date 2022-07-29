package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.converter.LtreeFormatter;
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
    private final LtreeFormatter ltreeFormatter;

    private final CsvParser csvParser = new CsvParser();

    public TestFileTree(String userId, TreeTestQueries testQueries) {
        this.userId = userId;
        this.testQueries = testQueries;
        this.recordByPath = new HashMap<>();
        this.ltreeFormatter =  new LtreeFormatter();
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
                .doOnNext(record -> recordByPath.put(record.getPath(), record) )
                .block();
    }
    public TreeRecord add(ObjectType objectType, String pathStr) {
        return add(ObjectType.ROOT, Ltree.valueOf( pathStr ) );
    }

    public List<TreeRecord> addFromCsv(String csv) {
        return csvParser.parse( csv )
                .map( csvRecord -> this.add( csvRecord.objectType(), csvRecord.path() ) )
                .peek( treeRecord -> Objects.requireNonNull(treeRecord.getObjectId(), "Insert error" ) )
                .peek( treeRecord -> recordByPath.put(treeRecord.getPath(), treeRecord) )
                .toList();
    }

    class CsvParser {
        record CsvRecord(ObjectType objectType, Ltree path) {}

        Stream<CsvRecord> parse(String csv) {
            return csv.lines().map(this::splitLine )
                    .filter( split -> split.length == 2 )
                    .map(this::toCsvRecord);
        }

        // returns String[0] for comments else String[2]
        private String[] splitLine(String line) {
            if (line.matches("^\s*#") ) {
                // a comment;
                return new String[0];
            }
            String[] split = line.strip()
                    .split("\s*,\s*");
            if (split.length != 2) {
                throw new IllegalArgumentException( "Unable to parse line: " + line );
            }
            return split;
        }

        private CsvRecord toCsvRecord(String[] split) {
            ObjectType objectType = ObjectType.valueOf(split[0].toUpperCase() );
            try {
                return new CsvRecord(objectType, ltreeFormatter.parse( split[1], null ) );
            } catch (Exception e) {
                throw new IllegalArgumentException( e );
            }
        }
    }
}
