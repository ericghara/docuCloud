package com.ericgha.docuCloud.service.testutil.assertion;

import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import com.ericgha.docuCloud.service.testutil.TestFileTree;
import com.ericgha.docuCloud.util.comparators.TreeRecordComparators;
import org.jooq.postgres.extensions.types.Ltree;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;


 /** A collection of assertions to verify specificity of mutation operations
  * on a {@link TestFileTree} and therefore the database.  Methods are designed
  * to test that mutation <em>did not</em> occur to certain database records.
 **/
public class TestFileTreeAssertions {

    static public void assertNoChanges(TestFileTree tree) {
        List<TreeRecord> found = tree.fetchAllUserObjects( TreeRecordComparators::compareByObjectId);
        List<TreeRecord> expected = tree.getTrackedObjects(TreeRecordComparators::compareByObjectId);
        assertIterableEquals( found, expected );
    }

    static public void assertNoChangesFor(TestFileTree tree, Ltree... paths) {
        Map<Ltree, TreeRecord> curObjects = tree.fetchAllUserObjects()
                .stream()
                .collect( Collectors.toMap(
                        TreeRecord::getPath,
                        record -> record ) );
        Stream.of( paths )
                .forEach( p -> assertEquals( tree.getOrigRecord( p ), curObjects.get( p ),
                        "Records didn't match for path " + p ) );
    }

    static public void assertNoChangesFor(TestFileTree tree, String... pathStrs) {
        Ltree[] paths = Stream.of( pathStrs )
                .map( Ltree::valueOf ).toArray( Ltree[]::new );
        assertNoChangesFor( tree, paths );
    }



}
