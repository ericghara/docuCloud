package com.ericgha.docuCloud.repository.testtool.assertion;

import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.repository.testtool.tree.TestFileTree;
import org.jooq.postgres.extensions.types.Ltree;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;


 /** A collection of assertions to verify specificity of mutation operations
  * on a {@link TestFileTree} and therefore the database.  Methods are designed
  * to test that mutation <em>did not</em> occur to certain database dtos.
 **/
public class TestFileTreeAssertion {

    static public void assertNoChanges(TestFileTree tree) {
        List<TreeDto> found = tree.fetchAllUserObjects( );
        List<TreeDto> expected = tree.getTrackedObjects( );
        assertIterableEquals( found, expected );
    }

    static public void assertNoChangesFor(TestFileTree tree, Ltree... paths) {
        Map<Ltree, TreeDto> curObjects = tree.fetchAllUserObjects()
                .stream()
                .collect( Collectors.toMap(
                        TreeDto::getPath,
                        dto -> dto ) );
        Stream.of( paths )
                .forEach( p -> assertEquals( getOrigRecordOrThrow( p, tree ), curObjects.get( p ),
                        "Records didn't match for path " + p ) );
    }

    static public void assertNoChangesFor(TestFileTree tree, String... pathStrs) {
        Ltree[] paths = Stream.of( pathStrs )
                .map( Ltree::valueOf ).toArray( Ltree[]::new );
        assertNoChangesFor( tree, paths );
    }

    private static TreeDto getOrigRecordOrThrow(Ltree path, TestFileTree tree) throws AssertionError {
        try {
            return tree.getOrigRecord( path );
        } catch (IllegalArgumentException e) {
            throw new AssertionError(String.format("Path %s could not be located", path), e);
        }
    }
}
