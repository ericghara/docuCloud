package com.ericgha.docuCloud.repository.testtool.assertion;

import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.repository.testtool.file.ObjectResourceAdjacencyParser;
import com.ericgha.docuCloud.repository.testtool.file.ObjectResourceAdjacencyParser.ObjectResourceAdjacency;
import com.ericgha.docuCloud.repository.testtool.file.TestFiles;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ericgha.docuCloud.repository.testtool.assertion.CollectionAssertion.*;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class TestFileAssertion {

    // Simply tests if treeObjwith specified path and fileResource with specified checksum
    // are linked and have same user_id.  No comparisons are made to the tracked TestFiles
    // state (ie file_id, timestamps, size)
    // Any other adjacency found will cause the test to fail
    public static void assertRepositoryState(TestFiles testFiles, String adjacencyCsv) {
        Iterable<ObjectResourceAdjacency> expectedAdjacencies = ObjectResourceAdjacencyParser.parse( adjacencyCsv )
                .collect( Collectors.toCollection( TreeSet::new ) );
        Iterable<ObjectResourceAdjacency> foundAdjacencies = testFilesToAdjacencyStream( testFiles )
                .collect( Collectors.toCollection( TreeSet::new ) );
        assertIterableEquals( expectedAdjacencies, foundAdjacencies );
    }

    // Similar to assertRepositoryState but additional adjacencies will not cause
    // a test failure.  Similar to Collection#containsAll(), something like
    // assertTrue(repository.containsAll(adjacencies) )
    public static void assertContains(TestFiles testFiles, String adjacencyCsv) {
        NavigableSet<ObjectResourceAdjacency> wantedAdjacencies = ObjectResourceAdjacencyParser.parse( adjacencyCsv )
                .collect( Collectors.toCollection( TreeSet::new ) );
        NavigableSet<ObjectResourceAdjacency> allAdjacencies = testFilesToAdjacencyStream( testFiles )
                .collect( Collectors.toCollection( TreeSet::new ) );
        assertCollectionContainsAll( allAdjacencies, wantedAdjacencies );
    }

    // More rigorous test than assertRepositoryState & assertLinked,
    // No user records whatsoever may differ from the tracked state
    public static void assertNoChanges(TestFiles testFiles) {
        // convert these to NavigableMap<String,NavigableSet<FileViewDto>>
        NavigableMap<String, NavigableSet<FileViewDto>> found = testFiles.fetchFileViewDtosGroupedByObjectPathStr();
        NavigableMap<String, NavigableSet<FileViewDto>> expected = testFiles.getOrigFileViewsGroupedByPathStr();
        assertCollectionMapsEqual( expected, found );
    }

    // Similar to assertNoChanges except only asserts no changes for objects
    // and resources specified in the adjacency csv.
    public static void assertNoChangesFor(TestFiles testFiles, String adjacencyCsv) {
        NavigableMap<String, NavigableSet<FileViewDto>> all = testFiles.getOrigFileViewsGroupedByPathStr();
        Stream<ObjectResourceAdjacency> adjacencyStream = ObjectResourceAdjacencyParser.parse( adjacencyCsv );
        NavigableMap<String, NavigableSet<FileViewDto>> wanted;
        try {
            wanted = testFiles.fetchFileViewDtosGroupedByObjectPathStr( adjacencyStream );
        } catch (IllegalArgumentException e) {
            throw new AssertionError("A wanted file untracked by this instance was provided");
        }
        assertCollectionMapContainsAll( all, wanted );
    }

    private static NavigableMap<String, NavigableSet<String>> adjacencyStreamToMap(
            Stream<ObjectResourceAdjacency> adjacencies) {
        return adjacencies.collect(
                Collectors.groupingBy( ObjectResourceAdjacency::fileChecksum,
                        TreeMap::new,
                        Collectors.mapping( ObjectResourceAdjacency::treePath,
                                Collectors.toCollection( TreeSet::new ) ) ) );
    }

    private static Stream<ObjectResourceAdjacency> testFilesToAdjacencyStream(TestFiles testFiles) {
        return testFiles.fetchFileViewDtosGroupedByObjectPathStr()
                .entrySet()
                .stream()
                .flatMap( entry -> {
                    String pathStr = entry.getKey();
                    return entry.getValue().stream()
                            .map( FileViewDto::getChecksum )
                            .map( checksum -> new ObjectResourceAdjacency( pathStr, checksum ) );
                } );
    }
}
