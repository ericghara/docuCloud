package com.ericgha.docuCloud.repository.testtool.assertion;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CollectionAssertionTest {

    @Test
    @DisplayName( "assertCollectionContainsAll should pass" )
    void assertCollectionContainsAllTestPass() {
        var available = Set.of("a", "b", "c");
        var wanted = Set.of("b","c");
        assertDoesNotThrow( () -> CollectionAssertion.assertCollectionContainsAll( available, wanted ) );
    }

    @Test
    @DisplayName( "assertCollectionContainsAll should pass when both null" )
    void assertCollectionContainsAllBothNullPasses() {
        assertDoesNotThrow( () -> CollectionAssertion.assertCollectionContainsAll( null, null ) );
    }

    @Test
    @DisplayName( "assertCollectionContainsAll should fail" )
    void assertCollectionContainsAllTestFail() {
        var available = Set.of("b", "c");
        var wanted = Set.of("a", "b","c");
        assertThrows(AssertionError.class, () -> CollectionAssertion.assertCollectionContainsAll( available, wanted ) );
    }

    @Test
    @DisplayName( "assertCollectionContainsAll fails one null" )
    void assertCollectionContainsAllFailsOneNull () {
        var available = Set.of();
        Set<Object> wanted = null;
        assertThrows(AssertionError.class, () -> CollectionAssertion.assertCollectionContainsAll( available, wanted ) );
    }

    @Test
    @DisplayName( "assetCollectionMapsEqual does not throw when identical maps" )
    void assertCollectionMapsEqualDoesNotThrowWhenEqual() {
        Map<Integer, Set<Character>> expected = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd') );
        Map<Integer, Set<Character>> found = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd') );
        assertDoesNotThrow( () -> CollectionAssertion.assertCollectionMapsEqual(expected, found) );
    }

    @Test
    @DisplayName( "assetCollectionMapsEqual throws when expected has an extra key" )
    void assertCollectionMapsEqualExpectedHasExtraKey() {
        Map<Integer, Set<Character>> expected = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd') );
        Map<Integer, Set<Character>> found = Map.of(0, Set.of('a', 'b') );
        assertThrows(AssertionError.class, () -> CollectionAssertion.assertCollectionMapsEqual(expected, found) );
    }

    @Test
    @DisplayName( "assetCollectionMapsEqual throws when found has an extra value" )
    void assertCollectionMapsEqualFoundHasExtraKey() {
        Map<Integer, Set<Character>> expected = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd') );
        Map<Integer, Set<Character>> found = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd','e') );
        assertThrows(AssertionError.class, () -> CollectionAssertion.assertCollectionMapsEqual(expected, found) );
    }

    @Test
    @DisplayName( "assertCollectionMapContainsAll passes when available has extra key" )
    void assertCollectionMapContainsPassesAvailableExtraKey() {
        Map<Integer, Set<Character>> available = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd') );
        Map<Integer, Set<Character>> wanted = Map.of(0, Set.of('a', 'b') );
        assertDoesNotThrow( () -> CollectionAssertion.assertCollectionMapContainsAll(available, wanted) );
    }

    @Test
    @DisplayName( "assertCollectionMapContainsAll passes when available has extra value" )
    void assertCollectionMapContainsAllPassesAvailableExtraValue() {
        Map<Integer, Set<Character>> available = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd','e') );
        Map<Integer, Set<Character>> wanted = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd') );
        assertDoesNotThrow( () -> CollectionAssertion.assertCollectionMapContainsAll(available, wanted) );
    }

    @Test
    @DisplayName( "assertCollectionMapContainsAll fails when wanted has extra key" )
    void assertCollectionMapContainsFailsWantedExtraKey() {
        Map<Integer, Set<Character>> wanted = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd') );
        Map<Integer, Set<Character>> available = Map.of(0, Set.of('a', 'b') );
        assertThrows(AssertionError.class, () -> CollectionAssertion.assertCollectionMapContainsAll(available, wanted) );
    }

    @Test
    @DisplayName( "assertCollectionMapContainsAll fails when wanted has extra value" )
    void assertCollectionMapContainsAllFailsWantedExtraValue() {
        Map<Integer, Set<Character>> available = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd') );
        Map<Integer, Set<Character>> wanted = Map.of(0, Set.of('a', 'b'), 1, Set.of('c', 'd','e') );
        assertThrows(AssertionError.class, () -> CollectionAssertion.assertCollectionMapContainsAll(available, wanted) );
    }
}