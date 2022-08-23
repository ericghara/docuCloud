package com.ericgha.docuCloud.repository.testtool.assertion;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/**
 * These are assertions that provide more descriptive
 * errors than could be directly achieved by using
 * vanilla JUnit
 */
public class CollectionAssertion {

    /**
     * Asserts that all items are found in the allItems.  The allItems
     * may contain additional items.  Similar to Collection#containsAll
     * however the error messages will be more descriptive than
     * {@code assertTrue(allItems.containsAll(itemCollection)}
     * @param wanted
     * @param allItems
     * @param <T>
     */
    public static <T> void assertCollectionContainsAll(Collection<T> allItems, Iterable<T> wanted) {
        if (Objects.isNull(allItems) && Objects.isNull( wanted ) ) {
            return;
        }
        throwIfOneNull( allItems, wanted );
        for (T i : wanted) {
            if (!allItems.contains( i ) ) {
                throw new AssertionError(String.format("Item: %s was not found in allItems", i) );
            }
        }
    }

    public static <T,U> void assertCollectionMapsEqual(Map<T, ? extends Collection<U>> expected, Map<T,? extends Collection<U>> found) {
        if (Objects.isNull(expected) && Objects.isNull( found ) ) {
            return;
        }
        throwIfOneNull( expected, found );
        assertIterableEquals( expected.keySet(), found.keySet() );
        for (Map.Entry<T, ? extends Collection<U>> entry : found.entrySet() ) {
            T foundKey = entry.getKey();
            Collection<U> foundVal = entry.getValue();
            Collection<U> expectedVal = expected.get(foundKey );
            if (!foundVal.equals(expectedVal)) {
                try {
                    assertIterableEquals(expectedVal, foundVal);
                } catch (AssertionError e) {
                    throw new AssertionError(String.format("Unequal values for key: %s%n" +
                                    "Expected Value: %s%n" +
                                    "Found Value: %s",
                            foundKey, expectedVal, foundVal), e );
                }
            }
        }
    }

    /**
     * Asserts that all items in the wanted map's Collections are present at corresponding
     * keys in the available map.<br><br>
     * example pass: <pre>
     *     available: 1=["a","b","c"]
     *     wanted: 1=["c"]
     * </pre>
     * example fail: <pre>
     *     available: 1=["a","b","c"]
     *     wanted: 2=["c"]
     * </pre>
     * example fail: <pre>
     *     available: 1=["a","b","c"]
     *     wanted: 1=["a","z"]
     * </pre>
     * @param available
     * @param wanted
     * @param <T>
     * @param <U>
     * @throws AssertionError
     */
    public static <T,U> void assertCollectionMapContainsAll(
            Map<T, ? extends Collection<U>> available, Map<T,? extends Collection<U>> wanted) throws AssertionError {
        if (Objects.isNull(available) && Objects.isNull( wanted ) ) {
            return;
        }
        throwIfOneNull( available, wanted );
        for (Map.Entry<T, ? extends Collection<U>> entry : wanted.entrySet() ) {
            T foundKey = entry.getKey();
            Collection<U> wantedVals = entry.getValue();
            Collection<U> allVals = available.get( foundKey );
            try {
                CollectionAssertion.assertCollectionContainsAll( allVals, wantedVals );
            } catch (AssertionError e) {
                throw new AssertionError(String.format("wanted contains unexpected value(s) for key: %s%n" +
                                "Available Items: %s%n" +
                                "Wanted Items: %s",
                        foundKey, allVals, wantedVals), e );
            }
        }
    }

    private static <T,U> void throwIfOneNull(T expected, U found) throws AssertionError {
        if (Objects.isNull(expected) || Objects.isNull( found ) ) {
            throw new AssertionError(String.format("Received a null input%n" +
                    "Expected was: %s%n" +
                    "Found was: %s", expected, found) );
        }
    }

}
