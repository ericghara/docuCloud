package com.ericgha.docuCloud.repository.testtool.assertion;

import java.time.OffsetDateTime;
import java.time.temporal.TemporalAmount;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffsetDateTimeAssertion {

    private OffsetDateTimeAssertion() throws IllegalAccessException {
        throw new IllegalAccessException("Do not instantiate.");
    }

    // asserts that an OffsetDateTime 1) Happened in the past 2) is younger than current time offset by timeUnit
    // Example, if a time unit of 1sec is provided the OffsetDateTime would fail if it is from >=1 sec ago
    // It will also fail if it's from .1 sec in the future.
    public static void assertPastDateTimeWithinLast(OffsetDateTime dateTime, TemporalAmount timeUnit) {
        OffsetDateTime cur = OffsetDateTime.now();
        assertTrue(cur.isAfter(dateTime), "Found date time is in the future" );
        assertTrue(cur.isBefore(dateTime.plus(timeUnit) ), "Found date time is older than expected" );
    }
}
