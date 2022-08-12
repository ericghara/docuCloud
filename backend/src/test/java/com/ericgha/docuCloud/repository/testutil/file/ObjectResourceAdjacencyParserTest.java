package com.ericgha.docuCloud.repository.testutil.file;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ObjectResourceAdjacencyParserTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "dir0.fileObj0, fileRes0",
            " dir0.fileObj0, fileRes0 ",
            "dir0.fileObj0,fileRes0"
    })
    @DisplayName("TestFilesCsvParser#parse returns expected CsvRecords -- data lines")
    void parseReturnsExpectedRecordsData(String csvLine) {
        String expectedPath = "dir0.fileObj0";
        String expectedChecksum = "fileRes0";
        Iterable<ObjectResourceAdjacencyParser.ObjectResourceAdjacency> expectedRec = List.of(new ObjectResourceAdjacencyParser.ObjectResourceAdjacency( expectedPath, expectedChecksum ) );
        Iterable<ObjectResourceAdjacencyParser.ObjectResourceAdjacency> foundRec = ObjectResourceAdjacencyParser.parse( csvLine ).toList();

        assertIterableEquals( expectedRec, foundRec );
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "# comment",
            "## comment",
            "# comment, comment, comment",
            " # # # # comment, comment  "
    })
    @DisplayName("TestFilesCsvParser#parse returns expected CsvRecords -- comment lines")
    void parseReturnsExpectedRecordsComment(String csvLine) {
        Iterable<ObjectResourceAdjacencyParser.ObjectResourceAdjacency> expectedRec = List.of();
        Iterable<ObjectResourceAdjacencyParser.ObjectResourceAdjacency> foundRec = ObjectResourceAdjacencyParser.parse( csvLine ).toList();

        assertIterableEquals( expectedRec, foundRec );
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "too, many, rows",
            "too few rows",
    })
    @DisplayName("TestFilesCsvParser#parse throws IllegalStateException when poorly formatted")
    void parseReturnsThrowsOnBadFormat(String csvLine) {
        // findAny to make stream hot
        assertThrows(IllegalArgumentException.class, () -> ObjectResourceAdjacencyParser.parse( csvLine ).findAny() );
    }
}