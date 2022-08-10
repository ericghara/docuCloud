package com.ericgha.docuCloud.converter;

import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.text.ParseException;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class pathStrToEncodedLtreeTest {

    private final PathStrToEncodedLtree formatter = new PathStrToEncodedLtree();

    @ParameterizedTest
    @DisplayName( "Parse returns the expected value for propery formatted text inputs" )
    @CsvSource(delimiter = '|', quoteCharacter = '"', textBlock = """
    #   text  |  expected
        "/" | ""
        "/dir0/dir1/file1.exe" | "dir0.dir1.file1ǃ2eexe"
        "/dir0/dir@1/file#1_&file2$.exe" | "dir0.dirǃ401.fileǃ231_ǃ26file2ǃ24ǃ2eexe"
    """)
    void parseReturnsExpectedValue(String text, String expected) throws ParseException {
        assertEquals(expected, formatter.parse(text, Locale.US).data() );
    }

    @Test
    @DisplayName( "Parse throws a ParseException when no leading '/'" )
    void parseThrowsWhenNoStartSlash() {
        String text = "a/b/c";
        assertThrows( ParseException.class, () -> formatter.parse(text, Locale.US) );
    }

    @ParameterizedTest
    @DisplayName( "Print decodes to the expected text for properly formatted ltrees" )
    @CsvSource(delimiter = '|', quoteCharacter = '"', textBlock = """
    #   expected  |  ltree
        "/" | ""
        "/dir0/dir1/file1.exe" | "dir0.dir1.file1ǃ2eexe"
        "/dir0/dir@1/file#1_&file2$.exe" | "dir0.dirǃ401.fileǃ231_ǃ26file2ǃ24ǃ2eexe"
    """)
    void printDecodesExpectedString(String expected, String ltree) {
        assertEquals( expected, formatter.print( Ltree.valueOf(ltree), Locale.US) );
    }

    @Test
    @DisplayName( "Print throws IllegalArgumentException when improperly formatted non-alphanumeric charcater" )
    void printThrowsWhenDecodingImroperlyFormattedLtree() {
        Ltree ltree = Ltree.valueOf("a/ǃ2");
        assertThrows( IllegalArgumentException.class, () -> formatter.print( ltree, Locale.US ) );
    }
}