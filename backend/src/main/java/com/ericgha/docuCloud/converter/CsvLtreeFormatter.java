package com.ericgha.docuCloud.converter;

import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.format.Formatter;
import org.springframework.lang.Nullable;

import java.text.ParseException;
import java.util.Locale;
import java.util.Objects;

/**
 * For parsing {@link Ltree}s from CSV files.  In text format
 * The valid {@code Ltree} with a {@code level = 0} is ambiguous
 * as it is an empty string.  Therefore, this formatter reads string
 * representations of Ltree that explicitly have enclosing quotes.
 * In this case an empty Ltree is represented as "" and a Ltree
 * with level of 2 is represented as {@code "level1.level2"}.
 */
public class CsvLtreeFormatter implements Formatter<Ltree> {

    static String parseExceptionMsg = "Unrecognized text input all text must be enclosed in quotes";

    public Ltree parse(@Nullable String text, Locale locale) throws ParseException {
        if (Objects.isNull(text) ) {
            return null;
        }
        final int len = text.length();
        if (len < 2) {
            throw new ParseException( parseExceptionMsg, len );
        }
        if (text.charAt( 0 ) != '"') {
            throw new ParseException( parseExceptionMsg, 0 );
        }
        if (text.charAt( len-1 ) != '"') {
            throw new ParseException( parseExceptionMsg, len-1 );
        }
        return Ltree.valueOf( text.substring( 1, len - 1 ) );
    }

    public String print(@Nullable Ltree ltree, Locale locale) {
        if (Objects.isNull(ltree) ) {
            return "";
        }
        return String.format("\"%s\"", ltree.data() );
    }
}
