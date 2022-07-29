package com.ericgha.docuCloud.converter;

import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.format.Formatter;
import org.springframework.lang.Nullable;

import java.text.ParseException;
import java.util.Locale;
import java.util.Objects;

public class LtreeFormatter implements Formatter<Ltree> {

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
