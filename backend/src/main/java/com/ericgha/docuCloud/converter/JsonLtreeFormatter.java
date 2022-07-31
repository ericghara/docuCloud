package com.ericgha.docuCloud.converter;

import lombok.extern.slf4j.Slf4j;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.format.Formatter;
import org.springframework.lang.Nullable;

import java.util.Locale;
import java.util.Objects;

/**
 * For parsing {@link Ltree}s from JSON files.  Since
 * JSON explicitly handles empty strings' no enclosing
 * parenthesis are added (unlike {@link CsvLtreeFormatter}).
 */
@Slf4j
public class JsonLtreeFormatter implements Formatter<Ltree> {

    static String parseExceptionMsg = "Unrecognized text input all text must be enclosed in quotes";

    public Ltree parse(@Nullable String text, Locale locale) {
        if (Objects.isNull(text) ) {
            log.debug( "Received a null Ltree" );
            return null;
        }
        return Ltree.valueOf( text );
    }

    public String print(@Nullable Ltree ltree, Locale locale) {
        if (Objects.isNull(ltree) ) {
            return null;
        }
        return ltree.data();
    }
}
