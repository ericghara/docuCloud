package com.ericgha.docuCloud.converter;


import lombok.extern.slf4j.Slf4j;
import org.jooq.postgres.extensions.types.Ltree;
import org.springframework.format.Formatter;

import java.text.ParseException;
import java.util.Locale;
import java.util.Objects;

/**
 * This is a workaround for ltree to enable representation of non-alphanumeric tree labels.  Ltree is
 * limited to alphanumeric characters and '_' for tree labels.  This class enables represenation
 * of non-letter ASCII printable characters by encoding them alphanumerically.
 */
@Slf4j
public class TextToLabel implements Formatter<Ltree> {

    static final char IDENTIFIER = 'ǃ'; // this is a 'Latin Letter Retroflex Click' character
    static final char SEPARATOR = '/'; // character that should be mapped to '.' the ltree separator

    @Override
    public Ltree parse(String text, Locale locale) throws ParseException {
        if (Objects.isNull(text) ) {
            log.debug( "Received a null Ltree" );
            return null;
        }
        if (text.length() < 1 || text.charAt(0) != SEPARATOR ) {
            throw new ParseException( String.format("Input string must being with the '%c' character", SEPARATOR ),
                    text.length() > 0 ? 0 : -1);
        }
        StringBuilder builder = new StringBuilder(text.length() );
        for (int i = 1; i < text.length(); i++) {
            char c = text.charAt( i );
            if (Character.isLetterOrDigit( c ) || c == '_' ) {
                if (c == IDENTIFIER) {
                    throw new ParseException( String.format("Illegal character %s in Ltree string.", IDENTIFIER), i );
                }
                builder.append(c);
            }
            else if (c == SEPARATOR) {
                builder.append('.');
            }
            else {
                builder.append(IDENTIFIER);
                builder.append( Integer.toHexString( c ) );
            }
        }
        return Ltree.valueOf(builder.toString() );
    }

    @Override
    public String print(Ltree ltree, Locale locale) {
        if (Objects.isNull(ltree) ) {
            return null;
        }
        String data = ltree.data();
        StringBuilder decodeStr = new StringBuilder(data.length() );
        decodeStr.append( SEPARATOR );
        try {
            for (int i = 0; i < data.length(); i++) {
                char  c = data.charAt( i );
                switch (c) {
                    case '.' -> decodeStr.append( SEPARATOR );
                    case IDENTIFIER -> {
                        i++;
                        char specialChar = (char) Integer.parseInt( data.substring( i, i + 2 ), 16 );
                        decodeStr.append( specialChar );
                        i++;
                    }
                    default -> decodeStr.append( c );
                }
            }
        } catch (StringIndexOutOfBoundsException e) {
            throw new IllegalArgumentException( "Improperly encoded non-alphanumeric character");
        }
        return decodeStr.toString();
    }
}
