package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.converter.LtreeFormatter;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import org.jooq.postgres.extensions.types.Ltree;

import java.util.stream.Stream;

class TestFileTreeCsvParser {

    static private final String COMMENT_REGEX = "^\s*#.*";
    static private final String DELIM_REGEX = "\s*,\s*";
    record CsvRecord(ObjectType objectType, Ltree path) {
    }

    private final LtreeFormatter ltreeFormatter = new LtreeFormatter();

    Stream<CsvRecord> parse(String csv) {
        return csv.lines().map( this::splitLine )
                .filter( split -> split.length == 2 )
                .map( this::toCsvRecord );
    }

    // returns String[0] for comments else String[2]
    private String[] splitLine(String line) throws IllegalStateException {
        if (line.matches( COMMENT_REGEX ) || line.isBlank() ) {
            // a comment;
            return new String[0];
        }
        String[] split = line.strip()
                .split( DELIM_REGEX );
        if (split.length != 2) {
            throw new IllegalArgumentException( "Unable to parse line: " + line );
        }
        return split;
    }

    private CsvRecord toCsvRecord(String[] split) {
        ObjectType objectType = ObjectType.valueOf( split[0].toUpperCase() );
        try {
            return new CsvRecord( objectType, ltreeFormatter.parse( split[1], null ) );
        } catch (Exception e) {
            throw new IllegalArgumentException( e );
        }
    }
}
