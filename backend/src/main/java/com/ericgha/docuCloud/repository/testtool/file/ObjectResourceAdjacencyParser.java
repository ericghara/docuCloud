package com.ericgha.docuCloud.repository.testtool.file;

import lombok.NonNull;

import java.util.Objects;
import java.util.stream.Stream;

public class ObjectResourceAdjacencyParser {

    private ObjectResourceAdjacencyParser() throws IllegalAccessException {
        throw new IllegalAccessException( "Do not instantiate -- utility class" );

    }

    static private final String COMMENT_REGEX = "^\s*#.*";
    static private final String DELIM_REGEX = "\s*,\s*";

    public static Stream<ObjectResourceAdjacency> parse(@NonNull String csv) {
        return csv.lines().map( ObjectResourceAdjacencyParser::splitLine )
                .filter( split -> split.length == 2 )
                .map( ObjectResourceAdjacencyParser::toCsvRecord );
    }

    // returns String[0] for comments else String[2]
    private static String[] splitLine(String line) throws IllegalArgumentException {
        if (line.matches( COMMENT_REGEX ) || line.isBlank()) {
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

    private static ObjectResourceAdjacency toCsvRecord(String[] split) {
        return new ObjectResourceAdjacency( split[0], split[1] );
    }

    public record ObjectResourceAdjacency(String treePath, String fileChecksum) implements Comparable<ObjectResourceAdjacency> {

        @Override
        public int compareTo(@NonNull ObjectResourceAdjacency other) {
            if (Objects.isNull(this.treePath) || Objects.isNull(other.treePath)
            || Objects.isNull(this.fileChecksum) || Objects.isNull(other.fileChecksum) ) {
                throw new NullPointerException("Comparison of objects with null fields is not supported");
            }
            int comp = this.treePath.compareTo( other.treePath );
            if (comp == 0) {
                comp = this.fileChecksum.compareTo( other.fileChecksum );
            }
            return comp;
        }
    }
}
