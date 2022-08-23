package com.ericgha.docuCloud.repository.testtool.tree;

import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.repository.testtool.tree.TestFileTreeCsvParser.CsvRecord;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class TestFileTreeCsvParserTest {

    @Test
    @DisplayName("csvParser#parse generates the expected output")
    void csvParserProducesExpectedOutput() {
        // @formatter:off
        String csv = """
                ## odd formatting is intentional for test ##
                            
                ROOT,""
                Dir, "dir0"
                   fILe,"file0"
                dir,"dir0.dir1"
                DIR,    "dir0.dir1.dir2"   
                    DIR, "dir0.dir3"   
                FILE, "dir0.dir3.file1"  
                """;
        // @formatter:on
        TestFileTreeCsvParser parser = new TestFileTreeCsvParser();
        List<CsvRecord> found = parser.parse( csv ).toList();
        List<CsvRecord> expected = List.of(
                new CsvRecord( ObjectType.ROOT, Ltree.valueOf( "" ) ),
                new CsvRecord( ObjectType.DIR, Ltree.valueOf( "dir0" ) ),
                new CsvRecord( ObjectType.FILE, Ltree.valueOf( "file0" ) ),
                new CsvRecord( ObjectType.DIR, Ltree.valueOf( "dir0.dir1" ) ),
                new CsvRecord( ObjectType.DIR, Ltree.valueOf( "dir0.dir1.dir2" ) ),
                new CsvRecord( ObjectType.DIR, Ltree.valueOf( "dir0.dir3" ) ),
                new CsvRecord( ObjectType.FILE, Ltree.valueOf( "dir0.dir3.file1" ) )
        );
        assertIterableEquals( found, expected );
    }

}
