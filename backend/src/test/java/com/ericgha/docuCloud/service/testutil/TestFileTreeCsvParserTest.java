package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.service.testutil.TestFileTreeCsvParser.CsvRecord;
import org.jooq.postgres.extensions.types.Ltree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class TestFileTreeCsvParserTest {

    @Test
    @DisplayName("csvParser#parse generates the expected output")
    void csvParserProducesExpectedOutput() {
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
        TestFileTreeCsvParser parser = new TestFileTreeCsvParser();
        List<CsvRecord> found = parser.parse( csv ).toList();
        List<CsvRecord> expected = List.of(
                new CsvRecord( ObjectType.ROOT, Ltree.valueOf("") ),
                new CsvRecord( ObjectType.DIR, Ltree.valueOf("dir0") ),
                new CsvRecord( ObjectType.FILE, Ltree.valueOf("file0") ),
                new CsvRecord( ObjectType.DIR, Ltree.valueOf("dir0.dir1") ),
                new CsvRecord( ObjectType.DIR, Ltree.valueOf("dir0.dir1.dir2") ),
                new CsvRecord( ObjectType.DIR, Ltree.valueOf("dir0.dir3") ),
                new CsvRecord( ObjectType.FILE, Ltree.valueOf("dir0.dir3.file1") )
        );
        assertIterableEquals(found, expected);
    }

}
