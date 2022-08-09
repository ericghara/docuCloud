package com.ericgha.docuCloud.repository.testutil.file;

import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.jooq.tables.records.FileViewRecord;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTree;
import com.ericgha.docuCloud.util.comparators.FileViewRecordComparators;
import com.ericgha.docuCloud.util.comparators.TreeDtoComparators;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Comparator;

@Component
@Profile("test")
@RequiredArgsConstructor
public class TestFilesFactory {

    private static final Comparator<TreeDto> treeDtoComp = TreeDtoComparators.compareByObjectId();
    private static final Comparator<FileViewRecord> fileViewRecordComp = FileViewRecordComparators.compareByObjectIdFileId();

    private final FileTestQueries fileQueries;

    public TestFiles construct(TestFileTree tree) {
        return new TestFiles(tree, fileQueries, treeDtoComp, fileViewRecordComp);
    }

    public TestFiles constructFromCsv(TestFileTree tree, String csv) {
        var testFiles = this.construct(tree);
        testFiles.createFileViewRecords( csv );
        return testFiles;
    }
}
