package com.ericgha.docuCloud.repository.testutil.file;

import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.dto.TreeDto;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTree;
import com.ericgha.docuCloud.util.comparators.FileViewDtoComparators;
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
    private static final Comparator<FileViewDto> fileViewDtoComp = FileViewDtoComparators.compareByObjectIdFileId();

    private final FileTestQueries fileQueries;

    public TestFiles construct(TestFileTree tree) {
        return new TestFiles(tree, fileQueries, treeDtoComp, fileViewDtoComp);
    }

    public TestFiles constructFromCsv(TestFileTree tree, String csv) {
        var testFiles = this.construct(tree);
        testFiles.createFileViewDtos( csv );
        return testFiles;
    }
}
