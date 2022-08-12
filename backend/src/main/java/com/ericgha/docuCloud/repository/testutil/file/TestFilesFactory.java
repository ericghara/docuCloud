package com.ericgha.docuCloud.repository.testutil.file;

import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.repository.testutil.tree.TestFileTree;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
@Profile("test")
@RequiredArgsConstructor
public class TestFilesFactory {
    private final FileTestQueries fileQueries;

    private final Converter<FileViewDto, FileDto> fileViewToFile;

    public TestFiles construct(TestFileTree tree) {
        return new TestFiles(tree, fileQueries, fileViewToFile);
    }

    public TestFiles constructFromCsv(String csv, TestFileTree tree) {
        var testFiles = this.construct(tree);
        testFiles.insertFileViewRecords( csv );
        return testFiles;
    }
}
