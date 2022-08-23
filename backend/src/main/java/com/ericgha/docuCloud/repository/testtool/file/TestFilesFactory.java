package com.ericgha.docuCloud.repository.testtool.file;

import com.ericgha.docuCloud.dto.FileDto;
import com.ericgha.docuCloud.dto.FileViewDto;
import com.ericgha.docuCloud.repository.testtool.tree.TestFileTree;
import lombok.RequiredArgsConstructor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
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
