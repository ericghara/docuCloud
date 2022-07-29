package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.dto.CloudUser;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TestFileTreeFactory {

    static String defaultTree = """
            ROOT, ""
            DIR, "dir0"
            FILE, "file0"
            DIR, "dir0.dir1"
            DIR, "dir0.dir1.dir2"
            DIR, "dir0.dir3"
            FILE, "dir0.dir3.file1"
            """; 


    private final TreeTestQueries testQueries;

    public TestFileTree construct(String userId) {
        return new TestFileTree(userId, testQueries);
    }

    public TestFileTree construct(CloudUser cloudUser) {
        return new TestFileTree(cloudUser, testQueries);
    }

    public TestFileTree constructDefault(String userId) {
        var testTree = new TestFileTree(userId, testQueries);
        testTree.addFromCsv( defaultTree );
        return testTree;
    }

    public TestFileTree constructDefault(CloudUser cloudUser) {
        return constructDefault( cloudUser.getUserId() );
    }
}
