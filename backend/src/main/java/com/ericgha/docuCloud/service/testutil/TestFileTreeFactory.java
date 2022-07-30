package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
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

    public TestFileTree constructRoot(String userId) {
        var tree = new TestFileTree(userId, testQueries);
        tree.add( ObjectType.ROOT, "");
        return tree;
    }

    public TestFileTree constructRoot(CloudUser cloudUser) {
        return this.constructRoot( cloudUser.getUserId() );
    }

    public TestFileTree constructFromCsv(String csv, String userId) {
        var tree = new TestFileTree(userId, testQueries);
        tree.addFromCsv( csv );
        return tree;
    }

    public TestFileTree constructFromCsv(String csv, CloudUser cloudUser) {
        return this.constructFromCsv( csv, cloudUser.getUserId() );
    }

    public TestFileTree construct(String userId) {
        return new TestFileTree( userId, testQueries );
    }

    public TestFileTree construct(CloudUser cloudUser) {
        return this.construct(cloudUser.getUserId() );
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
