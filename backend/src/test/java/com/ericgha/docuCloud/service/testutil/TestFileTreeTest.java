package com.ericgha.docuCloud.service.testutil;

import com.ericgha.docuCloud.dto.CloudUser;
import com.ericgha.docuCloud.jooq.enums.ObjectType;
import com.ericgha.docuCloud.jooq.tables.records.TreeRecord;
import com.ericgha.docuCloud.testconainer.EnablePostgresTestContainerContextCustomizerFactory.EnabledPostgresTestContainer;
import jakarta.annotation.PostConstruct;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EnabledPostgresTestContainer
public class TestFileTreeTest {

    @Autowired
    DSLContext dsl;
    TreeTestQueries treeTestQueries;
    TestFileTreeFactory treeFactory;
    @PostConstruct
    void postConstruct() {
        treeTestQueries =  new TreeTestQueries( dsl );
        treeFactory = new TestFileTreeFactory( treeTestQueries );
    }

    CloudUser user0 = CloudUser.builder()
            .userId( "1234567-89ab-cdef-fedc-ba9876543210" )
            .username( "unitTester" )
            .realm( "cloud9" ).build();

    CloudUser user1 = CloudUser.builder()
            .userId( "fffffff-ffff-ffff-fedc-ba9876543210" )
            .username( "unitTester" )
            .realm( "cloud9" ).build();




    @BeforeEach
    void before() throws URISyntaxException, IOException {
        // testcontainers cannot reliably run complex init scrips (ie with declared functions)
        // testcontainers/testcontainers-java issue #2814
        Path input = Paths.get( this.getClass().getClassLoader().getResource( "tests-schema.sql" ).toURI() );
        String sql = Files.readString( input );
        Mono.from( dsl.query( sql ) ).block();
    }

    @Test
    @DisplayName( "add adds the expected record" )
    void addAddsRecord() {
        String pathStr = "";
        ObjectType objectType = ObjectType.ROOT;
        var testTree = treeFactory.construct( user0 );
        TreeRecord newRecord = testTree.add( objectType, pathStr);
        assertEquals(newRecord, testTree.getRecordByPath( "" ) );
        assertEquals( pathStr, newRecord.getPath().data() );
        assertEquals(  objectType, newRecord.getObjectType() );
    }




}
