package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.sql.Statement;

class CreateDatabaseTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testNewDatabase(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        File fileMdb = createTempFileName(getClass().getSimpleName());
        fileMdb.deleteOnExit();

        UcanaccessConnectionBuilder bldr = buildConnection()
            .withDbPath(fileMdb.getAbsolutePath())
            .withoutUserPass()
            .withImmediatelyReleaseResources()
            .withNewDatabaseVersionProp(getFileFormat().name());
        getLogger().debug("Database url: {}", bldr.getUrl());
        UcanaccessConnection conn = bldr.build();

        assertNotNull(conn);
        ucanaccess.close();
        ucanaccess = conn;

        getLogger().info("Database file successfully created: {}", fileMdb.getAbsolutePath());


        try (Statement st = conn.createStatement()) {
            executeStatements(st,
                "CREATE TABLE AAA (baaaa TEXT(3) PRIMARY KEY, A LONG DEFAULT 3, C TEXT(4))",
                "INSERT INTO AAA(baaaa, c) VALUES ('33A','G' )",
                "INSERT INTO AAA VALUES ('33B',111,'G' )");
        }
        dumpQueryResult("SELECT * FROM AAA");
    }

}
