package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.File;
import java.lang.System.Logger.Level;

class CreateDatabaseTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testNewDatabase(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        File fileMdb = createTempFileName(getClass().getSimpleName());
        fileMdb.deleteOnExit();

        UcanaccessConnectionBuilder bldr = buildConnection()
            .withDbPath(fileMdb.getAbsolutePath())
            .withoutUserPass()
            .withImmediatelyReleaseResources()
            .withNewDatabaseVersion(getAccessVersion());
        getLogger().log(Level.DEBUG, "Database url: {0}", bldr.getUrl());
        UcanaccessConnection conn = bldr.build();

        assertNotNull(conn);
        ucanaccess.close();
        ucanaccess = conn;

        getLogger().log(Level.INFO, "Database file successfully created: {0}", fileMdb.getAbsolutePath());

        try (UcanaccessStatement st = conn.createStatement()) {
            executeStatements(st,
                "CREATE TABLE t_createdb (baaaa TEXT(3) PRIMARY KEY, A LONG DEFAULT 3, C TEXT(4))",
                "INSERT INTO t_createdb(baaaa, c) VALUES ('33A', 'G')",
                "INSERT INTO t_createdb VALUES ('33B',111,'G')");
        }
        dumpQueryResult("SELECT * FROM t_createdb");
    }

}
