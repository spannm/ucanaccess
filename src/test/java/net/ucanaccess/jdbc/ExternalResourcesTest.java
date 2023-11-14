package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

class ExternalResourcesTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.AccessVersion#getDefaultAccessVersion()")
    void testLinks(AccessVersion _accessVersion) throws SQLException, ClassNotFoundException {
        init(_accessVersion);

        File main = copyResourceToTempFile(TEST_DB_DIR + "main.mdb");
        File linkee1 = copyResourceToTempFile(TEST_DB_DIR + "linkee1.mdb");
        File linkee2 = copyResourceToTempFile(TEST_DB_DIR + "linkee2.mdb");

        UcanaccessConnectionBuilder bldr = buildConnection()
            .withDbPath(main.getAbsolutePath())
            .withUser("")
            .withPassword("")
            .withParm("immediatelyReleaseResources", true)
            .withParm("remap", "c:\\db\\linkee1.mdb|" + linkee1.getAbsolutePath() + "&c:\\db\\linkee2.mdb|" + linkee2.getAbsolutePath());
        getLogger().info("Database url: {}", bldr.buildUrl());
        
        try (Connection conn = bldr.build();
            Statement st = conn.createStatement()) {

            dumpQueryResult(() -> st.executeQuery("SELECT * FROM table1"));
            dumpQueryResult(() -> st.executeQuery("SELECT * FROM table2"));
        }
    }

}
