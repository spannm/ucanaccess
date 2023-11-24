package net.ucanaccess.jdbc;

import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.sql.SQLException;

class ExternalResourcesTest extends UcanaccessBaseTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testLinks(AccessVersion _accessVersion) throws SQLException, ClassNotFoundException {
        init(_accessVersion);

        File main = copyResourceToTempFile(getTestDbDir() + "main.mdb");
        File linkee1 = copyResourceToTempFile(getTestDbDir() + "linkee1.mdb");
        File linkee2 = copyResourceToTempFile(getTestDbDir() + "linkee2.mdb");

        UcanaccessConnectionBuilder bldr = buildConnection()
            .withDbPath(main.getAbsolutePath())
            .withoutUserPass()
            .withImmediatelyReleaseResources()
            .withProp(Property.reMap, "c:\\db\\linkee1.mdb|" + linkee1.getAbsolutePath() + "&c:\\db\\linkee2.mdb|" + linkee2.getAbsolutePath());
        getLogger().debug("Database url: {}", bldr.getUrl());

        try (UcanaccessConnection conn = bldr.build();
            UcanaccessStatement st = conn.createStatement()) {

            dumpQueryResult(() -> st.executeQuery("SELECT * FROM table1"));
            dumpQueryResult(() -> st.executeQuery("SELECT * FROM table2"));
        }
    }

}
