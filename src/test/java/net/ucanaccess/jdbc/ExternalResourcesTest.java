package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.lang.System.Logger.Level;
import java.sql.ResultSet;
import java.sql.SQLException;

class ExternalResourcesTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "linked.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testLinks(AccessVersion accessVersion) throws SQLException {
        setAccessVersion(accessVersion);

        String dbLinked = getAccessTempPath();
        String dbLinkee1 = copyResourceToTempFile(getTestDbDir() + "linkee1.mdb").getAbsolutePath();
        String dbLinkee2 = copyResourceToTempFile(getTestDbDir() + "linkee2.mdb").getAbsolutePath();
        String origPath = "c:\\db\\";

        UcanaccessConnectionBuilder bldr = buildConnection()
            .withDbPath(dbLinked)
            .withoutUserPass()
            .withImmediatelyReleaseResources()
            .withProp(Property.reMap, origPath + "linkee1.mdb" + '|' + dbLinkee1
                              + '&' + origPath + "linkee2.mdb" + '|' + dbLinkee2);
        getLogger().log(Level.DEBUG, "Database url: {0}", bldr.getUrl());

        try (UcanaccessConnection conn = bldr.build();
            UcanaccessStatement st = conn.createStatement()) {

            try (ResultSet rs = st.executeQuery("SELECT ID, field1 FROM table1")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isOne();
                assertThat(rs.getString(2)).isEqualTo("ciao bello");
                assertThat(rs.next()).isFalse();
            }
            try (ResultSet rs = st.executeQuery("SELECT ID, filed2 FROM table2")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isOne();
                assertThat(rs.getString(2)).isEqualTo("ciao brutto");
                assertThat(rs.next()).isFalse();
            }
        }
    }

}
