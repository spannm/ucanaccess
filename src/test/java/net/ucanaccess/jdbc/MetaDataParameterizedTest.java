package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.lang.System.Logger.Level;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

class MetaDataParameterizedTest extends UcanaccessBaseFileTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCreateBadMetadata(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        UcanaccessStatement st = ucanaccess.createStatement();
        st.execute("CREATE TABLE [健康] ([q3¹²³¼½¾ß€ Ð×ÝÞðýþäüöß] guiD PRIMARY KEY, [Sometime I wonder who I am ] TEXT )");
        st.execute("INSERT INTO [健康] ([Sometime I wonder who I am ] ) VALUES ('I''m a crazy man')");
        st.execute("UPDATE [健康] SET [Sometime I wonder who I am ]='d'");
        checkQuery("SELECT * FROM 健康 ");
        dumpQueryResult("SELECT * FROM [健康]");
        st.execute("CREATE TABLE [123456 nn%&/健康] ([q3¹²³¼½¾ß€ Ð×ÝÞðýþäüöß] AUTOINCREMENT PRIMARY KEY, [Sometime I wonder who I am ] text, "
            + "[Πλήθος Αντιγράφων] CURRENCY, [ជំរាបសួរ] CURRENCY, [ЗДОРОВЫЙ] CURRENCY, [健康] CURRENCY, [健康な] CURRENCY, "
                        + "[किआओ ] CURRENCY DEFAULT 12.88, [11q3 ¹²³¼½¾ß€] TEXT(2), UNIQUE ([किआओ ] ,[健康な]) )");
        st.execute("INSERT INTO [123456 nn%&/健康] ([Sometime I wonder who I am ], [Πλήθος Αντιγράφων], [健康], [健康な], [किआओ ] ) "
            + "VALUES('I''m a wonderful forty', 10.56, 10.33, 13, 14)");
        PreparedStatement ps = ucanaccess.prepareStatement("SELECT * FROM [123456 nn%&/健康]",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        ResultSet rs = ps.executeQuery();
        rs.moveToInsertRow();

        rs.updateString("Sometime I wonder who I am ", "Growing old without emotions");
        rs.updateString("11q3 ¹²³¼½¾ß€", "康");
        rs.insertRow();
        getLogger().log(Level.DEBUG, "Crazy names in create table with updatable resultset");
        dumpQueryResult("SELECT * FROM [123456 nn%&/健康]");

        assertThatThrownBy(() -> st.execute(
            "INSERT INTO [123456 nn%&/健康] ([Sometime I wonder who I am ], [Πλήθος Αντιγράφων], [健康], [किआओ ], 健康な) "
                + "VALUES('I''m a wonderful forty', 11, 11, 14, 13)"))
                    .isInstanceOf(UcanaccessSQLException.class)
                    .hasMessageContaining("integrity constraint violation: unique constraint or index violation");
        st.execute("INSERT INTO [123456 nn%&/健康] ([Sometime I wonder who I am ], [Πλήθος Αντιγράφων], [健康], [किआओ ], [健康な]) "
            + "VALUES('I''m a wonderful forty',11,11,14.01,13)");

        assertThatThrownBy(() -> st.execute(
            "UPDATE [123456 nn%&/健康] SET [健康な]=13, [किआओ ]=14"))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("integrity constraint violation: unique constraint or index violation");

        dumpQueryResult("SELECT * FROM [123456 nn%&/健康]");

        st.execute("UPDATE noroman SET [किआओ]='1vv'");
        checkQuery("SELECT * FROM noroman ORDER BY [किआओ]");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testRightCaseQuery(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        UcanaccessStatement st = ucanaccess.createStatement();
        assertEquals("Ciao", st.executeQuery("SELECT * FROM query1").getMetaData().getColumnLabel(1));
    }

    @SuppressWarnings("resource")
    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testBadMetadata(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        dumpQueryResult("SELECT * FROM NOROMAN");
        UcanaccessStatement st = ucanaccess.createStatement();
        assertThat(st.executeQuery("SELECT * FROM NOROMAN").getMetaData())
            .satisfies(x -> assertThat(x.isAutoIncrement(1)).isTrue())
            .satisfies(x -> assertThat(x.isCurrency(6)).isTrue())
            .satisfies(x -> assertThat(x.isCurrency(7)).isFalse());
        DatabaseMetaData dbmd = ucanaccess.getMetaData();

        getLogger().log(Level.INFO, "Noroman characters:");
        dumpQueryResult(() -> dbmd.getTables(null, null, "NOROMAn", null));
        dumpQueryResult(() -> dbmd.getColumns(null, null, "NOROMAn", null));
        dumpQueryResult(() -> dbmd.getColumns(null, null, "%ROMAn", null));
        getLogger().log(Level.INFO, "getColumns:");
        dumpQueryResult(() -> dbmd.getColumns(null, null, "Πλήθ%", null));
        dumpQueryResult(() -> dbmd.getColumns(null, null, "%健康", null));
        dumpQueryResult(() -> dbmd.getColumns(null, null, "TAbELLA1", "%e"));
        getLogger().log(Level.INFO, "getColumnPrivileges:");
        dumpQueryResult(() -> dbmd.getColumnPrivileges(null, null, "NOROMAn", null));
        getLogger().log(Level.INFO, "getExportedKeys:");
        dumpQueryResult(() -> dbmd.getExportedKeys(null, null, "??###"));
        getLogger().log(Level.INFO, "getImportedKeys:");
        dumpQueryResult(() -> dbmd.getImportedKeys(null, null, "Tabella1"));
        getLogger().log(Level.INFO, "getPrimaryKeys:");
        dumpQueryResult(() -> dbmd.getPrimaryKeys(null, null, "Tabella1"));
        getLogger().log(Level.INFO, "getIndexInfo:");
        dumpQueryResult(() -> dbmd.getIndexInfo(null, null, "Tabella1", false, false));
        getLogger().log(Level.INFO, "getCrossReference:");
        dumpQueryResult(() -> dbmd.getCrossReference(null, null, "??###", null, null, "Tabella1"));
        getLogger().log(Level.INFO, "getVersionColumns:");
        dumpQueryResult(() -> dbmd.getVersionColumns(null, null, "Πλήθος"));
        getLogger().log(Level.INFO, "getClientInfoProperties:");
        dumpQueryResult(dbmd::getClientInfoProperties);
        getLogger().log(Level.INFO, "getTablePrivileges:");
        dumpQueryResult(() -> dbmd.getTablePrivileges(null, null, "??###"));
        getLogger().log(Level.INFO, "getTables:");
        dumpQueryResult(() -> dbmd.getTables(null, null, "??###", new String[] {"TABLE"}));
        dumpQueryResult(() -> dbmd.getTables(null, null, null, new String[] {"VIEW"}));
        getLogger().log(Level.INFO, "getBestRowIdentifier:");
        dumpQueryResult(() -> dbmd.getBestRowIdentifier(null, null, "??###", DatabaseMetaData.bestRowTemporary, true));
        dumpQueryResult(() -> dbmd.getBestRowIdentifier(null, null, "??###", DatabaseMetaData.bestRowSession, true));
        getLogger().log(Level.INFO, "getTypeInfo:");
        dumpQueryResult(dbmd::getTypeInfo);
    }
}
