package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.*;

class MetaDataParameterizedTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "badDb.accdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCreateBadMetadata(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        Connection conn = ucanaccess;
        Statement st = conn.createStatement();
        st.execute("CREATE TABLE [健康] ([q3¹²³¼½¾ß€ Ð×ÝÞðýþäüöß] guiD PRIMARY KEY, [Sometime I wonder who I am ] text )");
        st.execute("INSERT INTO [健康] ([Sometime I wonder who I am ] ) values ('I''m a crazy man')");
        st.execute("UPDATE [健康] set [Sometime I wonder who I am ]='d'");
        checkQuery("SELECT * FROM 健康 ");
        dumpQueryResult("SELECT * FROM [健康]");
        st.execute("CREATE TABLE [123456 nn%&/健康] ([q3¹²³¼½¾ß€ Ð×ÝÞðýþäüöß] aUtoIncrement PRIMARY KEY, [Sometime I wonder who I am ] text, "
            + "[Πλήθος Αντιγράφων] CURRENCY,[ជំរាបសួរ] CURRENCY,[ЗДОРОВЫЙ] CURRENCY,[健康] CURRENCY,[健康な] CURRENCY,[किआओ ] CURRENCY default 12.88, [11q3 ¹²³¼½¾ß€] text(2), unique ([किआओ ] ,[健康な]) )");
        st.execute("INSERT INTO [123456 nn%&/健康] ([Sometime I wonder who I am ],[Πλήθος Αντιγράφων],[健康],[健康な],[किआओ ] ) VALUES('I''m a wonderful forty',10.56,10.33,13,14)");
        PreparedStatement ps = ucanaccess.prepareStatement("SELECT * FROM [123456 nn%&/健康]",
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        ResultSet rs = ps.executeQuery();
        rs.moveToInsertRow();

        rs.updateString("Sometime I wonder who I am ", "Growing old without emotions");
        rs.updateString("11q3 ¹²³¼½¾ß€", "康");
        rs.insertRow();
        getLogger().debug("Crazy names in create table with updatable resultset...");
        dumpQueryResult("SELECT * FROM [123456 nn%&/健康]");

        assertThatThrownBy(() -> st.execute(
            "INSERT INTO [123456 nn%&/健康] ([Sometime I wonder who I am ],[Πλήθος Αντιγράφων],[健康],[किआओ ] ,健康な) VALUES('I''m a wonderful forty',11,11,14,13)"))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("integrity constraint violation: unique constraint or index violation");
        st.execute("INSERT INTO [123456 nn%&/健康] ([Sometime I wonder who I am ],[Πλήθος Αντιγράφων],[健康],[किआओ ] ,[健康な]) "
            + "VALUES('I''m a wonderful forty',11,11,14.01,13)");

        assertThatThrownBy(() -> st.execute(
            "update [123456 nn%&/健康] set [健康な]=13, [किआओ ]=14"))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("integrity constraint violation: unique constraint or index violation");

        dumpQueryResult("SELECT * FROM [123456 nn%&/健康]");

        st.execute("update noroman set [किआओ]='1vv'");
        checkQuery("SELECT * FROM noroman ORDER BY [किआओ]");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testRightCaseQuery(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        Connection conn = ucanaccess;
        Statement st = conn.createStatement();
        assertEquals(st.executeQuery("SELECT * FROM Query1").getMetaData().getColumnLabel(1), "Ciao");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBadMetadata(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        dumpQueryResult("SELECT * FROM NOROMAN");
        Connection conn = ucanaccess;
        Statement st = conn.createStatement();
        assertThat(st.executeQuery("SELECT * FROM NOROMAN").getMetaData())
            .satisfies(x -> x.isAutoIncrement(1))
            .satisfies(x -> x.isCurrency(6))
            .satisfies(x -> x.isCurrency(7));
        DatabaseMetaData dbmd = ucanaccess.getMetaData();

        ResultSet rs = dbmd.getTables(null, null, "NOROMAn", null);
        getLogger().info("Noroman characters...");
        dumpQueryResult(rs);
        rs = dbmd.getColumns(null, null, "NOROMAn", null);
        dumpQueryResult(rs);
        rs = dbmd.getColumns(null, null, "%ROMAn", null);
        dumpQueryResult(rs);
        getLogger().info("getColumns...");
        rs = dbmd.getColumns(null, null, "Πλήθ%", null);
        dumpQueryResult(rs);
        rs = dbmd.getColumns(null, null, "%健康", null);
        dumpQueryResult(rs);
        getLogger().info("getColumns IS_GENERATEDCOLUMN...");
        rs = dbmd.getColumns(null, null, "TAbELLA1", "%e");
        dumpQueryResult(rs);
        getLogger().info("getColumnPrivileges...");
        rs = dbmd.getColumnPrivileges(null, null, "NOROMAn", null);
        dumpQueryResult(rs);
        // rs=dbmd.getColumnPrivileges(null, null, null, null);
        getLogger().info("getExportedKeys...");
        rs = dbmd.getExportedKeys(null, null, "??###");
        dumpQueryResult(rs);
        getLogger().info("getImportedKeys...");
        rs = dbmd.getImportedKeys(null, null, "Tabella1");
        dumpQueryResult(rs);
        getLogger().info("getPrimaryKeys...");
        rs = dbmd.getPrimaryKeys(null, null, "Tabella1");
        dumpQueryResult(rs);
        getLogger().info("getIndexInfo...");
        rs = dbmd.getIndexInfo(null, null, "Tabella1", false, false);
        dumpQueryResult(rs);
        getLogger().info("getCrossReference...");
        rs = dbmd.getCrossReference(null, null, "??###", null, null, "Tabella1");
        dumpQueryResult(rs);
        getLogger().info("getVersionColumns...");
        rs = dbmd.getVersionColumns(null, null, "Πλήθος");
        dumpQueryResult(rs);
        getLogger().info("getClientInfoProperties...");
        rs = dbmd.getClientInfoProperties();
        dumpQueryResult(rs);
        getLogger().info("getTablePrivileges...");
        rs = dbmd.getTablePrivileges(null, null, "??###");
        dumpQueryResult(rs);
        getLogger().info("getTables...");
        rs = dbmd.getTables(null, null, "??###", new String[] {"TABLE"});
        dumpQueryResult(rs);

        rs = dbmd.getTables(null, null, null, new String[] {"VIEW"});
        dumpQueryResult(rs);
        getLogger().info("getBestRowIdentifier...");
        rs = dbmd.getBestRowIdentifier(null, null, "??###", DatabaseMetaData.bestRowTemporary, true);
        dumpQueryResult(rs);
        rs = dbmd.getBestRowIdentifier(null, null, "??###", DatabaseMetaData.bestRowSession, true);
        dumpQueryResult(rs);
        getLogger().info("getTypesInfo...");
        rs = dbmd.getTypeInfo();
        dumpQueryResult(rs);
    }
}
