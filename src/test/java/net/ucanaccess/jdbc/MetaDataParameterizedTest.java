package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

class MetaDataParameterizedTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "badDb.accdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCreateBadMetadata(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        UcanaccessConnection conn = ucanaccess;
        UcanaccessStatement st = conn.createStatement();
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
        UcanaccessConnection conn = ucanaccess;
        UcanaccessStatement st = conn.createStatement();
        assertEquals("Ciao", st.executeQuery("SELECT * FROM Query1").getMetaData().getColumnLabel(1));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBadMetadata(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        dumpQueryResult("SELECT * FROM NOROMAN");
        UcanaccessStatement st = ucanaccess.createStatement();
        assertThat(st.executeQuery("SELECT * FROM NOROMAN").getMetaData())
            .satisfies(x -> assertThat(x.isAutoIncrement(1)).isTrue())
            .satisfies(x -> assertThat(x.isCurrency(6)).isTrue())
            .satisfies(x -> assertThat(x.isCurrency(7)).isFalse());
        DatabaseMetaData dbmd = ucanaccess.getMetaData();

        getLogger().info("Noroman characters:");
        dumpQueryResult(() -> dbmd.getTables(null, null, "NOROMAn", null));
        dumpQueryResult(() -> dbmd.getColumns(null, null, "NOROMAn", null));
        dumpQueryResult(() -> dbmd.getColumns(null, null, "%ROMAn", null));
        getLogger().info("getColumns:");
        dumpQueryResult(() -> dbmd.getColumns(null, null, "Πλήθ%", null));
        dumpQueryResult(() -> dbmd.getColumns(null, null, "%健康", null));
        dumpQueryResult(() -> dbmd.getColumns(null, null, "TAbELLA1", "%e"));
        getLogger().info("getColumnPrivileges:");
        dumpQueryResult(() -> dbmd.getColumnPrivileges(null, null, "NOROMAn", null));
        getLogger().info("getExportedKeys:");
        dumpQueryResult(() -> dbmd.getExportedKeys(null, null, "??###"));
        getLogger().info("getImportedKeys:");
        dumpQueryResult(() -> dbmd.getImportedKeys(null, null, "Tabella1"));
        getLogger().info("getPrimaryKeys:");
        dumpQueryResult(() -> dbmd.getPrimaryKeys(null, null, "Tabella1"));
        getLogger().info("getIndexInfo:");
        dumpQueryResult(() -> dbmd.getIndexInfo(null, null, "Tabella1", false, false));
        getLogger().info("getCrossReference:");
        dumpQueryResult(() -> dbmd.getCrossReference(null, null, "??###", null, null, "Tabella1"));
        getLogger().info("getVersionColumns:");
        dumpQueryResult(() -> dbmd.getVersionColumns(null, null, "Πλήθος"));
        getLogger().info("getClientInfoProperties:");
        dumpQueryResult(dbmd::getClientInfoProperties);
        getLogger().info("getTablePrivileges:");
        dumpQueryResult(() -> dbmd.getTablePrivileges(null, null, "??###"));
        getLogger().info("getTables:");
        dumpQueryResult(() -> dbmd.getTables(null, null, "??###", new String[] {"TABLE"}));
        dumpQueryResult(() -> dbmd.getTables(null, null, null, new String[] {"VIEW"}));
        getLogger().info("getBestRowIdentifier:");
        dumpQueryResult(() -> dbmd.getBestRowIdentifier(null, null, "??###", DatabaseMetaData.bestRowTemporary, true));
        dumpQueryResult(() -> dbmd.getBestRowIdentifier(null, null, "??###", DatabaseMetaData.bestRowSession, true));
        getLogger().info("getTypeInfo:");
        dumpQueryResult(dbmd::getTypeInfo);
    }
}
