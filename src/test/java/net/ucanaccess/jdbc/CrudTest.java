package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class CrudTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_crud (id LONG, descr TEXT)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCrud(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            int id1 = 1234;
            int id2 = 5678;

            st.execute("DELETE FROM t_crud");
            st.execute("INSERT INTO t_crud (id, descr) VALUES(" + id1 + ", 'nel mezzo del cammin di nostra vita')");
            assertEquals(1, getVerifyCount("SELECT COUNT(*) FROM t_crud"), "Insert failed");
            st.executeUpdate("UPDATE t_crud SET id=" + id2 + " WHERE id=" + id1);
            assertEquals(1, getVerifyCount("SELECT COUNT(*) FROM t_crud where id=" + id2), "Update failed");
            st.executeUpdate("DELETE FROM t_crud WHERE id=" + id2);
            assertEquals(0, getVerifyCount("SELECT COUNT(*) FROM t_crud where id=" + id2), "Delete failed");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCrudPS(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        int id1 = 1234;
        int id2 = 5678;

        try (UcanaccessStatement st = ucanaccess.createStatement();
             PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_crud (id, descr) VALUES(?, ?)")) {
            st.execute("DELETE FROM t_crud");
            ps.setInt(1, id1);
            ps.setString(2, "Prep1");
            ps.execute();
            assertEquals(1, getVerifyCount("SELECT COUNT(*) FROM t_crud"), "Insert failed");
        }

        try (PreparedStatement ps2 = ucanaccess.prepareStatement("UPDATE t_crud SET id=? WHERE id=?")) {
            ps2.setInt(1, id2);
            ps2.setInt(2, id1);
            ps2.executeUpdate();
            assertEquals(1, getVerifyCount("SELECT COUNT(*) FROM t_crud where id=" + id2), "Update failed");
        }

        try (PreparedStatement ps3 = ucanaccess.prepareStatement("DELETE * FROM t_crud WHERE id=?")) {
            ps3.setInt(1, id2);
            ps3.executeUpdate();
            assertEquals(0, getVerifyCount("SELECT COUNT(*) FROM t_crud WHERE id=" + id2), "Delete failed");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCrudPSBatch(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        int id1 = 1234;
        int id2 = 5678;
        try (PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_crud (id, descr) VALUES(?, ?)")) {
            ps.setInt(1, id1);
            ps.setString(2, "Prep1");
            ps.addBatch();
            ps.setInt(1, id2);
            ps.setString(2, "Prep2");
            ps.addBatch();
            ps.executeBatch();
            ps.clearBatch();
        }
        checkQuery("SELECT * FROM t_crud", recs(rec(id1, "Prep1"), rec(id2, "Prep2")));
        assertEquals(2, getVerifyCount("SELECT COUNT(*) FROM t_crud where id in (" + id1 + ", " + id2 + ")"), "Insert failed");

        try (PreparedStatement ps = ucanaccess.prepareStatement("DELETE FROM t_crud")) {
            ps.addBatch();
            ps.executeBatch();
        }
        assertEquals(0, getVerifyCount("SELECT COUNT(*) FROM t_crud"), "Delete failed");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testUpdatableRS(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            int id = 6666554;
            st.execute("DELETE FROM t_crud");
            st.execute("INSERT INTO t_crud (id,descr) VALUES( " + id + ", 'tre canarini volano su e cadono')");
            PreparedStatement ps = ucanaccess.prepareStatement("SELECT * FROM t_crud", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.updateString(2, "show must go off");
            rs.updateRow();
            checkQuery("SELECT * FROM t_crud", singleRec(6666554, "show must go off"));
            st.execute("DELETE FROM t_crud");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testDeleteRS(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        ucanaccess.setAutoCommit(false);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            int id = 6666554;
            st.execute("DELETE FROM t_crud");
            st.execute("INSERT INTO t_crud (id, descr) VALUES(" + id + ", 'tre canarini volano su e cadono')");
        }
        PreparedStatement ps = ucanaccess.prepareStatement("SELECT * FROM t_crud", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        ResultSet rs = ps.executeQuery();
        rs.next();

        rs.deleteRow();
        ps.getConnection().commit();
        checkQuery("SELECT COUNT(*) FROM t_crud", singleRec(0));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testInsertRS(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        ucanaccess.setAutoCommit(false);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            int id = 6666554;
            st.execute("DELETE FROM t_crud");
            st.execute("INSERT INTO t_crud (id, descr) VALUES(" + id + ", 'tre canarini volano su e cadono')");
            PreparedStatement ps = ucanaccess.prepareStatement("SELECT * FROM t_crud", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            ResultSet rs = ps.executeQuery();
            rs.moveToInsertRow();
            rs.updateInt(1, 4);
            rs.updateString(2, "Growing old in rural pleaces");

            rs.insertRow();
            ps.getConnection().commit();
            checkQuery("SELECT * FROM t_crud ORDER BY id",
                recs(rec(4, "Growing old in rural pleaces"), rec(6666554, "tre canarini volano su e cadono")));
            st.execute("DELETE FROM t_crud");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testInsertRSNoAllSet(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        ucanaccess.setAutoCommit(false);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE t_crud2 (id AUTOINCREMENT, descr TEXT)");
            st.execute("DELETE FROM t_crud2");

            PreparedStatement ps = ucanaccess.prepareStatement("SELECT * FROM t_crud2", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            ResultSet rs = ps.executeQuery();
            rs.moveToInsertRow();
            rs.updateInt(1, 0);
            rs.updateString(2, "Growing old in rural places");

            rs.insertRow();
            rs = ps.getGeneratedKeys();
            rs.next();
            ps.getConnection().commit();

            checkQuery("SELECT * FROM t_crud2 ORDER BY id", singleRec(1, "Growing old in rural places"));
            UcanaccessStatement stat = ucanaccess.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs2 = stat.executeQuery("SELECT * FROM t_crud2 ORDER BY id");
            rs2.last();

            st.execute("DELETE FROM t_crud");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testPartialInsertRS(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        ucanaccess.setAutoCommit(false);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE t_crud3 (id autoincrement, descr TEXT)");

            PreparedStatement ps = ucanaccess.prepareStatement("SELECT * FROM t_crud3", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            ResultSet rs = ps.executeQuery();
            rs.moveToInsertRow();

            rs.updateString(2, "Growing old without emotions");

            rs.insertRow();
            ps.getConnection().commit();
            checkQuery("SELECT * FROM t_crud3 ORDER BY id", singleRec(1, "Growing old without emotions"));
            st.execute("DELETE FROM t_crud3");
        }
    }

}
