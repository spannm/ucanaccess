package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

class TransactionTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE T4 (id LONG,descr text(200)) ");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable("T4");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testCommit(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        ucanaccess.setAutoCommit(false);
        Statement st = ucanaccess.createStatement();
        int i = getCount("SELECT COUNT(*) FROM T4", true);
        st.execute("INSERT INTO T4 (id,descr) VALUES( 6666554,'nel mezzo del cammin di nostra vita')");
        assertEquals(i, getCount("SELECT COUNT(*) FROM T4", false));
        ucanaccess.commit();
        assertEquals(i + 1, getCount("SELECT COUNT(*) FROM T4", true));
        st.close();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testSavepoint(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        int count = getCount("SELECT COUNT(*) FROM T4");
        ucanaccess.setAutoCommit(false);
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO T4 (id,descr) VALUES( 1,'nel mezzo del cammin di nostra vita')");
        Savepoint sp = ucanaccess.setSavepoint();
        assertEquals(count, getCount("SELECT COUNT(*) FROM T4", false));
        st.execute("INSERT INTO T4 (id,descr) VALUES( 2,'nel mezzo del cammin di nostra vita')");
        ucanaccess.rollback(sp);
        ucanaccess.commit();
        assertEquals(count + 1, getCount("SELECT COUNT(*) FROM T4"));
        ucanaccess.setAutoCommit(false);
        st.close();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testSavepoint2(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);
        int count = getCount("SELECT COUNT(*) FROM T4");
        ucanaccess.setAutoCommit(false);
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO T4 (id,descr) VALUES( 1,'nel mezzo del cammin di nostra vita')");
        Savepoint sp = ucanaccess.setSavepoint("Gord svp");
        assertEquals(count, getCount("SELECT COUNT(*) FROM T4", false));
        st.execute("INSERT INTO T4 (id,descr) VALUES( 2,'nel mezzo del cammin di nostra vita')");
        ucanaccess.rollback(sp);
        ucanaccess.commit();
        assertEquals(count + 1, getCount("SELECT COUNT(*) FROM T4"));
        ucanaccess.setAutoCommit(false);
        st.close();

    }
}
