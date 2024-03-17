package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.SQLException;
import java.sql.Savepoint;

class TransactionTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE T4 (id LONG, descr TEXT(200))");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testCommit(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        ucanaccess.setAutoCommit(false);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            int count = getVerifyCount("SELECT COUNT(*) FROM T4");
            st.execute("INSERT INTO T4 (id, descr) VALUES(6666554, 'nel mezzo del cammin di nostra vita')");
            assertEquals(count, getVerifyCount("SELECT COUNT(*) FROM T4", false));
            ucanaccess.commit();
            assertEquals(count + 1, getVerifyCount("SELECT COUNT(*) FROM T4"));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testSavepoint(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        int count = getVerifyCount("SELECT COUNT(*) FROM T4");
        ucanaccess.setAutoCommit(false);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO T4 (id, descr) VALUES(1, 'nel mezzo del cammin di nostra vita')");
            Savepoint sp = ucanaccess.setSavepoint();
            assertEquals(count, getVerifyCount("SELECT COUNT(*) FROM T4", false));
            st.execute("INSERT INTO T4 (id, descr) VALUES(2, 'nel mezzo del cammin di nostra vita')");
            ucanaccess.rollback(sp);
            ucanaccess.commit();
            assertEquals(count + 1, getVerifyCount("SELECT COUNT(*) FROM T4"));
            ucanaccess.setAutoCommit(false);
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testSavepoint2(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        int count = getVerifyCount("SELECT COUNT(*) FROM T4");
        ucanaccess.setAutoCommit(false);
        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO T4 (id, descr) VALUES(1, 'nel mezzo del cammin di nostra vita')");
            Savepoint sp = ucanaccess.setSavepoint("Gord svp");
            assertEquals(count, getVerifyCount("SELECT COUNT(*) FROM T4", false));
            st.execute("INSERT INTO T4 (id, descr) VALUES(2, 'nel mezzo del cammin di nostra vita')");
            ucanaccess.rollback(sp);
            ucanaccess.commit();
            assertEquals(count + 1, getVerifyCount("SELECT COUNT(*) FROM T4"));
            ucanaccess.setAutoCommit(false);
        }
    }

}
