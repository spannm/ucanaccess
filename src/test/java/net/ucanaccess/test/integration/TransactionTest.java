package net.ucanaccess.test.integration;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class TransactionTest extends AccessVersionAllTest {

    public TransactionTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE T4 (id LONG,descr text(200)) ");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("T4");
    }

    @Test
    public void testCommit() throws SQLException, IOException {
        ucanaccess.setAutoCommit(false);
        Statement st = ucanaccess.createStatement();
        int i = getCount("SELECT COUNT(*) FROM T4", true);
        st.execute("INSERT INTO T4 (id,descr)  VALUES( 6666554,'nel mezzo del cammin di nostra vita')");
        assertEquals(i, getCount("SELECT COUNT(*) FROM T4", false));
        ucanaccess.commit();
        assertEquals(i + 1, getCount("SELECT COUNT(*) FROM T4", true));
        st.close();
    }

    @Test
    public void testSavepoint() throws SQLException, IOException {
        int count = getCount("SELECT COUNT(*) FROM T4");
        ucanaccess.setAutoCommit(false);
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO T4 (id,descr)  VALUES( 1,'nel mezzo del cammin di nostra vita')");
        Savepoint sp = ucanaccess.setSavepoint();
        assertEquals(count, getCount("SELECT COUNT(*) FROM T4", false));
        st.execute("INSERT INTO T4 (id,descr)  VALUES( 2,'nel mezzo del cammin di nostra vita')");
        ucanaccess.rollback(sp);
        ucanaccess.commit();
        assertEquals(count + 1, getCount("SELECT COUNT(*) FROM T4"));
        ucanaccess.setAutoCommit(false);
        st.close();
    }

    @Test
    public void testSavepoint2() throws SQLException, IOException {
        int count = getCount("SELECT COUNT(*) FROM T4");
        ucanaccess.setAutoCommit(false);
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO T4 (id,descr)  VALUES( 1,'nel mezzo del cammin di nostra vita')");
        Savepoint sp = ucanaccess.setSavepoint("Gord svp");
        assertEquals(count, getCount("SELECT COUNT(*) FROM T4", false));
        st.execute("INSERT INTO T4 (id,descr)  VALUES( 2,'nel mezzo del cammin di nostra vita')");
        ucanaccess.rollback(sp);
        ucanaccess.commit();
        assertEquals(count + 1, getCount("SELECT COUNT(*) FROM T4"));
        ucanaccess.setAutoCommit(false);
        st.close();

    }
}
