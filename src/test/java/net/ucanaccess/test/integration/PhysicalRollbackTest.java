package net.ucanaccess.test.integration;

import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class PhysicalRollbackTest extends AccessVersionDefaultTest {

    public PhysicalRollbackTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        // this db does not exist!
        return getClass().getSimpleName() + fileFormat.getFileExtension();
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE T4 (id LONG, descr VARCHAR(400))");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable("T4");
    }

    @Test
    public void testCommit() throws Exception {
        ucanaccess.setAutoCommit(false);

        Method mth = UcanaccessConnection.class.getDeclaredMethod("setTestRollback", boolean.class);
        mth.setAccessible(true);
        mth.invoke(ucanaccess, Boolean.TRUE);
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO T4 (id,descr) VALUES( 6666554,  'nel mezzo del cammin di nostra vita')");
        st.execute("INSERT INTO T4 (id,descr) VALUES( 77666554, 'nel mezzo del cammin di nostra vita')");
        st.execute("UPDATE T4 SET ID=0 where id=77666554");

        st.execute("INSERT INTO T4 (id,descr) VALUES( 4,'nel mezzo del cammin di nostra vita')");

        st.execute("DELETE FROM T4 WHERE id=4");
        Exception ex = null;
        try {
            ucanaccess.commit();
        } catch (Exception _ex) {
            ex = _ex;
        }
        assertNotNull(ex);
        assertContains(ex.getMessage(), getClass().getSimpleName());

        ucanaccess = getUcanaccessConnection();
        dumpQueryResult("SELECT * FROM T4");

        assertEquals(0, getCount("SELECT COUNT(*) FROM T4", true));
    }

}
