package net.ucanaccess.test.integration;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;

@RunWith(Parameterized.class)
@org.junit.Ignore
public class MemoryTest extends AccessVersionDefaultTest {

    public MemoryTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        getLogger().info("Thread.activeCount (setup): " + Thread.activeCount());

        setInactivityTimeout(1000L);
        executeStatements("CREATE TABLE memm( id LONG PRIMARY KEY,A LONG , C TEXT,D TEXT)");
    }

    @Test
    public void testMemory() throws SQLException, IOException, InterruptedException {
        ucanaccess.setAutoCommit(false);
        Statement st = null;
        st = ucanaccess.createStatement();

        getLogger().info("total memory 0={}", Runtime.getRuntime().totalMemory());
        getLogger().info("free memory 0={}", Runtime.getRuntime().freeMemory());
        int nbRecords = 100000;
        for (int i = 0; i <= nbRecords; i++) {
            st.execute("INSERT INTO memm(id,a,c,d) VALUES (" + i
                    + ",'33','booddddddddddddddddddddddddddddddo','dddddddddddddddddsssssssssssssssdddd' )");
        }
        ucanaccess.commit();

        long occ = Runtime.getRuntime().freeMemory();
        int ac = Thread.activeCount();
        getLogger().info("Thread.activeCount {}", Thread.activeCount());
        getLogger().info("total memory 1={}", Runtime.getRuntime().totalMemory());
        getLogger().info("free memory 1={}", occ);

        Thread.sleep(2000L);

        getLogger().info("Thread.activeCount() diff {}", (Thread.activeCount() - ac));
        getLogger().info("total memory 2={}", Runtime.getRuntime().totalMemory());
        getLogger().info("free memory 2={}", Runtime.getRuntime().freeMemory());
        getLogger().info("free memory diff = {}", (Runtime.getRuntime().freeMemory() - occ));

        dumpQueryResult("SELECT * FROM memm limit 10");
        getLogger().info("Thread.activeCount() diff {}", (Thread.activeCount() - ac));

        st.close();
    }

}
