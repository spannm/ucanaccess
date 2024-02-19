package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.System.Logger.Level;
import java.sql.SQLException;

@Disabled
class MemoryTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        getLogger().log(Level.DEBUG, "Thread.activeCount (setup): {0}", Thread.activeCount());

        executeStatements("CREATE TABLE t_mem(id LONG PRIMARY KEY, a LONG, c TEXT, d TEXT)");
    }

    @Override
    protected UcanaccessConnectionBuilder buildConnection() {
        return super.buildConnection()
            .withInactivityTimeout(1000);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testMemory(AccessVersion _accessVersion) throws SQLException, InterruptedException {
        init(_accessVersion);
        ucanaccess.setAutoCommit(false);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            getLogger().log(Level.DEBUG, "Total memory 0= {0}", Runtime.getRuntime().totalMemory());
            getLogger().log(Level.DEBUG, "Free memory 0= {0}", Runtime.getRuntime().freeMemory());
            int nbRecords = 100000;
            for (int i = 0; i <= nbRecords; i++) {
                st.execute("INSERT INTO t_mem(id, a, c, d) VALUES ("
                    + i + ",'33','booddddddddddddddddddddddddddddddo','dddddddddddddddddsssssssssssssssdddd' )");
            }
            ucanaccess.commit();

            long occ = Runtime.getRuntime().freeMemory();
            int ac = Thread.activeCount();
            getLogger().log(Level.DEBUG, "Thread.activeCount {0}", Thread.activeCount());
            getLogger().log(Level.DEBUG, "total memory 1 = {0}", Runtime.getRuntime().totalMemory());
            getLogger().log(Level.DEBUG, "free memory 1 = {0}", occ);

            Thread.sleep(2000L);

            getLogger().log(Level.DEBUG, "Thread.activeCount() diff {0}", Thread.activeCount() - ac);
            getLogger().log(Level.DEBUG, "Total memory 2 = {0}", Runtime.getRuntime().totalMemory());
            getLogger().log(Level.DEBUG, "Tree memory 2 = {0}", Runtime.getRuntime().freeMemory());
            getLogger().log(Level.DEBUG, "Tree memory diff = {0}", Runtime.getRuntime().freeMemory() - occ);

            dumpQueryResult("SELECT * FROM t_mem LIMIT 10");
            getLogger().log(Level.INFO, "Thread.activeCount() diff {0}", Thread.activeCount() - ac);
        }
    }

}
