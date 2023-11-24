package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;

@Disabled
class MemoryTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        getLogger().debug("Thread.activeCount (setup): " + Thread.activeCount());

        executeStatements("CREATE TABLE memm(id LONG PRIMARY KEY, A LONG, C TEXT, D TEXT)");
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
            getLogger().debug("Total memory 0={}", Runtime.getRuntime().totalMemory());
            getLogger().debug("Free memory 0={}", Runtime.getRuntime().freeMemory());
            int nbRecords = 100000;
            for (int i = 0; i <= nbRecords; i++) {
                st.execute("INSERT INTO memm(id,a,c,d) VALUES (" + i
                        + ",'33','booddddddddddddddddddddddddddddddo','dddddddddddddddddsssssssssssssssdddd' )");
            }
            ucanaccess.commit();

            long occ = Runtime.getRuntime().freeMemory();
            int ac = Thread.activeCount();
            getLogger().debug("Thread.activeCount {}", Thread.activeCount());
            getLogger().debug("total memory 1={}", Runtime.getRuntime().totalMemory());
            getLogger().debug("free memory 1={}", occ);

            Thread.sleep(2000L);

            getLogger().debug("Thread.activeCount() diff {}", Thread.activeCount() - ac);
            getLogger().debug("total memory 2={}", Runtime.getRuntime().totalMemory());
            getLogger().debug("free memory 2={}", Runtime.getRuntime().freeMemory());
            getLogger().debug("free memory diff = {}", Runtime.getRuntime().freeMemory() - occ);

            dumpQueryResult("SELECT * FROM memm limit 10");
            getLogger().info("Thread.activeCount() diff {}", Thread.activeCount() - ac);
        }
    }

}
