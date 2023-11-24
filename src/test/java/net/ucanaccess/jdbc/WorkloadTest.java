package net.ucanaccess.jdbc;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

class WorkloadTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE AAAB (id COUNTER PRIMARY KEY, A LONG, C TEXT, D TEXT)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testLoadMany(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        final int nbRecords = 30000;
        ucanaccess.setAutoCommit(false);

        long startTime = System.currentTimeMillis();

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            for (int i = 0; i <= nbRecords; i++) {
                st.execute("INSERT INTO AAAB(id,a,c,d) VALUES (" + i + ",'33','booo','ddddddddddddddddddddd' )");
            }
        }

        ucanaccess.commit();

        long midTime = System.currentTimeMillis();
        getLogger().info("Autoincrement insert performance test, {} records inserted in {} seconds.", nbRecords,
                TimeUnit.MILLISECONDS.toSeconds(midTime - startTime));

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.executeUpdate("update aaAB set c='yessssss'&a");
            ucanaccess.commit();
            getLogger().info("Update performance test, all {} table records updated in {} seconds.", nbRecords,
                    TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - midTime));
        }
    }

}
