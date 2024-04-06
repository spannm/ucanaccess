package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessDefaultVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.lang.System.Logger.Level;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

class WorkloadTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE AAAB (id COUNTER PRIMARY KEY, A LONG, C TEXT, D TEXT)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessDefaultVersionSource
    void testLoadMany(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        final int nbRecords = 30000;
        ucanaccess.setAutoCommit(false);

        long startTime = System.currentTimeMillis();

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            for (int i = 0; i <= nbRecords; i++) {
                st.execute("INSERT INTO AAAB(id,a,c,d) VALUES (" + i + ",'33', 'booo', 'ddddddddddddddddddddd')");
            }
        }

        ucanaccess.commit();

        long midTime = System.currentTimeMillis();
        getLogger().log(Level.INFO, "Autoincrement insert performance test, {0} records inserted in {1} seconds.", nbRecords,
                TimeUnit.MILLISECONDS.toSeconds(midTime - startTime));

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.executeUpdate("update aaAB set c='yessssss'&a");
            ucanaccess.commit();
            getLogger().log(Level.INFO, "Update performance test, all {0} table records updated in {1} seconds.", nbRecords,
                    TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - midTime));
        }
    }

}
