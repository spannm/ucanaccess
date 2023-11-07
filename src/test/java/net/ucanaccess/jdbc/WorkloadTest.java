package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

class WorkloadTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE AAAB ( id COUNTER PRIMARY KEY,A LONG , C TEXT,D TEXT) ");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable("AAAB");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.AccessVersion#getDefaultAccessVersion()")
    void testLoadMany(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
        final int nbRecords = 30000;
        ucanaccess.setAutoCommit(false);
        Statement st = ucanaccess.createStatement();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i <= nbRecords; i++) {
            st.execute("INSERT INTO AAAB(id,a,c,d) VALUES (" + i + ",'33','booo','ddddddddddddddddddddd' )");
        }
        ucanaccess.commit();
        long time1 = System.currentTimeMillis();
        getLogger().info("Autoincrement insert performance test, {} records inserted in {} seconds.", nbRecords,
                TimeUnit.MILLISECONDS.toSeconds(time1 - startTime));
        st = ucanaccess.createStatement();
        st.executeUpdate("update aaAB set c='yessssss'&a");
        ucanaccess.commit();
        long time2 = System.currentTimeMillis();
        getLogger().info("Update performance test, all {} table records updated in {} seconds.", nbRecords,
                TimeUnit.MILLISECONDS.toSeconds(time2 - time1));

        st.close();
    }

}
