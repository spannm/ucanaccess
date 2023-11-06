package net.ucanaccess.test.integration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;

class PhysicalRollbackTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        // this db does not exist!
        return getClass().getSimpleName() + getFileFormat().getFileExtension();
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE T4 (id LONG, descr VARCHAR(400))");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable("T4");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.test.util.AccessVersion#getDefaultAccessVersion()")
    void testCommit(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);
        ucanaccess.setAutoCommit(false);

        Method mth = UcanaccessConnection.class.getDeclaredMethod("setTestRollback", boolean.class);
        mth.setAccessible(true);
        mth.invoke(ucanaccess, Boolean.TRUE);
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO T4 (id, descr) VALUES(6666554, 'nel mezzo del cammin di nostra vita')");
        st.execute("INSERT INTO T4 (id, descr) VALUES(77666554, 'nel mezzo del cammin di nostra vita')");
        st.execute("UPDATE T4 SET ID=0 WHERE id=77666554");

        st.execute("INSERT INTO T4 (id, descr) VALUES(4, 'nel mezzo del cammin di nostra vita')");

        st.execute("DELETE FROM T4 WHERE id=4");

        assertThatThrownBy(() -> ucanaccess.commit())
            .isInstanceOf(SQLException.class)
            .hasMessageContaining(getClass().getSimpleName());

        ucanaccess = getUcanaccessConnection();
        dumpQueryResult("SELECT * FROM T4");

        assertEquals(0, getCount("SELECT COUNT(*) FROM T4"));
    }

}
