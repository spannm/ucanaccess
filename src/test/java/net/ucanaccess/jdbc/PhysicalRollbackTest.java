package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.UcanaccessRuntimeException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.sql.SQLException;

class PhysicalRollbackTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        // this db does not exist!
        return getClass().getSimpleName() + getFileFormat().getFileExtension();
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_pr (id LONG, descr VARCHAR(400))");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("net.ucanaccess.type.AccessVersion#getDefaultAccessVersion()")
    void testCommit(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        ucanaccess = Mockito.spy(ucanaccess);
        Mockito.doThrow(new UcanaccessRuntimeException(getTestMethodName()))
            .when(ucanaccess).afterFlushIoHook();

        ucanaccess.setAutoCommit(false);

        try (UcanaccessStatement st = ucanaccess.createStatement()) {
            st.execute("INSERT INTO t_pr (id, descr) VALUES(6666554, 'nel mezzo del cammin di nostra vita')");
            st.execute("INSERT INTO t_pr (id, descr) VALUES(77666554, 'nel mezzo del cammin di nostra vita')");
            st.execute("UPDATE t_pr SET ID=0 WHERE id=77666554");

            st.execute("INSERT INTO t_pr (id, descr) VALUES(4, 'nel mezzo del cammin di nostra vita')");

            st.execute("DELETE FROM t_pr WHERE id=4");
        }
        assertThatThrownBy(ucanaccess::commit)
            .isInstanceOf(SQLException.class)
            .hasMessageContaining(getClass().getSimpleName());

        ucanaccess = createUcanaccessConnection();
        dumpQueryResult("SELECT * FROM t_pr");

        assertEquals(0, getVerifyCount("SELECT COUNT(*) FROM t_pr"));
    }

}
