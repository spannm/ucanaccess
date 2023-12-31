package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import net.ucanaccess.util.UcanaccessRuntimeException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

class ComplexTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "complex.accdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2010"})
    void testComplex(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        complex0();
        complex1();
    }

    private void complex0() throws SQLException {
        try (PreparedStatement ps = ucanaccess.prepareStatement("SELECT COUNT(*) FROM t_complex WHERE contains([multi-value-data], ?)")) {
            ps.setObject(1, SingleValue.multipleValue("value1", "value2"));
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertEquals(2, rs.getInt(1));
            ps.setObject(1, new SingleValue("value1"));
            rs = ps.executeQuery();
            rs.next();
            assertEquals(3, rs.getInt(1));
        }

        try (PreparedStatement ps = ucanaccess.prepareStatement("SELECT COUNT(*) FROM t_complex WHERE EQUALS([multi-value-data], ?)")) {
            ps.setObject(1, SingleValue.multipleValue("value4", "value1"));
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertEquals(0, rs.getInt(1));
            ps.setObject(1, SingleValue.multipleValue("value1", "value4"));
            rs = ps.executeQuery();
            rs.next();
            assertEquals(1, rs.getInt(1));
        }

        try (PreparedStatement ps = ucanaccess.prepareStatement("SELECT COUNT(*) FROM t_complex WHERE EQUALSIGNOREORDER([multi-value-data],?)")) {
            ps.setObject(1, SingleValue.multipleValue("value4", "value1"));
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }

    private void complex1() throws SQLException {
        dumpQueryResult("SELECT * FROM t_complex ORDER BY id");
        checkQuery("SELECT * FROM t_complex ORDER BY id");

        try (PreparedStatement ps = ucanaccess.prepareStatement(
            "INSERT INTO t_complex(id, [memo-data], [append-memo-data], [multi-value-data], [attach-data]) "
                + "VALUES (?,?,?,?,?)")) {
            ps.setString(1, "row12");
            ps.setString(2, "ciao");
            ps.setString(3, "to version");
            ps.setObject(4, new SingleValue[] {new SingleValue("ccc16"), new SingleValue("ccc24")});
            LocalDateTime now = LocalDateTime.now();
            Attachment[] atcs = new Attachment[] {
                new Attachment(null, "ccc.txt", "txt", "ddddd ddd".getBytes(), now, null),
                new Attachment(null, "ccczz.txt", "txt", "ddddd zzddd".getBytes(), now, null)};
            ps.setObject(5, atcs);
            ps.execute();
            dumpQueryResult("SELECT * FROM t_complex ORDER BY id");
            checkQuery("SELECT * FROM t_complex ORDER BY id");
        }

        try (PreparedStatement ps = ucanaccess.prepareStatement("UPDATE t_complex SET [append-memo-data]='THE CAT'")) {
            ps.execute();
        }

        Attachment[] atc = new Attachment[] {new Attachment(null, "cccsss.cvs", "cvs",
            "ddddd ;sssssssssssssssssssddd".getBytes(), LocalDateTime.now(), null)};
        try (PreparedStatement ps = ucanaccess.prepareStatement("UPDATE t_complex SET [attach-data]=? WHERE id=?")) {
            ps.setObject(1, atc);
            ps.setString(2, "row12");
            ps.execute();
        }

        try (PreparedStatement ps = ucanaccess.prepareStatement("SELECT COUNT(*) FROM t_complex where EQUALS([attach-data],?) ")) {
            ps.setObject(1, atc);
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertEquals(1, rs.getInt(1));
        }

        try (PreparedStatement ps = ucanaccess.prepareStatement("UPDATE t_complex SET [multi-value-data]=?")) {
            SingleValue[] svs = new SingleValue[] {new SingleValue("aaaaaaa14"), new SingleValue("2eeeeeeeeeee")};
            ps.setObject(1, svs);
            ps.execute();
            checkQuery("SELECT * FROM t_complex ORDER BY id");
            assertEquals(7, getVerifyCount("SELECT COUNT(*) FROM t_complex"));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2010"})
    void testComplexRollback(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        ucanaccess = Mockito.spy(ucanaccess);
        Mockito.doThrow(new UcanaccessRuntimeException(getTestMethodName()))
            .when(ucanaccess).afterFlushIoHook();

        ucanaccess.setAutoCommit(false);

        final int countBefore = getVerifyCount("SELECT COUNT(*) FROM t_complex");

        try (PreparedStatement ps = ucanaccess.prepareStatement(
            "INSERT INTO t_complex(id, [memo-data], [append-memo-data], [multi-value-data], [attach-data]) VALUES (?,?,?,?,?)")) {

            ps.setString(1, "row123");
            ps.setString(2, "ciao");
            ps.setString(3, "to version");
            SingleValue[] svs = new SingleValue[] {new SingleValue("16"), new SingleValue("24")};
            ps.setObject(4, svs);
            Attachment[] atcs = new Attachment[] {
                new Attachment(null, "ccc.txt", "txt", "ddddd ddd".getBytes(), LocalDateTime.now(), null),
                new Attachment(null, "ccczz.txt", "txt", "ddddd zzddd".getBytes(), LocalDateTime.now(), null)};
            ps.setObject(5, atcs);
            ps.execute();
        }

        try (PreparedStatement ps = ucanaccess.prepareStatement("UPDATE t_complex SET [append-memo-data]='THE BIG BIG CAT' WHERE id='row12'")) {
            ps.execute();
        }
        dumpQueryResult("SELECT * FROM t_complex");

        assertThatThrownBy(ucanaccess::commit)
            .isInstanceOf(UcanaccessSQLException.class)
            .hasMessageEndingWith("testComplexRollback");

        ucanaccess = createUcanaccessConnection();
        checkQuery("SELECT * FROM t_complex ORDER BY id");
        dumpQueryResult("SELECT * FROM t_complex");
        checkQuery("SELECT * FROM t_complex WHERE id='row12' ORDER BY id");
        assertEquals(countBefore, getVerifyCount("SELECT COUNT(*) FROM t_complex"));
    }

}
