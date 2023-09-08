package net.ucanaccess.test.integration;

import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

class ComplexTest extends UcanaccessTestBase {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "complex.accdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode=Mode.INCLUDE, names = {"V2010"})
    @Disabled("TODO: Fails with Java 11 under Ubuntu")
    void testComplex(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        complex0();
        complex1();
    }

    private void complex0() throws SQLException {
        PreparedStatement ps = null;
        ps = ucanaccess.prepareStatement("SELECT COUNT(*) FROM TABLE1 WHERE contains([MULTI-VALUE-DATA],?)");
        ps.setObject(1, SingleValue.multipleValue("value1", "value2"));
        ResultSet rs = ps.executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));
        ps.setObject(1, new SingleValue("value1"));
        rs = ps.executeQuery();
        rs.next();
        assertEquals(3, rs.getInt(1));
        ps.close();

        ps = ucanaccess.prepareStatement("SELECT COUNT(*) FROM TABLE1 WHERE EQUALS([MULTI-VALUE-DATA],?)");
        ps.setObject(1, SingleValue.multipleValue("value4", "value1"));
        rs = ps.executeQuery();
        rs.next();
        assertEquals(0, rs.getInt(1));
        ps.setObject(1, SingleValue.multipleValue("value1", "value4"));
        rs = ps.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        ps.close();

        ps = ucanaccess.prepareStatement("SELECT COUNT(*) FROM TABLE1 WHERE EQUALSIGNOREORDER([MULTI-VALUE-DATA],?)");
        ps.setObject(1, SingleValue.multipleValue("value4", "value1"));
        rs = ps.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        ps.close();
    }

    private void complex1() throws Exception {
        dumpQueryResult("SELECT * FROM Table1 ORDER BY id");
        checkQuery("SELECT * FROM Table1 ORDER BY id");
        PreparedStatement ps =
            ucanaccess.prepareStatement(
                "INSERT INTO TABLE1(ID, [MEMO-DATA], [APPEND-MEMO-DATA], [MULTI-VALUE-DATA], [ATTACH-DATA]) "
                    + "VALUES (?,?,?,?,?)");

        ps.setString(1, "row12");
        ps.setString(2, "ciao");
        ps.setString(3, "to version");
        SingleValue[] svs = new SingleValue[] {new SingleValue("ccc16"), new SingleValue("ccc24")};
        ps.setObject(4, svs);
        LocalDateTime now = LocalDateTime.now();
        Attachment[] atcs = new Attachment[] {
            new Attachment(null, "ccc.txt", "txt", "ddddd ddd".getBytes(), now, null),
            new Attachment(null, "ccczz.txt", "txt", "ddddd zzddd".getBytes(), now, null)};
        ps.setObject(5, atcs);
        ps.execute();
        dumpQueryResult("SELECT * FROM Table1 ORDER BY id");
        checkQuery("SELECT * FROM Table1 ORDER BY id");
        ps.close();
        ps = ucanaccess.prepareStatement("UPDATE TABLE1 SET [APPEND-MEMO-DATA]='THE CAT' ");
        ps.execute();
        ps.close();
        ps = ucanaccess.prepareStatement("UPDATE TABLE1 SET [ATTACH-DATA]=? WHERE ID=?");
        Attachment[] atc = new Attachment[] {new Attachment(null, "cccsss.cvs", "cvs",
            "ddddd ;sssssssssssssssssssddd".getBytes(), LocalDateTime.now(), null)};
        ps.setObject(1, atc);
        ps.setString(2, "row12");
        ps.execute();

        ps = ucanaccess.prepareStatement("SELECT COUNT(*) FROM Table1 where EQUALS([ATTACH-DATA],?) ");
        ps.setObject(1, atc);
        ResultSet rs = ps.executeQuery();
        rs.next();
        assertEquals(rs.getInt(1), 1);
        ps = ucanaccess.prepareStatement("UPDATE TABLE1 SET [MULTi-VALUE-DATA]=? ");
        svs = new SingleValue[] {new SingleValue("aaaaaaa14"), new SingleValue("2eeeeeeeeeee")};
        ps.setObject(1, svs);
        ps.execute();
        checkQuery("SELECT * FROM TABLE1 ORDER BY id");
        assertEquals(7, getCount("SELECT COUNT(*) FROM TABLE1", true));
        ps.close();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode=Mode.INCLUDE, names = {"V2010"})
    void testComplexRollback(AccessVersion _accessVersion) throws SQLException, IOException, SecurityException, IllegalArgumentException {
        init(_accessVersion);

        PreparedStatement ps = null;
        int i = getCount("SELECT COUNT(*) FROM TABLE1", true);
        try {

            ucanaccess.setAutoCommit(false);

            Method mth = UcanaccessConnection.class.getDeclaredMethod("setTestRollback", boolean.class);
            mth.setAccessible(true);
            mth.invoke(ucanaccess, Boolean.TRUE);
            ps = ucanaccess.prepareStatement(
                "INSERT INTO TABLE1(ID  , [MEMO-DATA] , [APPEND-MEMO-DATA] , [MULTI-VALUE-DATA] , [ATTACH-DATA]) "
                    + "VALUES (?,?,?,?,?)");

            ps.setString(1, "row123");
            ps.setString(2, "ciao");
            ps.setString(3, "to version");
            SingleValue[] svs = new SingleValue[] {new SingleValue("16"), new SingleValue("24")};
            ps.setObject(4, svs);
            Attachment[] atcs =
                new Attachment[] {new Attachment(null, "ccc.txt", "txt", "ddddd ddd".getBytes(), LocalDateTime.now(), null), new Attachment(null, "ccczz.txt", "txt", "ddddd zzddd".getBytes(),
                    LocalDateTime.now(), null)};
            ps.setObject(5, atcs);
            ps.execute();
            ps.close();
            ps = ucanaccess
                .prepareStatement("UPDATE TABLE1 SET [APPEND-MEMO-DATA]='THE BIG BIG CAT' WHERE ID='row12' ");
            ps.execute();
            ps.close();
            dumpQueryResult("SELECT * FROM TABLE1");
            ucanaccess.commit();
            checkQuery("SELECT * FROM TABLE1 ORDER BY id");

        } catch (Throwable _ex) {
            getLogger().warn("Encountered exception: {}: ", _ex.getMessage(), _ex);
        } finally {
            if (ps != null) {
                ps.close();
            }
        }

        ucanaccess = getUcanaccessConnection();
        dumpQueryResult("SELECT * FROM TABLE1");
        checkQuery("SELECT * FROM TABLE1  WHERE ID='row12' ORDER BY id");
        assertEquals(i, getCount("SELECT COUNT(*) FROM TABLE1", true));
    }
}
