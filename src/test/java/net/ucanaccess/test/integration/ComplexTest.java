/*
Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package net.ucanaccess.test.integration;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2010Test;

@RunWith(Parameterized.class)
public class ComplexTest extends AccessVersion2010Test {

    public ComplexTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/2010.accdb"; // Access 2010
    }

    @Test
    public void testComplex() throws Exception {
        complex0();
        complex1();
    }

    private void complex0() throws SQLException, IOException, ParseException {
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
        dumpQueryResult("SELECT * FROM Table1");
        checkQuery("SELECT * FROM Table1");
        PreparedStatement ps =
                ucanaccess.prepareStatement(
                        "INSERT INTO TABLE1(ID  , [MEMO-DATA] , [APPEND-MEMO-DATA] , [MULTI-VALUE-DATA] , [ATTACH-DATA]) "
                                + "VALUES (?,?,?,?,?)");

        ps.setString(1, "row12");
        ps.setString(2, "ciao");
        ps.setString(3, "to version");
        SingleValue[] svs = new SingleValue[] { new SingleValue("ccc16"), new SingleValue("ccc24") };
        ps.setObject(4, svs);
        Attachment[] atcs =
                new Attachment[] { new Attachment(null, "ccc.txt", "txt", "ddddd ddd".getBytes(), new Date(), null),
                        new Attachment(null, "ccczz.txt", "txt", "ddddd zzddd".getBytes(), new Date(), null) };
        ps.setObject(5, atcs);
        ps.execute();
        dumpQueryResult("SELECT * FROM Table1");
        checkQuery("SELECT * FROM Table1");
        ps.close();
        ps = ucanaccess.prepareStatement("UPDATE TABLE1 SET [APPEND-MEMO-DATA]='THE CAT' ");
        ps.execute();
        ps.close();
        ps = ucanaccess.prepareStatement("UPDATE TABLE1 SET [ATTACH-DATA]=? WHERE ID=?");
        Attachment[] atc;
        ps.setObject(1, atc = new Attachment[] { new Attachment(null, "cccsss.cvs", "cvs",
                "ddddd ;sssssssssssssssssssddd".getBytes(), new Date(), null) });
        ps.setString(2, "row12");
        ps.execute();

        ps = ucanaccess.prepareStatement("SELECT COUNT(*) FROM Table1 where EQUALS([ATTACH-DATA],?) ");
        ps.setObject(1, atc);
        ResultSet rs = ps.executeQuery();
        rs.next();
        assertEquals(rs.getInt(1), 1);
        ps = ucanaccess.prepareStatement("UPDATE TABLE1 SET [MULTi-VALUE-DATA]=? ");
        svs = new SingleValue[] { new SingleValue("aaaaaaa14"), new SingleValue("2eeeeeeeeeee") };
        ps.setObject(1, svs);
        ps.execute();
        checkQuery("SELECT * FROM TABLE1 order by id");
        assertEquals(7, getCount("SELECT COUNT(*) FROM TABLE1", true));
        ps.close();
    }

    @Test
    public void testComplexRollback() throws SQLException, IOException, ParseException, SecurityException,
            NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        PreparedStatement ps = null;
        int i = getCount("SELECT COUNT(*) FROM TABLE1", true);
        try {

            ucanaccess.setAutoCommit(false);

            Method mth = UcanaccessConnection.class.getDeclaredMethod("setTestRollback", new Class[] { boolean.class });
            mth.setAccessible(true);
            mth.invoke(ucanaccess, new Object[] { Boolean.TRUE });
            ps = ucanaccess.prepareStatement(
                    "INSERT INTO TABLE1(ID  , [MEMO-DATA] , [APPEND-MEMO-DATA] , [MULTI-VALUE-DATA] , [ATTACH-DATA]) "
                            + "VALUES (?,?,?,?,?)");

            ps.setString(1, "row123");
            ps.setString(2, "ciao");
            ps.setString(3, "to version");
            SingleValue[] svs = new SingleValue[] { new SingleValue("16"), new SingleValue("24") };
            ps.setObject(4, svs);
            Attachment[] atcs =
                    new Attachment[] { new Attachment(null, "ccc.txt", "txt", "ddddd ddd".getBytes(), new Date(), null),
                            new Attachment(null, "ccczz.txt", "txt", "ddddd zzddd".getBytes(), new Date(), null) };
            ps.setObject(5, atcs);
            ps.execute();
            ps.close();
            ps = ucanaccess
                    .prepareStatement("UPDATE TABLE1 SET [APPEND-MEMO-DATA]='THE BIG BIG CAT' WHERE ID='row12' ");
            ps.execute();
            ps.close();
            dumpQueryResult("SELECT * FROM TABLE1");
            ucanaccess.commit();
            checkQuery("SELECT * FROM TABLE1 order by id");

        } catch (Throwable e) {
            getLogger().info("Encountered exception: " + e.getMessage());
        } finally {
            if (ps != null) {
                ps.close();
            }
        }

        this.ucanaccess = getUcanaccessConnection();
        dumpQueryResult("SELECT * FROM TABLE1");
        checkQuery("SELECT * FROM TABLE1  WHERE ID='row12' order by id");
        assertEquals(i, getCount("SELECT COUNT(*) FROM TABLE1", true));
    }
}
