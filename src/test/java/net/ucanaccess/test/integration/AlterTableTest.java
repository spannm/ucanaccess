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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Index.Column;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import net.ucanaccess.util.HibernateSupport;

@RunWith(Parameterized.class)
public class AlterTableTest extends AccessVersionAllTest {

    public AlterTableTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/badDB.accdb";
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4))",
                "CREATE TABLE [AAA n] ( baaaa TEXT(3) ,A INTEGER , C TEXT(4), b yesNo, d datetime default now(), e numeric(8,3),[f f]TEXT ) ");
    }

    @Test
    public void testRename() throws SQLException, IOException {
        Statement st;

        st = ucanaccess.createStatement();
        st.execute("ALTER TABLE [??###] RENAME TO [1GIà GIà]");
        boolean b = false;
        try {
            st.execute("ALTER TABLE T4 RENAME TO [1GIà GIà]");
        } catch (SQLException e) {
            b = true;
        }
        assertTrue(b);
        checkQuery("SELECT * from [1GIà GIà]");
        dumpQueryResult("SELECT * from [1GIà GIà]");
        getLogger().info("After having renamed a few tables ...");
        dumpQueryResult("SELECT * from UCA_METADATA.TABLES");
        st.close();
    }

    @Test
    public void testAddColumn() throws SQLException, IOException {
        Statement st = null;
        st = ucanaccess.createStatement();
        st.execute("ALTER TABLE AAAn RENAME TO [GIà GIà]");
        st.execute("Insert into [GIà GIà] (baaaa) values('chi')");
        checkQuery("SELECT * from [GIà GIà] ORDER BY c");
        dumpQueryResult("SELECT * from [GIà GIà] ORDER BY c");
        st.execute("ALTER TABLE [GIà GIà] RENAME TO [22 amadeimargmail111]");
        checkQuery("SELECT * from [22 amadeimargmail111] ORDER BY c");
        dumpQueryResult("SELECT * from [22 amadeimargmail111] ORDER BY c");
        st.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [ci ci]  TEXT(100) NOT NULL DEFAULT 'PIPPO'  ");
        st.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [健康] decimal (23,5) ");
        st.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [£健康] numeric (23,6) default 13.031955 not null");
        boolean b = false;
        try {
            st.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN defaultwwwdefault numeric (23,6) not null");
        } catch (UcanaccessSQLException e) {
            b = true;
            System.err.println(e.getMessage());
        }
        assertTrue(b);
        st.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [ci ci1]  DATETIME NOT NULL DEFAULT now() ");
        st.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [ci ci2]  YESNO  ");
        st.execute("Insert into [22 amadeimargmail111] (baaaa) values('cha')");
        st.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN Memo  Memo  ");
        st.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN ole  OLE  ");
        checkQuery("SELECT * from [22 amadeimargmail111] ORDER BY c");
        dumpQueryResult("SELECT * from [22 amadeimargmail111] ORDER BY c");
        st.executeUpdate("Update sample set Description='wRRRw'");
        st.execute("ALTER TABLE Sample ADD COLUMN dt datetime default now()  ");

        st.execute("Update sample set Description='ww'");
        checkQuery("SELECT * from Sample");
        dumpQueryResult("SELECT * from Sample");

        getLogger().info("After having added a few columns...");
        dumpQueryResult("SELECT * from UCA_METADATA.Columns");

        createFK();

        st.execute("ALTER TABLE Sample ADD COLUMN website HYPERLINK");
        ResultSet rs = ucanaccess.getMetaData().getColumns(null, null, "Sample", "website");
        rs.next();
        assertEquals("HYPERLINK", rs.getString("ORIGINAL_TYPE"));
        st.close();

    }

    @Test
    public void testCreateIndex() throws SQLException, IOException {
        Statement st = null;
        st = ucanaccess.createStatement();
        st.execute("CREATE unique INDEX [èèè 23] on [AAA n]  (a ASC,c ASC )");
        boolean b = false;
        try {
            st.execute("INSERT INT0 [AAA n]  (a,C ) values (24,'su')");
            st.execute("INSERT INT0 [AAA n]  (a,C ) values (24,'su')");
        } catch (Exception e) {
            b = true;
        }
        assertTrue(b);
        Database db = ucanaccess.getDbIO();
        Table tb = db.getTable("AAA n");

        boolean found = false;
        for (Index idx : tb.getIndexes()) {
            if ("èèè 23".equals(idx.getName()) && idx.isUnique()) {
                found = true;
                ArrayList<String> ar = new ArrayList<String>();
                for (Column cl : idx.getColumns()) {
                    ar.add(cl.getName());
                }
                assertTrue(ar.contains("A"));
                assertTrue(ar.contains("C"));
            }
        }
        assertTrue(found);
        found = false;
        st.execute("CREATE  INDEX [健 康] on [AAA n]  (c DESC )");
        for (Index idx : tb.getIndexes()) {
            if ("健 康".equals(idx.getName()) && !idx.isUnique()) {
                found = true;
                assertTrue(idx.getColumns().get(0).getName().equals("C"));

            }
        }

        st.execute("CREATE  INDEX [%健 康] on [AAA n]  (b,d,e )");
        for (Index idx : tb.getIndexes()) {
            if ("%健 康".equals(idx.getName()) && !idx.isUnique()) {
                found = true;
                ArrayList<String> ar = new ArrayList<String>();
                for (Column cl : idx.getColumns()) {
                    ar.add(cl.getName());
                }
                assertTrue(ar.size() == 3);
                assertTrue(ar.contains("b"));
                assertTrue(ar.contains("d"));
                assertTrue(ar.contains("e"));

            }
        }

        st.execute("CREATE  INDEX ciao on Sample  (description)");
        for (Index idx : tb.getIndexes()) {
            if ("ciao".equals(idx.getName()) && !idx.isUnique()) {
                found = true;
                ArrayList<String> ar = new ArrayList<String>();
                for (Column cl : idx.getColumns()) {
                    ar.add(cl.getName());
                }
                assertTrue(ar.size() == 1);
                assertTrue(ar.contains("field"));

            }
        }

        assertTrue(found);
        st.close();
    }

    @Test
    public void testCreatePK() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();

        st.execute("ALTER TABLE [AAA n] add  Primary key (baaaa,a)");
        Database db = ucanaccess.getDbIO();
        Table tb = db.getTable("AAA n");
        Index idx = tb.getPrimaryKeyIndex();
        ArrayList<String> ar = new ArrayList<String>();
        for (Column cl : idx.getColumns()) {
            ar.add(cl.getName());
        }
        assertTrue(ar.contains("A"));
        assertTrue(ar.contains("baaaa"));

        st.execute("ALTER TABLE Sample add  Primary key (RegionId)");
        tb = db.getTable("Sample");
        idx = tb.getPrimaryKeyIndex();
        ar.clear();
        for (Column cl : idx.getColumns()) {
            ar.add(cl.getName());
        }
        assertTrue(ar.contains("RegionId"));

        st.close();
    }

    private void createFK() throws SQLException, IOException {
        Statement st = null;
        st = ucanaccess.createStatement();

        // test case: constraint name specified
        st.execute(
                "ALTER TABLE [AAA n] add constraint [pippo1] foreign key (c) references [22 amadeimargmail111] (baaaa) ON delete cascade");
        Database db = ucanaccess.getDbIO();
        Table tb = db.getTable("AAA n");
        Table tbr = db.getTable("22 amadeimargmail111");
        Index idx = tb.getForeignKeyIndex(tbr);
        ArrayList<String> ar = new ArrayList<String>();
        for (Column cl : idx.getColumns()) {
            ar.add(cl.getName());
        }
        assertTrue(ar.contains("C"));
        //
        // also verify that the Relationship name was actually used in the Access database ...
        tb = db.getSystemTable("MSysRelationships");
        IndexCursor crsr = CursorBuilder.createCursor(tb.getIndex("szRelationship"));
        Row r = crsr.findRowByEntry("pippo1");
        assertTrue(r != null);
        // ... and the right name was used in the HSQLDB database
        Connection hsqldbConn = ucanaccess.getHSQLDBConnection();
        Statement hsqldbStmt = hsqldbConn.createStatement();
        ResultSet hsqldbRs =
                hsqldbStmt.executeQuery("SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
                        + "WHERE CONSTRAINT_TYPE='FOREIGN KEY' AND TABLE_NAME='AAA N'");
        hsqldbRs.next();
        assertEquals("AAA N_PIPPO1", hsqldbRs.getString(1));
        //
        // now test dropping the foreign key
        // first try with Hibernate mode inactive
        HibernateSupport.setActive(false);
        try {
            st.execute("ALTER TABLE [AAA n] DROP CONSTRAINT [pippo1]");
            org.junit.Assert.fail("UcanaccessSQLException should have been thrown");
        } catch (UcanaccessSQLException ucaSqlEx) {
        }
        // now try again with Hibernate mode active
        HibernateSupport.setActive(true);
        st.execute("ALTER TABLE [AAA n] DROP CONSTRAINT [pippo1]");
        // and verify that it actually got dropped
        try {
            st.execute("ALTER TABLE [AAA n] DROP CONSTRAINT [pippo1]"); // again
            org.junit.Assert.fail("UcanaccessSQLException should have been thrown");
        } catch (UcanaccessSQLException ucaSqlEx) {
        }
        HibernateSupport.setActive(null);

        // test case: constraint name not specified
        st.execute("ALTER TABLE Son add foreign key (integer, txt) references Father(id,txt) ON delete cascade");
        st.close();
    }

    @Test
    public void testMiscellaneous() throws SQLException, IOException {
        Statement st = null;
        st = ucanaccess.createStatement();
        st.execute("ALTER TABLE tx add constraint pk primary key ([i d]) ");
        st.execute("ALTER TABLE tx add column [my best friend] long ");
        st.execute("ALTER TABLE tx add column [my worst friend] single ");
        st.execute("ALTER TABLE tx add column  [Is Pippo] TEXT(100) ");
        st.execute("ALTER TABLE tx add column  [Is not Pippo]TEXT default \"what's this?\"");

        st.execute("create TABLE tx1  (n1 long, [n 2] text)");
        st.execute("ALTER TABLE tx1 add primary key (n1, [n 2])");
        st.execute(
                "ALTER TABLE tx add  foreign key ([my best friend],[Is Pippo])references tx1(n1, [n 2])ON delete cascade");
        st.execute("INSERT INTO tx1 values(1,\"ciao\")");
        st.execute("INSERT INTO tx ([my best friend], [my worst friend], [Is Pippo]) values(1,2,\"ciao\")");
        checkQuery("SELECT count(*) from tx", 1);
        st.execute("delete from tx1");
        checkQuery("SELECT count(*) from tx");
        checkQuery("SELECT count(*) from tx", 0);
        st.execute("DROP TABLE tx ");
        st.execute("DROP TABLE tx1  ");

        st.execute(
                "CREATE TABLE tx (id counter primary key, [my best friend]long , [my worst friend] single,[Is Pippo] TEXT(100) ,[Is not Pippo]TEXT default \"what's this?\" )");
        st.execute("create TABLE tx1  (n1 long, [n 2] text)");
        st.execute("ALTER TABLE tx1 add primary key (n1, [n 2])");
        st.execute(
                "ALTER TABLE tx add  foreign key ([my best friend],[Is Pippo])references tx1(n1, [n 2])ON delete set null");
        st.execute("INSERT INTO tx1 values(1,\"ciao\")");
        st.execute("INSERT INTO tx ([my best friend], [my worst friend], [Is Pippo]) values(1,2,\"ciao\")");
        checkQuery("SELECT count(*) from tx", 1);
        st.execute("delete from tx1");
        checkQuery("SELECT count(*) from tx", 1);
        checkQuery("SELECT * from tx", 1, null, 2.0, null, "what's this?");
        st.execute("CREATE  UNIQUE  INDEX IDX111 ON tx ([my best friend])");

        boolean b = false;
        try {
            st.execute("INSERT INTO tx ([my best friend], [my worst friend], [Is Pippo]) values(1,2,\"ciao\")");
        } catch (UcanaccessSQLException e) {
            b = true;
            System.err.println(e.getMessage());
        }
        assertTrue(b);
        st.close();
    }

    private void executeErr(String _ddl, String _expectedMessage) throws SQLException {
        Statement st = null;
        try {
            st = ucanaccess.createStatement();
            st.execute(_ddl);
        } catch (SQLException _ex) {
            assertTrue(_ex.getMessage().endsWith(_expectedMessage));
            return;
        } finally {
            st.close();
        }
        fail("Should have encountered error: " + _expectedMessage);
    }

    @Test
    public void testSqlErrors() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.execute(
                "CREATE TABLE tx2 (id counter , [my best friend]long , [my worst friend] single,[Is Pippo] TEXT(100) ,[Is not Pippo]TEXT default \"what's this?\" )");
        st.execute("INSERT INTO tx2 ([my best friend], [my worst friend], [Is Pippo]) values(1,2,\"ciao\")");
        executeErr("ALTER TABLE tx2 add constraint primary key ([i d]) ", "unexpected token: PRIMARY");
        executeErr("ALTER TABLE tx2 add column [my best friend]  ", "unexpected end of statement");
        executeErr(
                "ALTER TABLE tx2 add constraint foreign key ([my best friend],[Is Pippo])references tx1(n1, [n 2])ON delete cascade",
                "type not found or user lacks privilege: FOREIGN");
        executeErr("DROP TABLE tx2 cascade", "Feature not supported yet.");
        executeErr("ALTER TABLE tx2 add constraint primary key (id)", "unexpected token: PRIMARY");
        executeErr("ALTER TABLE tx2 ALTER COLUMN [my best friend] SET DEFAULT 33", "Feature not supported yet.");
        executeErr("ALTER TABLE tx2 drop COLUMN [my best friend]", "Feature not supported yet.");
        executeErr("ALTER TABLE tx2 add COLUMN [1 my best friend]lonG not null",
                "x2 already contains one or more records(1 records)");
        st.close();
    }

}
