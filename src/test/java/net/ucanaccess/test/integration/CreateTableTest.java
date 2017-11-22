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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Index.Column;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Table;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;

@RunWith(Parameterized.class)
public class CreateTableTest extends AccessVersionAllTest {

    public CreateTableTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/badDB.accdb";
    }

    private void createAsSelect() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.executeUpdate("CREATE TABLE AAA_BIS as (SELECT baaaa,a,c FROM AAA) WITH DATA");
        Object[][] ver = { { "33A", 3, "G" }, { "33B", 111, "G" } };
        checkQuery("SELECT * FROM AAA_bis order by baaaa", ver);
        st.executeUpdate("CREATE TABLE AAA_quadris as (SELECT AAA.baaaa,AAA_BIS.baaaa as xxx FROM AAA,AAA_BIS) WITH DATA");
        dumpQueryResult("SELECT * FROM AAA_quadris order by baaaa");
        st.close();
    }

    private void createAsSelect2() throws SQLException, IOException {
        Statement st = null;
        try {
            st = ucanaccess.createStatement();
            st.executeUpdate("CREATE TABLE AAA_TRIS as (SELECT baaaa,a,c FROM AAA) WITH no DATA ");
            st.execute("INSERT INTO AAA_TRIS SELECT * from AAA_bis");
            Object[][] ver = { { "33A", 3, "G" }, { "33B", 111, "G" } };
            checkQuery("SELECT * FROM AAA_tris order by baaaa", ver);
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    private void createPs() throws SQLException, IOException {
        PreparedStatement ps = null;
        try {
            ps = ucanaccess.prepareStatement(" CREATE \nTABLE BBB ( baaaa \nvarchar(2) PRIMARY KEY)");
            ps.execute(" CREATE TABLE BBB ( baaaa text PRIMARY KEY,b text)");
            throw new RuntimeException("To block DDL with PreparedStatement");
        } catch (SQLException ex) {
            getLogger().info("ok");
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    private void createSimple() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO AAA(baaaa,c)   VALUES ('33A','G'     )");
        st.execute("INSERT INTO AAA(baaaa,a,c) VALUES ('33B',111,'G' )");
        Object[][] ver = { { "33A", 3, "G" }, { "33B", 111, "G" } };
        checkQuery("SELECT baaaa,a,c FROM AAA ORDER BY baaaa", ver);
        st.close();
    }

    public void defaults() throws Exception {
        Statement st = null;
        try {
            st = ucanaccess.createStatement();
            ResultSet rs = st.executeQuery("SELECT D, E FROM AAA");
            while (rs.next()) {
                assertNotNull(rs.getObject(1));
                assertNotNull(rs.getObject(2));
            }
            Database db = ucanaccess.getDbIO();
            Table tb = db.getTable("AAA");
            PropertyMap pm = tb.getColumn("d").getProperties();
            assertEquals("now()", pm.getValue(PropertyMap.DEFAULT_VALUE_PROP));
            PropertyMap pm1 = tb.getColumn("a").getProperties();
            assertEquals(true, pm1.getValue(PropertyMap.REQUIRED_PROP));
            tb = db.getTable("TBL");
            pm = tb.getColumn("NUMBER").getProperties();
            assertEquals("-4.6", pm.getValue(PropertyMap.DEFAULT_VALUE_PROP));
            assertEquals(true, pm.getValue(PropertyMap.REQUIRED_PROP));
            pm = tb.getColumn("BLANK").getProperties();
            assertEquals(" ", pm.getValue(PropertyMap.DEFAULT_VALUE_PROP));
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    public void setDPK() throws SQLException, IOException {
        Statement st = null;
        try {
            st = ucanaccess.createStatement();
            st.execute("create table dkey(c counter  , " + "number numeric(23,5)  , " + "  PRIMARY KEY (C,NUMBER))");
            st.execute("create table dunique(c text  , " + "number numeric(23,5)  , " + "  unique (C,NUMBER))");
            this.ucanaccess.setAutoCommit(false);
            try {
                st = ucanaccess.createStatement();
                st.execute("insert into  dunique values('ddl forces commit',2.3)");
                st.close();
                st = ucanaccess.createStatement();
                st.execute("create table dtrx(c text  , " + "number numeric(23,5) , " + "  unique (C,NUMBER))");
                st.execute("insert into  dtrx values('I''ll be forgotten sob sob ',55555.3)");
                st.close();
                st = ucanaccess.createStatement();
                st.execute("alter table dtrx ADD CONSTRAINT pk_dtrx PRIMARY KEY (c,number)");
                st.close();
            } catch (Exception e) {
                ucanaccess.rollback();
            }
            st = ucanaccess.createStatement();
            st.execute("insert into  dtrx values('Hi all',444.3)");
            st.execute("insert into  dtrx values('Hi all',4454.3)");
            dumpQueryResult("SELECT * FROM dtrx");
            dumpQueryResult("SELECT * FROM dunique");
            ucanaccess.commit();
            checkQuery("SELECT * FROM  dunique");
            checkQuery("SELECT * FROM  dtrx");
        } finally {
            st.close();
        }
    }

    public void setTableProperties() throws SQLException {
        Statement st = null;
        try {
            st = ucanaccess.createStatement();
            st.execute("create table tbl(c counter  primary key , " + "number numeric(23,5) default -4.6 not null , "
                    + "txt1 text(23)  default 'ciao', blank text  default ' ', dt date default date(), txt2 text(33),"
                    + "txt3 text)");
        } finally {
            st.close();
        }
    }

    private void notNullBug() throws SQLException, IOException {
        Statement st = null;
        try {
            st = ucanaccess.createStatement();
            st.execute("create table nnb(c counter  primary key , " + "number decimal (23,5) default -4.6 not null , "
                    + "txt1 text(23)  not null, blank text  , dt date not null, txt2 text  ," + "txt3 text not null)");

            checkNotNull("nnb", "number", true);
            checkNotNull("nnb", "txt1", true);
            checkNotNull("nnb", "blank", false);
            checkNotNull("nnb", "dt", true);
            checkNotNull("nnb", "txt2", false);
            checkNotNull("nnb", "txt3", true);
        } finally {
            st.close();
        }
    }

    private void checkNotNull(String tn, String cn, boolean notNull) throws IOException {
        Database db = ucanaccess.getDbIO();
        Table tb = db.getTable(tn);
        PropertyMap pm = tb.getColumn(cn).getProperties();
        assertEquals(notNull, pm.getValue(PropertyMap.REQUIRED_PROP));

    }

    @Test
    public void testCreate() throws Exception {
        executeStatements(
                "CREATE \nTABLE AAA ( baaaa \ntext PRIMARY KEY,A long   default 3 not null, C text(255) not null, "
                        + "d DATETIME default now(), e text default 'l''aria')");

        createSimple();
        createPs();
        createAsSelect();
        createAsSelect2();
        setTableProperties();
        setDPK();
        defaults();
        notNullBug();

        dropTable("AAA");
    }

    @Test
    public void testNaming() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.execute(
                " CREATE TABLE [ggg kk]( [---bgaaf aa] autoincrement PRIMARY KEY, [---bghhaaf b aa] text(222) default 'vvv')");
        st.execute(
                " CREATE TABLE [ggg kkff]( [---bgaaf() aa] autoincrement PRIMARY KEY, [---bghhaaf b aa()] text(222) default 'vvv')");
        st.execute(
                " CREATE TABLE [wHere12]( [where] autoincrement PRIMARY KEY, [---bghhaaf b aa] text(222) default 'vvv')");
        st.execute(" drop table  [ggg kk]");
        st.execute(
                " CREATE TABLE [ggg kk]( [---bgaaf aa] autoincrement PRIMARY KEY, [---bghhaaf b aa] numeric(22,6) default 12.99)");
        st.execute(
                " CREATE TABLE kkk ( [---bgaaf aa] autoincrement PRIMARY KEY, [---bghhaaf b aa] text(222) default 'vvv')");
        st.execute(" insert into kkk([---bgaaf aa],[---bghhaaf b aa]) values(1,'23fff')");
        st.execute(" CREATE TABLE counter ( counter autoincrement PRIMARY KEY, [simple] text(222) default 'vvv')");
        st.close();

        dumpQueryResult("SELECT * FROM counter");
    }

    @Test
    public void testCreateWithFK() throws SQLException, IOException {
        Statement st = ucanaccess.createStatement();
        st.execute(" CREATE TABLE Parent( x autoincrement PRIMARY KEY, y text(222))");
        st.execute(
                " CREATE TABLE Babe( k LONG , y LONG, PRIMARY KEY(k,y), FOREIGN KEY (y) REFERENCES Parent (x)  )");
        Database db = ucanaccess.getDbIO();
        Table tb = db.getTable("Babe");
        Table tbr = db.getTable("Parent");
        Index idx = tb.getForeignKeyIndex(tbr);
        List<String> ar = new ArrayList<String>();
        for (Column cl : idx.getColumns()) {
            ar.add(cl.getName());
        }
        assertTrue(ar.contains("y"));
        st.execute(" CREATE TABLE [1 Parent]( [x 0] long , y long, PRIMARY KEY([x 0],y))");
        st.execute(
                " CREATE TABLE [1 Babe]( k LONG , y LONG, [0 z] LONG, PRIMARY KEY(k,y), FOREIGN KEY (y,[0 z] ) REFERENCES [1 Parent] ( [x 0] , y)  )");

        st.close();
    }

    @Test
    public void testPs() throws SQLException {
        PreparedStatement ps = ucanaccess.prepareStatement("CREATE TABLE PS (PS AUTOINCREMENT PRIMARY KEY)");
        ps.execute();
        ps = ucanaccess.prepareStatement("CREATE TABLE PS3 (PS AUTOINCREMENT PRIMARY KEY)", 0);
        ps.execute();
        ps = ucanaccess.prepareStatement("CREATE TABLE PS1 (PS AUTOINCREMENT PRIMARY KEY)", 0, 0);
        ps.execute();
        ps = ucanaccess.prepareStatement("CREATE TABLE PS2 (PS AUTOINCREMENT PRIMARY KEY)", 0, 0, 0);
        ps.execute();

    }

    @Test
    public void testPsHyphen() throws SQLException {
        String ddl = "CREATE TABLE zzzFoo1 ([Req-MTI] TEXT(20))";
        // #9 hyphen in DDL column name confuses PreparedStatement
        PreparedStatement prepStmt = ucanaccess.prepareStatement(ddl);
        prepStmt.executeUpdate();
    }

    @Test
    public void testCreateHyperlink() throws SQLException {
        Statement st = ucanaccess.createStatement();
        ResultSet rs = null;
        st.execute("CREATE TABLE urlTest (id LONG PRIMARY KEY, website HYPERLINK)");
        st.execute("INSERT INTO urlTest (id, website) VALUES (1, '#http://whatever#')");
        st.execute("INSERT INTO urlTest (id, website) VALUES (2, 'example.com#http://example.com#')");
        st.execute("INSERT INTO urlTest (id, website) VALUES (3, 'the works#http://burger#with_bacon#and_cheese')");
        st.execute("INSERT INTO urlTest (id, website) VALUES (4, 'http://bad_link_no_hash_characters')");
        rs = ucanaccess.getMetaData().getColumns(null, null, "urlTest", "website");
        rs.next();
        assertEquals("HYPERLINK", rs.getString("ORIGINAL_TYPE"));
        rs = st.executeQuery("SELECT website FROM urlTest ORDER BY id");
        rs.next();
        assertEquals("http://whatever", rs.getURL(1).toString());
        rs.next();
        assertEquals("http://example.com", rs.getURL(1).toString());
        rs.next();
        assertEquals("http://burger#with_bacon", rs.getURL(1).toString());
        rs.next();
        try {
            rs.getURL(1);
            fail("UcanaccessSQLException should have been thrown");
        } catch (UcanaccessSQLException use) {
            if (!use.getMessage().endsWith("Invalid or unsupported URL format")) {
                throw use;
            }
        }
        rs.close();
        st.close();

    }

}
