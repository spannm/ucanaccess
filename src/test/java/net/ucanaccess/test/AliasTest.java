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
package net.ucanaccess.test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class AliasTest extends UcanaccessTestBase {
    public static boolean tableCreated;

    public AliasTest() {
        super();
    }

    public AliasTest(FileFormat accVer) {
        super(accVer);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        executeCreateTable("CREATE TABLE Talias (id LONG,descr memo,  Actuación  text) ");

    }

    public void testBig() throws SQLException, IOException {
        Statement st = null;
        try {
            st = super.ucanaccess.createStatement();
            int id = 6666554;
            st.execute("INSERT INTO Talias (id,descr)  VALUES( " + id + ",'t')");
            ResultSet rs = st
                    .executeQuery("select descr as [cipol%'&la]  from Talias " + " where descr<>'ciao'&'bye'&'pippo'");
            rs.next();
            System.out.println(rs.getMetaData().getColumnLabel(1));
            System.out.println(rs.getObject("cipol%'&la"));

        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    public void testAccent() throws SQLException, IOException {
        Statement st = null;
        try {
            st = super.ucanaccess.createStatement();
            st.executeQuery("select  Actuación as Actuació8_0_0_ , descr from Talias ");
            ResultSet rs = st.executeQuery("select  [Actuación] as Actuació8_0_0_ from Talias ");
            rs.next();
            System.out.println(rs.getMetaData().getColumnLabel(1));
            System.out.println(rs.getObject("Actuació8_0_0_"));

        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    public void testAsin() throws SQLException, IOException {
        Statement st = null;
        try {
            st = super.ucanaccess.createStatement();
            st.execute("create table xxxx (asin text, ff text)");
            dump("select  asin, ff from xxxx");

        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

}
