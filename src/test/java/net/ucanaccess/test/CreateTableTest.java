/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Marco Amadei at amadei.mar@gmail.com.

 */
package net.ucanaccess.test;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.ucanaccess.jdbc.UcanaccessConnection;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Table;

public class CreateTableTest extends UcanaccessTestBase {
	public CreateTableTest() {
		super();
	}

	public CreateTableTest(FileFormat accVer) {
		super(accVer);
	}

	private void createAsSelect() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.executeUpdate("CREATE TABLE AAA_BIS as (select baaaa,a,c from AAA) 	WITH DATA");
			Object[][] ver = { { "33A", 3, "G" }, { "33B", 111, "G" } };
			checkQuery("select * from AAA_bis order by baaaa", ver);
		} finally {
			if (st != null)
				st.close();
		}
	}

	private void createAsSelect2() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.executeUpdate("CREATE TABLE AAA_TRIS as (select baaaa,a,c from AAA) WITH no DATA ");
			st.execute("INSERT INTO AAA_TRIS SELECT * from AAA_bis");
			Object[][] ver = { { "33A", 3, "G" }, { "33B", 111, "G" } };
			checkQuery("select * from AAA_tris order by baaaa", ver);
		} finally {
			if (st != null)
				st.close();
		}
	}

	private void createPs() throws SQLException, IOException {
		PreparedStatement ps = null;
		try {
			ps = super.ucanaccess
					.prepareStatement(" CREATE \nTABLE BBB ( baaaa \nvarchar(2) PRIMARY KEY)");
			ps.execute(" CREATE TABLE BBB ( baaaa text PRIMARY KEY,b text)");
			throw new RuntimeException("To block DDL with PreparedStatement");
		} catch (SQLException ex) {
			System.out.println("ok");
		} finally {
			if (ps != null)
				ps.close();
		}
	}

	private void createSimple() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("INSERT INTO AAA(baaaa,c) VALUES ('33A','G'   )");
			st.execute("INSERT INTO AAA(baaaa,a,c) VALUES ('33B',111,'G'   )");
			Object[][] ver = { { "33A", 3, "G" }, { "33B", 111, "G" } };
			checkQuery("select baaaa,a,c from AAA order by baaaa", ver);
		} finally {
			if (st != null)
				st.close();
		}
	}

	public void defaults() throws Exception {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			ResultSet rs = st.executeQuery("SELECT D, E FROM AAA");
			while (rs.next()) {
				assertNotNull(rs.getObject(1));
				assertNotNull(rs.getObject(2));
			}
			Database db = ((UcanaccessConnection) super.ucanaccess).getDbIO();
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
			if (st != null)
				st.close();
		}
	}

	public void setDPK() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("create table dkey(c counter  , "
					+ "number numeric(23,5)  , " + "  PRIMARY KEY (C,NUMBER))");
			st.execute("create table dunique(c text  , "
					+ "number numeric(23,5)  , " + "  unique (C,NUMBER))");
			this.ucanaccess.setAutoCommit(false);
			try {
				st = super.ucanaccess.createStatement();
				st.execute("insert into  dunique values('ddl forces commit',2.3)");
				st.close();
				st = super.ucanaccess.createStatement();
				st.execute("create table dtrx(c text  , "
						+ "number numeric(23,5) , " + "  unique (C,NUMBER))");
				st.execute("insert into  dtrx values('I''ll be forgotten sob sob ',55555.3)");
				st.close();
				st = super.ucanaccess.createStatement();
				st.execute("alter table dtrx ADD CONSTRAINT pk_dtrx PRIMARY KEY (c,number))");
			} catch (Exception e) {
				super.ucanaccess.rollback();
			}
			st = super.ucanaccess.createStatement();
			st.execute("insert into  dtrx values('Hi all',444.3)");
			st.execute("insert into  dtrx values('Hi all',4454.3)");
			dump("select * from dtrx");
			dump("select * from dunique");
			super.ucanaccess.commit();
			checkQuery("select * from  dunique");
			checkQuery("select * from  dtrx");
		} finally {
			st.close();
		}
	}

	public void setTableProperties() throws SQLException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("create table tbl(c counter  primary key , "
					+ "number numeric(23,5) default -4.6 not null , "
					+ "txt1 text(23)  default 'ciao', blank text  default ' ', dt date default date(), txt2 text(33),"
					+ "txt3 text)");
		} finally {
			st.close();
		}
	}

	protected void setUp() throws Exception {
		super.setUp();
		executeCreateTable(" CREATE \nTABLE AAA ( baaaa \ntext PRIMARY KEY,A long   default 3 not null, C text(255) not null, "
				+ "d DATETIME default now(), e text default 'l''aria')");
		
	}

	public void testCreate() throws Exception {
		 createSimple();
		 createPs();
		 createAsSelect() ;
		 createAsSelect2();
		 setTableProperties();
		setDPK();
		 defaults();
	}
	
	
	public void testNaming() throws SQLException{
		Statement st = super.ucanaccess.createStatement();
		st.execute(" CREATE TABLE [ggg kk]( [---bgaaf aa] autoincrement PRIMARY KEY, [---bghhaaf b aa] text(222) default 'vvv')");
		st.execute(" CREATE TABLE [ggg kkff]( [---bgaaf() aa] autoincrement PRIMARY KEY, [---bghhaaf b aa()] text(222) default 'vvv')");
		st.execute(" CREATE TABLE [wHere]( [where] autoincrement PRIMARY KEY, [---bghhaaf b aa] text(222) default 'vvv')");
		st.execute(" drop table  [ggg kk]");
		st.execute(" CREATE TABLE [ggg kk]( [---bgaaf aa] autoincrement PRIMARY KEY, [---bghhaaf b aa] numeric(22,6) default 12.99)");
		st.execute(" CREATE TABLE kkk ( [---bgaaf aa] autoincrement PRIMARY KEY, [---bghhaaf b aa] text(222) default 'vvv')");
		st.execute(" insert into kkk([---bgaaf aa],[---bghhaaf b aa]) values(1,'23fff')");
		
	}
}
