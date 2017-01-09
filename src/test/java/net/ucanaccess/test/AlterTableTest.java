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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import net.ucanaccess.jdbc.UcanaccessConnection;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.Index.Column;

public class AlterTableTest extends UcanaccessTestBase {

	public AlterTableTest() {
		super();
	}

	public AlterTableTest(FileFormat accVer) {
		super(accVer);
	}

	protected void setUp() throws Exception {
		super.setUp();
		executeCreateTable("CREATE TABLE AAAn ( baaaa TEXT(3) PRIMARY KEY,A INTEGER , C TEXT(4)) ");
		executeCreateTable(
				"CREATE TABLE [AAA n] ( baaaa TEXT(3) ,A INTEGER , C TEXT(4), b yesNo, d datetime, e numeric(8,3)) ",
				1);
	}

	public String getAccessPath() {
		return "net/ucanaccess/test/resources/badDB.accdb";
	}

	public void testRename() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("ALTER TABLE [??###] RENAME TO [1GIà GIà]");
			boolean b = false;
			try {
				st.execute("ALTER TABLE T4 RENAME TO [1GIà GIà]");
			} catch (SQLException e) {
				b = true;
			}
			assertTrue(b);
			checkQuery("select * from [1GIà GIà]");
			dump("select * from [1GIà GIà]");
			System.out.println("after having renamed a few tables....");
			dump("select * from UCA_METADATA.TABLES");
			
		}

		finally {
			if (st != null)
				st.close();
		}
	}

	public void testAddColumn() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("ALTER TABLE AAAn RENAME TO [GIà GIà]");
			st.execute("Insert into [GIà GIà] (baaaa) values('chi')");
			checkQuery("select * from [GIà GIà] ORDER BY c");
			dump("select * from [GIà GIà] ORDER BY c");
			st
					.execute("ALTER TABLE [GIà GIà] RENAME TO [22 amadeimargmail111]");
			checkQuery("select * from [22 amadeimargmail111] ORDER BY c");
			dump("select * from [22 amadeimargmail111] ORDER BY c");
			st
					.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [ci ci]  TEXT NOT NULL DEFAULT 'PIPPO'  ");
			st
					.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [健康] decimal (23,5) ");
			st
					.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [£健康] numeric (23,6) default 13.031955 not null");
			try{
				st
				.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN defaultwwwdefault numeric (23,6) not null");
			}catch(Exception e){
				e.printStackTrace();
			}
			st
					.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [ci ci1]  DATETIME NOT NULL DEFAULT now() ");
			st
					.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [ci ci2]  YESNO  ");
			st
					.execute("Insert into [22 amadeimargmail111] (baaaa) values('cha')");
			st
					.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN Memo  Memo  ");
			st
					.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN ole  OLE  ");
			checkQuery("select * from [22 amadeimargmail111] ORDER BY c");
			dump("select * from [22 amadeimargmail111] ORDER BY c");
			st
			.execute("ALTER TABLE Sample ADD COLUMN YeSNo  YesNo  ");
			checkQuery("select * from Sample");
			dump("select * from Sample");
			
			System.out.println("after having added a few columns....");
			dump("select * from UCA_METADATA.Columns");
		}

		finally {
			if (st != null)
				st.close();
		}

	}

	public void testCreateIndex() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st
					.execute("CREATE unique INDEX [èèè 23] on [AAA n]  (a ASC,c ASC )");
			boolean b = false;
			try {
				st.execute("INSERT INT0 [AAA n]  (a,C ) values (24,'su')");
				st.execute("INSERT INT0 [AAA n]  (a,C ) values (24,'su')");
			} catch (Exception e) {
				b = true;
			}
			assertTrue(b);
			Database db = ((UcanaccessConnection) super.ucanaccess).getDbIO();
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
			
			
			st.execute("CREATE  INDEX ciao on Sample  (field)");
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
		}

		finally {
			if (st != null)
				st.close();
		}
	}

	public void testCreatePK() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("ALTER TABLE [AAA n] add  Primary key (baaaa,a)");
			Database db = ((UcanaccessConnection) super.ucanaccess).getDbIO();
			Table tb = db.getTable("AAA n");
			Index idx = tb.getPrimaryKeyIndex();
			ArrayList<String> ar = new ArrayList<String>();
			for (Column cl : idx.getColumns()) {
				ar.add(cl.getName());
			}
			assertTrue(ar.contains("A"));
			assertTrue(ar.contains("baaaa"));
			
			
			
			st.execute("ALTER TABLE Sample add  Primary key (id)");
			tb = db.getTable("Sample");
		    idx = tb.getPrimaryKeyIndex();
		    ar.clear();
			for (Column cl : idx.getColumns()) {
				ar.add(cl.getName());
			}
			assertTrue(ar.contains("ID"));
			
		}

		finally {
			if (st != null)
				st.close();
		}
	}

	public void testCreateFK() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("ALTER TABLE [AAA n] add constraint pippo1 foreign key (c) references [22 amadeimargmail111] (baaaa) ON delete cascade");
			Database db = ((UcanaccessConnection) super.ucanaccess).getDbIO();
			Table tb = db.getTable("AAA n");
			Table tbr = db.getTable("22 amadeimargmail111");
			Index idx = tb.getForeignKeyIndex(tbr);
			ArrayList<String> ar = new ArrayList<String>();
			for (Column cl : idx.getColumns()) {
				ar.add(cl.getName());
			}
			assertTrue(ar.contains("C"));
			
			st.execute("ALTER TABLE Son add foreign key (integer, txt) references Father(id,txt) ON delete cascade");
			
		}

		finally {
			if (st != null)
				st.close();
		}
	}

}
