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
import net.ucanaccess.jdbc.UcanaccessSQLException;

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
				"CREATE TABLE [AAA n] ( baaaa TEXT(3) ,A INTEGER , C TEXT(4), b yesNo, d datetime default now(), e numeric(8,3),[f f]TEXT ) ",
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
					.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [ci ci]  TEXT(100) NOT NULL DEFAULT 'PIPPO'  ");
			st
					.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [健康] decimal (23,5) ");
			st
						.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [£健康] numeric (23,6) default 13.031955 not null");
			boolean b=false;
			try{
				st
				.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN defaultwwwdefault numeric (23,6) not null");
			}catch(UcanaccessSQLException e){
				b=true;
				System.err.println(e.getMessage());
			}
			assertTrue(b);
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
			this.createFK();
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

	private void createFK() throws SQLException, IOException {
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
	
	public void testMiscellaneus() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("ALTER TABLE tx add constraint pk primary key ([i d]) ");
			st.execute("ALTER TABLE tx add column [my best friend] long ");
			st.execute("ALTER TABLE tx add column [my worst friend] single ");
			st.execute("ALTER TABLE tx add column  [Is Pippo] TEXT(100) ");
			st.execute("ALTER TABLE tx add column  [Is not Pippo]TEXT default \"what's this?\"");
			
			st.execute("create TABLE tx1  (n1 long, [n 2] text)");
			st.execute("ALTER TABLE tx1 add primary key (n1, [n 2])");
			st.execute("ALTER TABLE tx add  foreign key ([my best friend],[Is Pippo])references tx1(n1, [n 2])ON delete cascade");
			st.execute("insert into tx1 values(1,\"ciao\")");
			st.execute("insert into tx ([my best friend], [my worst friend], [Is Pippo]) values(1,2,\"ciao\")");
			checkQuery("select count(*) from tx",1);
			st.execute("delete from tx1");
			checkQuery("select count(*) from tx");
			checkQuery("select count(*) from tx",0);
			st.execute("drop table tx ");
			st.execute("drop table tx1  ");
			
			st.execute("create table tx (id counter primary key, [my best friend]long , [my worst friend] single,[Is Pippo] TEXT(100) ,[Is not Pippo]TEXT default \"what's this?\" )");
			st.execute("create TABLE tx1  (n1 long, [n 2] text)");
			st.execute("ALTER TABLE tx1 add primary key (n1, [n 2])");
			st.execute("ALTER TABLE tx add  foreign key ([my best friend],[Is Pippo])references tx1(n1, [n 2])ON delete set null");
			st.execute("insert into tx1 values(1,\"ciao\")");
			st.execute("insert into tx ([my best friend], [my worst friend], [Is Pippo]) values(1,2,\"ciao\")");
			checkQuery("select count(*) from tx",1);
			st.execute("delete from tx1");
			checkQuery("select count(*) from tx",1);
			checkQuery("select * from tx",1 , null , 2.0 , null , "what's this?");
			st.execute("CREATE  UNIQUE  INDEX IDX111 ON tx ([my best friend])");
			
			boolean b=false;
			try{
				st.execute("insert into tx ([my best friend], [my worst friend], [Is Pippo]) values(1,2,\"ciao\")");
			}catch(UcanaccessSQLException e){
				b=true;
				System.err.println(e.getMessage());
			}
			assertTrue(b);
		}

		finally {
			if (st != null)
				st.close();
		}
	}
	
	private void executeErr(String ddl, String expectedMessage) throws SQLException{
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute(ddl);
		}
		catch(SQLException e){
			System.err.println(e.getMessage());
			assertTrue(e.getMessage().endsWith(expectedMessage));
		}

		finally {
			if (st != null)
				st.close();
		}
	} 
	
	
	
	public void testSQLErrors() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("create table tx2 (id counter , [my best friend]long , [my worst friend] single,[Is Pippo] TEXT(100) ,[Is not Pippo]TEXT default \"what's this?\" )");
			st.execute("insert into tx2 ([my best friend], [my worst friend], [Is Pippo]) values(1,2,\"ciao\")");
			executeErr("ALTER TABLE tx2 add constraint primary key ([i d]) ","unexpected token: PRIMARY");
			executeErr("ALTER TABLE tx2 add column [my best friend]  ","unexpected end of statement");
			executeErr("ALTER TABLE tx2 add constraint foreign key ([my best friend],[Is Pippo])references tx1(n1, [n 2])ON delete cascade","unexpected token: KEY");
			executeErr("drop table tx2 cascade","Feature not supported yet.");
			executeErr("ALTER TABLE tx2 add constraint primary key (id)","unexpected token: PRIMARY");
			executeErr("ALTER TABLE tx2 ALTER COLUMN [my best friend] SET DEFAULT 33","Feature not supported yet.");
			executeErr("ALTER TABLE tx2 drop COLUMN [my best friend]","Feature not supported yet.");
			executeErr("ALTER TABLE tx2 add COLUMN [1 my best friend]lonG not null","x2 already contains one or more records(1 records)");
			
			
		}

		finally {
			if (st != null)
				st.close();
		}
	}


}
