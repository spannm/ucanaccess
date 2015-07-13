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
import java.sql.Savepoint;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class TransactionTest extends UcanaccessTestBase {

	public TransactionTest() {
		super();
	}

	public TransactionTest(FileFormat accVer) {
		super(accVer);
	}

	protected void setUp() throws Exception {
		super.setUp();
		executeCreateTable("CREATE TABLE T4 (id LONG,descr text(200)) ");
	}

	public void testCommit() throws SQLException, IOException {
		super.ucanaccess.setAutoCommit(false);
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			int i=getCount("select count(*) from T4", true);
			st
					.execute("INSERT INTO T4 (id,descr)  VALUES( 6666554,'nel mezzo del cammin di nostra vita')");
			assertTrue(getCount("select count(*) from T4", false) == i);
			super.ucanaccess.commit();
			assertTrue(getCount("select count(*) from T4", true) == i+1);
			
		} finally {
			if (st != null)
				st.close();
		}
	}

	public void testSavepoint() throws SQLException, IOException {
		int count = getCount("select count(*) from T4");
		super.ucanaccess.setAutoCommit(false);
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st
					.execute("INSERT INTO T4 (id,descr)  VALUES( 1,'nel mezzo del cammin di nostra vita')");
			Savepoint sp = super.ucanaccess.setSavepoint();
			assertTrue(getCount("select count(*) from T4", false) == count);
			st
					.execute("INSERT INTO T4 (id,descr)  VALUES( 2,'nel mezzo del cammin di nostra vita')");
			super.ucanaccess.rollback(sp);
			super.ucanaccess.commit();
			assertTrue(getCount("select count(*) from T4") == (count + 1));
			super.ucanaccess.setAutoCommit(false);
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	public void testSavepoint2() throws SQLException, IOException {
		int count = getCount("select count(*) from T4");
		super.ucanaccess.setAutoCommit(false);
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st
					.execute("INSERT INTO T4 (id,descr)  VALUES( 1,'nel mezzo del cammin di nostra vita')");
			Savepoint sp = super.ucanaccess.setSavepoint("Gord svp");
			assertTrue(getCount("select count(*) from T4", false) == count);
			st
					.execute("INSERT INTO T4 (id,descr)  VALUES( 2,'nel mezzo del cammin di nostra vita')");
			super.ucanaccess.rollback(sp);
			super.ucanaccess.commit();
			assertTrue(getCount("select count(*) from T4") == (count + 1));
			super.ucanaccess.setAutoCommit(false);
		} finally {
			if (st != null)
				st.close();
		}
	}
}
