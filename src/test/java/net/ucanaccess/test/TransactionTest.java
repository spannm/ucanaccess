/*
Copyright (c) 2012 Marco Atrymadei.

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
			st
					.execute("INSERT INTO T4 (id,descr)  VALUES( 6666554,'nel mezzo del cammin di nostra vita')");
			assertTrue(getCount("select count(*) from T4", false) == 0);
			super.ucanaccess.commit();
			assertTrue(getCount("select count(*) from T4", true) == 1);
			
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
}
