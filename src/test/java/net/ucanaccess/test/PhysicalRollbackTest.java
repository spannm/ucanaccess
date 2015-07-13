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
import java.lang.reflect.Method;
import java.sql.SQLException;

import java.sql.Statement;

import net.ucanaccess.jdbc.UcanaccessConnection;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class PhysicalRollbackTest extends UcanaccessTestBase {
	private static boolean tableCreated;

	public PhysicalRollbackTest() {
		super();
	}

	public PhysicalRollbackTest(FileFormat accVer) {
		super(accVer);
	}

	protected void setUp() throws Exception {
		super.setUp();
		if (!tableCreated) {
			Statement st =null;
			try{	
			st=	super.ucanaccess.createStatement();
			st
					.executeUpdate("CREATE TABLE T4 (id LONG,descr VARCHAR(400)) ");
			tableCreated = true;
			} finally {
				if (st != null)
					st.close();
			}
		}
	}

	public void testCommit() throws SQLException, IOException {
		super.ucanaccess.setAutoCommit(false);
		Statement st = null;
		try {
			//((UcanaccessConnection)super.ucanaccess).setTestRollback(true);
			Method mth=UcanaccessConnection.class.getDeclaredMethod("setTestRollback", new Class[]{boolean.class});
			mth.setAccessible(true);
			mth.invoke(super.ucanaccess, new Object[]{Boolean.TRUE});
			st = super.ucanaccess.createStatement();
			st
					.execute("INSERT INTO T4 (id,descr)  VALUES( 6666554,'nel mezzo del cammin di nostra vita')");
			st
			.execute("INSERT INTO T4 (id,descr)  VALUES( 77666554,'nel mezzo del cammin di nostra vita')");
			st
			.execute("UPDATE T4 SET ID=0 where id=77666554");
			
			
			st
			.execute("INSERT INTO T4 (id,descr)  VALUES( 4,'nel mezzo del cammin di nostra vita')");
			
			st
			.execute("delete from T4 where id=4");
			super.ucanaccess.commit();
		} catch (Throwable e){
			e.printStackTrace();
		}
		
		
		finally {
			if (st != null)
				st.close();
		}
		this.ucanaccess=super.getUcanaccessConnection();
		dump("select * from t4");
		
		assertTrue(getCount("select count(*) from T4", true) == 0);
		
		
		
	}

}
