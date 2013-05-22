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
		dump("select * from t4");
		
		assertTrue(getCount("select count(*) from T4", true) == 0);
		
		
		
	}

}
