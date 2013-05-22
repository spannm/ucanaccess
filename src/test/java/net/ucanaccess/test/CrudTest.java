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

import com.healthmarketscience.jackcess.Database.FileFormat;

public class CrudTest extends UcanaccessTestBase {
	public static boolean tableCreated;
	
	public CrudTest() {
		super();
	}
	
	public CrudTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		 executeCreateTable("CREATE TABLE T1 (id LONG,descr TEXT) ");
		
	}
	
	public void testCrud() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			int id = 6666554;
			int id1 = 5556664;
			st.execute("INSERT INTO T1 (id,descr)  VALUES( " + id
					+ ",'nel mezzo del cammin di nostra vita')");
			boolean ret = getCount("select count(*) from T1") == 1;
			assertTrue("Failed Insert", ret);
			st.executeUpdate("UPDATE T1 SET id=" + id1 + " WHERE  id=" + id);
			ret = getCount("select count(*) from T1 where id=" + id1) == 1;
			assertTrue("Failed Update", ret);
			st.executeUpdate("DELETE FROM  T1  WHERE  id=" + id1);
			ret = getCount("select count(*) from T1 where id=" + id1) == 0;
			assertTrue("Failed Delete", ret);
		} finally {
			if (st != null)
				st.close();
		}
	}
	
	public void testCrudPS() throws SQLException, IOException {
		PreparedStatement ps = null;
		try {
			ps = super.ucanaccess
					.prepareStatement("INSERT INTO T1 (id,descr)  VALUES( ?,?)");
			int id = 6666554;
			int id1 = 5556664;
			ps.setInt(1, id);
			ps.setString(2, "Prep1");
			ps.execute();
			
			boolean ret = getCount("select count(*) from T1") == 1;
			assertTrue("Failed Insert", ret);
			ps.close();
			ps = super.ucanaccess
					.prepareStatement("UPDATE T1 SET id=? WHERE  id=?");
			ps.setInt(1, id1);
			ps.setInt(2, id);
			ps.executeUpdate();
			ret = getCount("select count(*) from T1 where id=" + id1) == 1;
			assertTrue("Failed Update", ret);
			ps.close();
			ps = super.ucanaccess
					.prepareStatement("DELETE FROM  t1  WHERE  id=?");
			ps.setInt(1, id1);
			ps.executeUpdate();
			ret = getCount("select count(*) from T1 where id=" + id1) == 0;
			assertTrue("Failed Delete", ret);
		} finally {
			if (ps != null)
				ps.close();
		}
	}
	
	
	
	public void testCrudPSBatch() throws SQLException, IOException {
		PreparedStatement ps = null;
		try {
			
			ps = super.ucanaccess
					.prepareStatement("INSERT INTO T1 (id,descr)  VALUES( ?,?)");
			int id = 1234;
			int id1 = 12345;
			ps.setInt(1, id);
			ps.setString(2, "Prep1");  
			ps.addBatch();
			ps.setInt(1, id1);
			ps.setString(2, "Prep2");
			ps.addBatch();
			ps.executeBatch();
			Object[][] ver = {{1234,"Prep1"},{12345,"Prep2"}};
			super.checkQuery("SELECT *  FROM T1",ver);
			boolean	ret = getCount("select count(*) from T1 where id in (1234,12345)") == 2;
			ps.clearBatch();
			assertTrue("Failed Insert", ret);
			ps = super.ucanaccess
					.prepareStatement("DELETE FROM  t1 ");
			ps.addBatch();
			ps.executeBatch();
			ret = getCount("select count(*) from T1 " ) == 0;
			assertTrue("Failed Delete", ret);
		} catch(Exception e){
			e.printStackTrace();
		}
		
		finally {
			if (ps != null)
				ps.close();
		}
	}
	
	public void testUpdatableRS() throws SQLException, IOException {
		Statement st = null;
		ResultSet rs =null;
		try {
	    st = super.ucanaccess.createStatement();
		int id = 6666554;
		st.execute("delete from t1");
		st.execute("INSERT INTO T1 (id,descr)  VALUES( " + id
				+ ",'tre canarini volano su e cadono')");
		PreparedStatement ps = super.ucanaccess.prepareStatement(
				"SELECT *  FROM T1", ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT);
		rs = ps.executeQuery();
		rs.next();
		rs.updateString(2, "show must go off");
		rs.updateRow();
		Object[][] ver = { { 6666554, "show must go off" } };
		super.checkQuery("SELECT *  FROM T1", ver);
		} 
		finally {
			if (rs != null)
				rs.close();
			if (st != null)
				st.close();
			
		}
	}
}
