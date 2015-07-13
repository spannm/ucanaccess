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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class MultiThreadAccessTest extends UcanaccessTestBase {
	static int i;
	public static boolean tableCreated;

	public MultiThreadAccessTest() {
		super();
	}

	public MultiThreadAccessTest(FileFormat accVer) {
		super(accVer);
	}

	public void crud() throws SQLException, IOException {
		Connection conn = this.getUcanaccessConnection();
		conn.setAutoCommit(false);
		Statement st = conn.createStatement();
		++i;
		st.execute("INSERT INTO T1 (id,descr)  VALUES( " + (i) + ",'" + (i)
				+ "Bla bla bla bla:" + Thread.currentThread() + "')");
		conn.commit();
		conn.close();
	}

	public void crudPS() throws SQLException, IOException {
		Connection conn = this.getUcanaccessConnection();
		conn.setAutoCommit(false);
		System.out.println(conn);
		PreparedStatement ps = conn
				.prepareStatement("INSERT INTO T1 (id,descr)  VALUES(?, ?)");
		ps.setInt(1, ++i);
		ps.setString(2, "ciao");
		ps.execute();
		ps = conn.prepareStatement("UPDATE T1 SET descr='"
				+ Thread.currentThread() + "'");
		ps.executeUpdate();
		ps = conn.prepareStatement("DELETE FROM  t1  WHERE  descr='"
				+ Thread.currentThread() + "'");
		conn.commit();
		conn.close();
	}

	public void crudUpdatableRS() throws SQLException, IOException {
		Connection conn = this.getUcanaccessConnection();
		conn.setAutoCommit(false);
		Statement st = conn.createStatement();
		st.execute("INSERT INTO T1 (id,descr)  VALUES(" + (++i) + "  ,'"
				+ Thread.currentThread() + "')");
		PreparedStatement ps = conn.prepareStatement("SELECT *  FROM T1",
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE,
				ResultSet.CLOSE_CURSORS_AT_COMMIT);
		ResultSet rs = ps.executeQuery();
		rs.next();
		rs.updateString(2, "" + Thread.currentThread());
		rs.updateRow();
		conn.commit();
		conn.close();
	}

	protected void setUp() throws Exception {
		super.setUp();
		if (!tableCreated) {
			tableCreated = true;
			Statement st = super.ucanaccess.createStatement();
			st.executeUpdate("DROP TABLE T1 IF EXISTS");
			st
					.executeUpdate("CREATE TABLE T1 (id COUNTER primary key,descr MEMO) ");
		}
	}

	public void testMultiThread() throws SQLException, IOException {
		int nt = 200;
		Thread[] ths = new Thread[nt];
		for (int i = 0; i < nt; i++) {
			ths[i] = new Thread() {
				@Override
				public void run() {
					try {
						crud();
						crudPS();
						crudUpdatableRS();
					} catch (SQLException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			};
			ths[i].start();
		}
		for (Thread th : ths) {
			try {
				th.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		super.ucanaccess = this.getUcanaccessConnection();
		dump("select * from t1 ORDER BY id");
	
		this.checkQuery("select * from t1 ORDER BY id");
	
	}
}
