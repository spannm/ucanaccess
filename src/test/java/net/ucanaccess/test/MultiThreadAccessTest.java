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
