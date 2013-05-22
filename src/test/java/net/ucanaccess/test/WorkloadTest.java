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
import java.sql.SQLException;
import java.sql.Statement;
import com.healthmarketscience.jackcess.Database.FileFormat;

public class WorkloadTest extends UcanaccessTestBase {

	public WorkloadTest() {
		super();
	}

	public WorkloadTest(FileFormat accVer) {
		super(accVer);
	}

	protected void setUp() throws Exception {
		super.setUp();
		executeCreateTable("CREATE TABLE AAAB ( id COUNTER PRIMARY KEY,A LONG , C TEXT,D TEXT) ");

	}

	public void testLoad30000() throws SQLException, IOException, InterruptedException {
		super.ucanaccess.setAutoCommit(false);
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();

			long time = System.currentTimeMillis();
			for (int i = 0; i <= 30000; i++)
				st.execute("INSERT INTO AAAB(id,a,c,d) VALUES (" + i
						+ ",'33','booo','ddddddddddddddddddddd' )");
			super.ucanaccess.commit();
			long time1 = System.currentTimeMillis();
			System.out
					.println("Autoincrement insert performance test, 30000 records inserted in "
							+ (time1 - time) + " seconds");
			st = super.ucanaccess.createStatement();
			st.executeUpdate("update aaAB set c='yessssss'&a");
			super.ucanaccess.commit();
			long time2 = System.currentTimeMillis();
			System.out
					.println("Update performance test, all 30000 table records updated in "
							+ (time2 - time1) + " seconds");
			
		} finally {
			if (st != null)
				st.close();
		}
	}

}
