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
