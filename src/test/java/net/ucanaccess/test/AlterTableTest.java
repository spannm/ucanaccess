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
		executeCreateTable("CREATE TABLE [AAA n] ( baaaa TEXT(3) ,A INTEGER , C TEXT(4)) ",1);
	}
	
	public void testAlter() throws SQLException,
			IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("ALTER TABLE AAAn RENAME TO [GIà GIà]");
			st.execute("Insert into [GIà GIà] (baaaa) values('chi')");
			checkQuery("select * from [GIà GIà] ORDER BY c");
			dump("select * from [GIà GIà] ORDER BY c");
			st.execute("ALTER TABLE [GIà GIà] RENAME TO [22 amadeimargmail111]");
			checkQuery("select * from [22 amadeimargmail111] ORDER BY c");
			dump("select * from [22 amadeimargmail111] ORDER BY c");
			st.execute("ALTER TABLE [22 amadeimargmail111] ADD COLUMN [ci ci]  TEXT  NOT NULL DEFAULT 'PIPPO' ");
			st.execute("Insert into [22 amadeimargmail111] (baaaa) values('cha')");
			dump("select * from [22 amadeimargmail111] ORDER BY c");
			st.execute("CREATE unique INDEX [èèè 23] on [22 amadeimargmail111] (baaaa ASC,[ci ci] ASC )");
			st.execute("ALTER TABLE [AAA n] add constraint pippo Primary key (baaaa,a)"); 
			st.execute("ALTER TABLE [AAA n] add constraint pippo1 foreign key (c) references [22 amadeimargmail111] (baaaa) ON delete cascade");  
		} 
		
		finally {
			if (st != null)
				st.close();
		}
	}
	

	
	
}
