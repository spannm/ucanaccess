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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class Size97Test extends UcanaccessTestBase {
	public Size97Test() {
		super();
	}
	
	public Size97Test(FileFormat accVer) {
		super(accVer);
	}
	
	public String getAccessPath() {
		return "net/ucanaccess/test/resources/size97.mdb";
	}
	
	
	public void testSize() throws Exception {
		
		Connection	conn = getUcanaccessConnection();
		DatabaseMetaData dbmd=conn.getMetaData();
		ResultSet rs=dbmd.getColumns(null, null, "table1", "field1");
		rs.next();
		super.assertEquals(10, rs.getInt("COLUMN_SIZE"));
		
		
	}
}
