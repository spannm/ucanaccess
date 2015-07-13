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
import java.sql.Date;
import java.sql.PreparedStatement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class ColumnOrderTest extends UcanaccessTestBase {
	public ColumnOrderTest() {
		super();
	}
	
	public ColumnOrderTest(FileFormat accVer) {
		super(accVer);
	}
	
	public String getAccessPath() {
		return "net/ucanaccess/test/resources/columnOrder.accdb";
	}
	protected void setUp() throws Exception {}
	
	
	
	
	public void testColumnOrder1() throws Exception {
	
		super.setColumnOrder("display");
		Connection uca = getUcanaccessConnection();
		PreparedStatement ps=uca.prepareStatement("insert into t1 values (?,?,?)");
		ps.setInt(3, 3);
		ps.setDate(2, new Date(System.currentTimeMillis()));
		ps.setString(1, "This is the display order");
		ps.close();
		uca.close();
	}
}
