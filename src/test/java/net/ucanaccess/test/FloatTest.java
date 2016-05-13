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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;

import java.util.Locale;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class FloatTest extends UcanaccessTestBase {
	

	public FloatTest() {
		super();
		Locale.setDefault(Locale.US);
	}

	public FloatTest(FileFormat accVer) {
		super(accVer);
		Locale.setDefault(Locale.US);
	}

	@Override
	protected void setUp() throws Exception {

		super.setUp();
		

	}
	
	
	
	
	public void testCreate() throws SQLException, IOException, ParseException {
		PreparedStatement ps= ucanaccess.prepareStatement("insert into t (row) values(?)");
		ps.setFloat(1, 1.4f);
		ps.execute();
		 ps= ucanaccess.prepareStatement("update t  set [row]=?");
		 ps.setObject(1, 4.9d);
		 ps.execute();
		 checkQuery("select [row] from t");
	}
	
	public String getAccessPath() {
		return  "net/ucanaccess/test/resources/float.accdb";
	}
	

	
	
}
