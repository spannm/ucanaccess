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

import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class WeirdObjectNamesTest extends UcanaccessTestBase {
	public WeirdObjectNamesTest() {
		super();
	}

	public WeirdObjectNamesTest(FileFormat accVer) {
		super(accVer);
	}

	public String getAccessPath() {
		return "net/ucanaccess/test/resources/WeirdObjectNames.mdb";
	}
	

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		
	}

	public void testTableNameEndsInQuestionMarks() throws Exception {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			checkQuery("SELECT * FROM [19 MB 01 BEZAHLT ???]");
		} finally {
			if (st != null)
				st.close();
		}
	}

}
