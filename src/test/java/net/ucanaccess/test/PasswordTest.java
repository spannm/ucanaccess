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

import com.healthmarketscience.jackcess.Database.FileFormat;

public class PasswordTest extends UcanaccessTestBase {
	public PasswordTest() {
		super();
	}
	
	public PasswordTest(FileFormat accVer) {
		super(accVer);
	}
	
	public String getAccessPath() {
		return "net/ucanaccess/test/resources/pwd.mdb";
	}
	protected void setUp() throws Exception {}
	
	
	public void testPassword() throws Exception {
		Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
		Connection ucanaccessConnection = null;
		try {
			ucanaccessConnection = getUcanaccessConnection();
		} catch (Exception e) {
		}
		assertNull(ucanaccessConnection);
		super.setPassword("ucanaccess");
		//url will be
		try {
			ucanaccessConnection = getUcanaccessConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertNotNull(ucanaccessConnection);
	}
}
