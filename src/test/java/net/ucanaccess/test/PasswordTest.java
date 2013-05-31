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
