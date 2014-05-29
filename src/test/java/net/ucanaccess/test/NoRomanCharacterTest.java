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

import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class NoRomanCharacterTest extends UcanaccessTestBase {
	public NoRomanCharacterTest() {
		super();
	}

	public NoRomanCharacterTest(FileFormat accVer) {
		super(accVer);
	}

	public String getAccessPath() {
		return "net/ucanaccess/test/resources/noroman.mdb";
	}

	public void testNoRomanCharactersInColumnName() throws Exception {
		dump("SELECT * FROM NOROMAN");
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute("INSERT INTO NOROMAN ([end],[q3¹²³¼½¾ß€Ð×ÝÞðýþ])  VALUES( 'the end','yeeep')");
			st.execute("UPDATE NOROMAN SET [q3¹²³¼½¾ß€Ð×ÝÞðýþ]='NOOOp' WHERE [end]='the end' ");
			checkQuery("SELECT * FROM NOROMAN");
		} finally {
			if (st != null)
				st.close();
		}
	}
}
