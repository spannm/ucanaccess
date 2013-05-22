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
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class LoadTypesAccessTest extends UcanaccessTestBase {
	private final static SimpleDateFormat SDF=new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss");
	private static boolean tableCreated;
	
	public LoadTypesAccessTest() {
		super();
	}
	
	public LoadTypesAccessTest(FileFormat accVer) {
		super(accVer);
	}
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		if (!tableCreated) {
			Statement st =null; 
			try{	
			st=	super.ucanaccess.createStatement();
			st
					.executeUpdate("CREATE TABLE pluto (id LONG, descr MEMO, dt DATETIME,euros CURRENCY,float1 SINGLE, double1 DOUBLE, int1 INTEGER,numeric1 double) ");
			st
					.execute("INSERT INTO pluto (id,descr,dt,euros,float1,double1,int1,numeric1 )  VALUES( 1234,'I like trippa with spaghettis bolognese',#10/03/2008 10:34:35 PM#,4.55555,5.6666,6.7,5,4.677856)");
			tableCreated = true;}
			finally{
				if(st!=null)st.close();
			}
		}
	}
	
	public void testDate() throws SQLException, IOException, ParseException {
		checkQuery("select #10/03/2004# , #11/23/1811#,#10/03/2008 22:34:35#,#10/03/2008 22:34:35 aM#,#10/03/2008 10:34:35 PM# from pluto",
			SDF.parse("2004-10-03 00:00:00.0"),
			SDF.parse("1811-11-23 00:00:00.0"),
			SDF.parse("2008-10-03 22:34:35.0"),
			SDF.parse("2008-10-03 22:34:35.0"),
			SDF.parse("2008-10-03 22:34:35.0"));
		checkQuery("select #22:34:35#,#10:34:35 AM#,#10:34:35 pM# from pluto",
			SDF.parse("1899-12-30 22:34:35.0"),
			SDF.parse("1899-12-30 10:34:35.0"),
			SDF.parse("1899-12-30 22:34:35.0"));
	}
	
	public void testQuery() throws SQLException, IOException, ParseException {
		checkQuery("select * from pluto",1234,"I like trippa with spaghettis bolognese",SDF.parse("2008-10-03 22:34:35"),4.5555,5.6666,6.7,5,4.677856);
	}
}
