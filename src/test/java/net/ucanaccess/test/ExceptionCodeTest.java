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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import java.util.Locale;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.jdbc.UcanaccessErrorCodes;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class ExceptionCodeTest extends UcanaccessTestBase {
	
	public ExceptionCodeTest() {
		super();
		Locale.setDefault(Locale.US);
	}

	public ExceptionCodeTest(FileFormat accVer) {
		super(accVer);
		Locale.setDefault(Locale.US);
	}

	@Override
	protected void setUp() throws Exception {

		super.setUp();
		
	}
	
	
	
	
	public void testVUKException() throws SQLException, IOException, ParseException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
		
			st.execute("INSERT INTO T(pk,b)  VALUES( 'pippo',true)");
			st.execute("INSERT INTO T(pk,b)  VALUES( 'pippo',true)");
					
			
			
		}catch(SQLException e){
			
			assertEquals(e.getErrorCode(), -UcanaccessErrorCodes.X_23505);
			assertEquals(e.getSQLState(), "23505");
		}
		
		finally {
			if (st != null)
				st.close();
		}
	}
	
	public void testGenException() throws SQLException, IOException, ParseException {
		Statement st = null;
		try {
			throw new UcanaccessSQLException(ExceptionMessages.CONCURRENT_PROCESS_ACCESS.name(), "ko",11111);
					
			
			
		}catch(SQLException e){
			assertEquals(e.getErrorCode(),11111);
			assertEquals(e.getSQLState(),"ko");
		}
		
		finally {
			if (st != null)
				st.close();
		}
	}
	
	public void testGException() throws SQLException, IOException, ParseException {
		Statement st = null;
		try {
			DriverManager.getConnection(UcanaccessDriver.URL_PREFIX +"ciao ciao");
				
			
			
		}catch(SQLException e){
		
			assertEquals(e.getErrorCode(),UcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR);
			assertEquals(e.getSQLState(),UcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR+"");
		}
		
		finally {
			if (st != null)
				st.close();
		}
	}
	
	public String getAccessPath() {
		return  "net/ucanaccess/test/resources/bool.accdb";
	}
	

	
	
}
