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
import java.sql.Connection;
import java.sql.SQLException;


public class MultipleGroupByTest extends UcanaccessTestBase
{
	public void testMultiple() throws SQLException, IOException 
	{
		Connection conn = null;
		
		try
		{
			conn = super.getUcanaccessConnection();
			String wCreateTable = "CREATE TABLE TXXX (F1 VARCHAR, F2 VARCHAR, F3 VARCHAR, F4 VARCHAR, VAL NUMBER)";
			conn.createStatement().executeUpdate(wCreateTable);
			wCreateTable = "CREATE TABLE TABLEXXX_KO (F1,F2,VAL) AS  (SELECT F1 , F2 , SUM(VAL) FROM TXXX GROUP BY F1,F2) WITH DATA";
			conn.createStatement().executeUpdate(wCreateTable);
		}
		
		finally
		{
			if (conn!=null)
				conn.close();
			
		}
	}
}