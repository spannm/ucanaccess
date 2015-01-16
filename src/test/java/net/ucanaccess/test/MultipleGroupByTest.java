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