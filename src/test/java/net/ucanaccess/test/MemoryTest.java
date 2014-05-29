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
import com.healthmarketscience.jackcess.Database.FileFormat;

public class MemoryTest extends UcanaccessTestBase {

	public MemoryTest() {
		super();
	}

	public MemoryTest(FileFormat accVer) {
		super(accVer);
	}

	protected void setUp() throws Exception {
	
		
		setInactivityTimeout(1);
		super.setUp();
		executeCreateTable("CREATE TABLE memm( id LONG PRIMARY KEY,A LONG , C TEXT,D TEXT) ");

	}

	public void testMemory() throws SQLException, IOException, InterruptedException {
		super.ucanaccess.setAutoCommit(false);
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();

			System.out.println("total memory 0="+Runtime.getRuntime().totalMemory());
			System.out.println("free memory 0="+Runtime.getRuntime().freeMemory());
			for (int i = 0; i <= 100000; i++)
				st.execute("INSERT INTO memm(id,a,c,d) VALUES (" + i
						+ ",'33','booddddddddddddddddddddddddddddddo','dddddddddddddddddsssssssssssssssdddd' )");
			super.ucanaccess.commit();
			super.ucanaccess.close();
			super.ucanaccess=null;
			long occ=Runtime.getRuntime().freeMemory();
			System.out.println("total memory 1="+Runtime.getRuntime().totalMemory());
			System.out.println("free memory 1="+occ);
			Thread.sleep(61000);
		  
			System.out.println("total memory 2="+Runtime.getRuntime().totalMemory());
			System.out.println("free memory 2="+Runtime.getRuntime().freeMemory());
			System.out.println("free memory diff ="+(Runtime.getRuntime().freeMemory()-occ));
			//super.ucanaccess=super.getUcanaccessConnection();
			super.ucanaccess=super.getUcanaccessConnection();
			
			dump("select * from memm limit 10");
			
//			for (int i=0;i<3;i++){
//				Thread.sleep(60000);
//				super.ucanaccess=super.getUcanaccessConnection();
//				
//				dump("select * from memm");
//				super.ucanaccess.close();
//			}
			
			
		} finally {
			if (st != null)
				st.close();
		}
	}

}
