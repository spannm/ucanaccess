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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import net.ucanaccess.jdbc.UcanaccessDriver;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class FolderTest extends UcanaccessTestBase {
	public FolderTest() {
		super();
	}
	
	public FolderTest(FileFormat accVer) {
		super(accVer);
	}
	

	protected void setUp() throws Exception {
		//super.setIgnoreCase(true);
		//ignorecase=true is the default
		super.setUp();
	}
	
	
	public void testFolderContent() throws SQLException, IOException, ClassNotFoundException {
		Statement st = null;
		String folderPath=System.getProperty("accessFolder");
		if(folderPath==null)return;
		File folder=new File(folderPath);
		Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
		for(File fl:folder.listFiles())
		
		try {
			String url=UcanaccessDriver.URL_PREFIX
			+ fl.getAbsolutePath();
			Connection conn=DriverManager.getConnection(url  );
			SQLWarning sqlw= conn.getWarnings();
			System.out.println("open "+fl.getAbsolutePath());
			while(sqlw!=null){
				System.out.println(sqlw.getMessage());
				sqlw=sqlw.getNextWarning();	
			}
		
			
		} 
		catch(Exception e){
			System.out.println("error "+fl.getAbsolutePath());
		}
		finally {
			if (st != null)
				st.close();
		}
	}
}
