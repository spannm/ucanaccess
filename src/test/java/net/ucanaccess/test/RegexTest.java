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

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class RegexTest extends UcanaccessTestBase {
	public static boolean tableCreated;
	
	public RegexTest() {
		super();
	}
	
	public RegexTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		 executeCreateTable("CREATE TABLE reg (id COUNTER,descr memo) ");
		
	}
	
	public void testRegex() throws SQLException, IOException {
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			String s="";
			for(int i=0;i<5000;i++){
				s+="C";
			}
			
			String[] in =new String[]{"","\"\"'tCC",s,s+"'",s+"\"",s+"\"''t","\"'\"t"+s, "ss\"1234567890wwwwwwwwww1","ssss'DDDD",s+"\"\"\""+s} ;	
			for(String c:in){
				executeStatement(c);
			}
			String[][] out=new String[in.length*2][1];
			int k=0;
			for(int j=0;j<out.length;j++){
				out[j][0]=in[k];
				if(j%2==1)k++;
			}
			checkQuery("select descr from reg order by id asc ",out);
			
		} finally {
			if (st != null)
				st.close();
		}
	}
	private void executeStatement(String s) throws SQLException{
		Statement st = null;
		try {
			st = super.ucanaccess.createStatement();
			st.execute(getStatement(s.replaceAll("'", "''"),"'"));
			
			st.execute(getStatement(s.replaceAll("\"", "\"\""),"\""));
			
		} 
		catch(SQLException sqle ){
			System.err.println(getStatement(s,"\""));
			System.err.println("converted sql:"+super.ucanaccess.nativeSQL(getStatement(s.replaceAll("\"", "\"\""),"\"")));
			throw sqle;
		}
		finally {
			if (st != null)
				st.close();
		}
	}
	
	
	private String getStatement(String s,String dlm){
		
		return "INSERT INTO reg (descr)  VALUES(  "+dlm+s+dlm+")";
	}
	
	
}
