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
			System.out.println("ok1");
			st.execute(getStatement(s.replaceAll("\"", "\"\""),"\""));
			System.out.println("ok2");
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
		System.out.println("delimiter:"+dlm+ " string:"+s);
		return "INSERT INTO reg (descr)  VALUES(  "+dlm+s+dlm+")";
	}
	
	
}
