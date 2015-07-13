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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;

import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.jdbc.UcanaccessConnection;


import com.healthmarketscience.jackcess.Database.FileFormat;

public class ComplexTest extends UcanaccessTestBase {
	
	public ComplexTest() {
		super();
	}
	
	public ComplexTest(FileFormat accVer) {
		super(accVer);
	}
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
	}
	
	@Override
	public String getAccessPath() {
		return "net/ucanaccess/test/resources/2010.accdb";
	}
	
	
	public void testComplex() throws Exception {
		PreparedStatement ps = null;
		try {
			complex0();
			complex1() ;
		} 
		catch(Exception e){throw e;}
		finally {
			if (ps != null)
				ps.close();
		}
	}
		
		private void complex0() throws SQLException, IOException, ParseException {
			PreparedStatement ps = null;
			try {
				ps=super.ucanaccess.prepareStatement("select count(*) from TABLE1 WHERE  contains([MULTI-VALUE-DATA],?)");
				ps.setObject(1, SingleValue.multipleValue("value1","value2"));
				ResultSet rs=ps.executeQuery();
				rs.next();
				assertEquals(2, rs.getInt(1));
				ps.setObject(1, new SingleValue("value1"));
				rs=ps.executeQuery();
				rs.next();
				assertEquals(3, rs.getInt(1));
				ps.close();
				
				ps=super.ucanaccess.prepareStatement("select count(*) from TABLE1 WHERE  EQUALS([MULTI-VALUE-DATA],?)");
				ps.setObject(1, SingleValue.multipleValue("value4","value1"));
				rs=ps.executeQuery();
				rs.next();
				assertEquals(0, rs.getInt(1));
				ps.setObject(1, SingleValue.multipleValue("value1","value4"));
				rs=ps.executeQuery();
				rs.next();
				assertEquals(1, rs.getInt(1));
				ps.close();
				
				ps=super.ucanaccess.prepareStatement("select count(*) from TABLE1 WHERE  EQUALSIGNOREORDER([MULTI-VALUE-DATA],?)");
				ps.setObject(1, SingleValue.multipleValue("value4","value1"));
				rs=ps.executeQuery();
				rs.next();
				assertEquals(1, rs.getInt(1));
				ps.close();
			} finally {
				if (ps != null)
					ps.close();
			}
		}
	
	
	private void complex1() throws Exception {
		PreparedStatement ps = null;
		try {
			dump("select * from Table1");
			checkQuery("select * from Table1");
			ps=super.ucanaccess.prepareStatement("INSERT INTO TABLE1(ID  , [MEMO-DATA] , [APPEND-MEMO-DATA] , [MULTI-VALUE-DATA] , [ATTACH-DATA]) " +
					"VALUES (?,?,?,?,?)");
			
			ps.setString(1, "row12");
			ps.setString(2,"ciao");
			ps.setString(3,"to version");
			SingleValue[] svs=new SingleValue[]{new SingleValue("ccc16"),new SingleValue("ccc24") };
			ps.setObject(4,svs);
			Attachment[] atcs=new Attachment[]{new Attachment(null,"ccc.txt","txt","ddddd ddd".getBytes(), new Date(),null),
					new Attachment(null,"ccczz.txt","txt","ddddd zzddd".getBytes(), new Date(),null) };
			ps.setObject(5,atcs);
			ps.execute();
			dump("select * from Table1");
			checkQuery("select * from Table1");
			ps.close();
			ps=super.ucanaccess.prepareStatement("UPDATE TABLE1 SET [APPEND-MEMO-DATA]='THE CAT' ");
			ps.execute();
			ps.close();
			ps=super.ucanaccess.prepareStatement("UPDATE TABLE1 SET [ATTACH-DATA]=? WHERE ID=?");
			Attachment[] atc;
			ps.setObject(1,atc=new Attachment[]{new Attachment(null,"cccsss.cvs","cvs","ddddd ;sssssssssssssssssssddd".getBytes(), new Date(),null) });
			ps.setString(2, "row12");
			ps.execute();
			
			ps=super.ucanaccess.prepareStatement("select COUNT(*) from Table1 where EQUALS([ATTACH-DATA],?) ");
			ps.setObject(1,atc);
			ResultSet rs=ps.executeQuery();
			rs.next();
			assertEquals( rs.getInt(1), 1);
			ps=super.ucanaccess.prepareStatement("UPDATE TABLE1 SET [MULTi-VALUE-DATA]=? ");
			svs=new SingleValue[]{new SingleValue("aaaaaaa14"),new SingleValue("2eeeeeeeeeee") }; 
			ps.setObject(1,svs);
			ps.execute();
			checkQuery("select * from TABLE1 order by id");
			assertTrue(getCount("select count(*) from TABLE1", true) == 7);
		}
		catch(Exception e){e.printStackTrace(); throw e;}
		finally {
			if (ps != null)
				ps.close();
		}
	}
	
	public void testComplexRoolback() throws SQLException, IOException, ParseException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		PreparedStatement ps = null;
		int i=getCount("select count(*) from TABLE1", true);
		try {
			
			super.ucanaccess.setAutoCommit(false);
			
			Method mth=UcanaccessConnection.class.getDeclaredMethod("setTestRollback", new Class[]{boolean.class});
			mth.setAccessible(true);
			mth.invoke(super.ucanaccess, new Object[]{Boolean.TRUE});
			ps=super.ucanaccess.prepareStatement("INSERT INTO TABLE1(ID  , [MEMO-DATA] , [APPEND-MEMO-DATA] , [MULTI-VALUE-DATA] , [ATTACH-DATA]) " +
					"VALUES (?,?,?,?,?)");
			
			ps.setString(1, "row123");
			ps.setString(2,"ciao");
			ps.setString(3,"to version");
			SingleValue[] svs=new SingleValue[]{new SingleValue("16"),new SingleValue("24") };
			ps.setObject(4,svs);
			Attachment[] atcs=new Attachment[]{new Attachment(null,"ccc.txt","txt","ddddd ddd".getBytes(), new Date(),null),
					new Attachment(null,"ccczz.txt","txt","ddddd zzddd".getBytes(), new Date(),null) };
			ps.setObject(5,atcs);
			ps.execute();
			ps.close();
			ps=super.ucanaccess.prepareStatement("UPDATE TABLE1 SET [APPEND-MEMO-DATA]='THE BIG BIG CAT' WHERE ID='row12' ");
			ps.execute();
			ps.close();
			dump("select * from TABLE1");
			super.ucanaccess.commit();
			 checkQuery("select * from TABLE1 order by id");
		
		} catch (Throwable e){
			e.printStackTrace();
		}
		finally {
			if (ps != null)
				ps.close();
		}
		this.ucanaccess=super.getUcanaccessConnection();
		dump("select * from TABLE1");
		 checkQuery("select * from TABLE1  WHERE ID='row12' order by id");
		 assertTrue(getCount("select count(*) from TABLE1", true) == i);
	}
}
