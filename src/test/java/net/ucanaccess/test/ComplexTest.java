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
	
	
	public void testComplex() throws SQLException, IOException, ParseException {
		PreparedStatement ps = null;
		try {
			
			ps=super.ucanaccess.prepareStatement("INSERT INTO TABLE1(ID  , MEMO_DATA , APPEND_MEMO_DATA , MULTI_VALUE_DATA , ATTACH_DATA) " +
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
			checkQuery("select * from Table1");
			ps.close();
			ps=super.ucanaccess.prepareStatement("UPDATE TABLE1 SET APPEND_MEMO_DATA='THE CAT' ");
			ps.execute();
			ps.close();
			ps=super.ucanaccess.prepareStatement("UPDATE TABLE1 SET ATTACH_DATA=? ");
			Attachment[] atc;
			ps.setObject(1,atc=new Attachment[]{new Attachment(null,"cccsss.cvs","cvs","ddddd ;sssssssssssssssssssddd".getBytes(), new Date(),null) });
			ps.execute();
			ps=super.ucanaccess.prepareStatement("select * from Table1 where ATTACH_DATA=? ");
			ps.setObject(1,atc);
			ResultSet rs=ps.executeQuery();
			dump(rs);
			ps=super.ucanaccess.prepareStatement("UPDATE TABLE1 SET MULTI_VALUE_DATA=? ");
			svs=new SingleValue[]{new SingleValue("aaaaaaa14"),new SingleValue("2eeeeeeeeeee") }; 
			ps.setObject(1,svs);
			ps.execute();
			checkQuery("select * from TABLE1 order by id");
			assertTrue(getCount("select count(*) from TABLE1", true) == 7);
		} finally {
			if (ps != null)
				ps.close();
		}
	}
	
	public void testComplexRoolback() throws SQLException, IOException, ParseException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		PreparedStatement ps = null;
		try {
			super.ucanaccess.setAutoCommit(false);
			Method mth=UcanaccessConnection.class.getDeclaredMethod("setTestRollback", new Class[]{boolean.class});
			mth.setAccessible(true);
			mth.invoke(super.ucanaccess, new Object[]{Boolean.TRUE});
			ps=super.ucanaccess.prepareStatement("INSERT INTO TABLE1(ID  , MEMO_DATA , APPEND_MEMO_DATA , MULTI_VALUE_DATA , ATTACH_DATA) " +
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
			ps=super.ucanaccess.prepareStatement("UPDATE TABLE1 SET APPEND_MEMO_DATA='THE BIG BIG CAT' WHERE ID='row12' ");
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
		 assertTrue(getCount("select count(*) from TABLE1", true) == 7);
	}
}
