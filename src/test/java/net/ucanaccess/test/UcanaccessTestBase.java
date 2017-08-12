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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.DatabaseBuilder;

import junit.framework.TestCase;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.console.Main;
import net.ucanaccess.jdbc.UcanaccessDriver;

public abstract class UcanaccessTestBase extends TestCase {
	private static FileFormat defaultFileFormat = FileFormat.V2003;
	private static File fileAccdb;
	private static Class<?extends TestCase> testingClass;
	

	static {
		Main.setBatchMode(true);
		
	}
	
	public static void setDefaultFileFormat(FileFormat defaultFileFormat) {
		UcanaccessTestBase.defaultFileFormat = defaultFileFormat;
	}
	
	private FileFormat fileFormat;
	private String password = "";
	protected Connection ucanaccess;
	private String user = "ucanaccess";
	protected Connection verifyConnection;
	private Boolean ignoreCase;
	private int inactivityTimeout=-1;
	private String columnOrder;
	private String append2JdbcURL="";
	private Boolean showSchema;
	Boolean getShowSchema() {
		return showSchema;
	}

	void setShowSchema(Boolean showSchema) {
		this.showSchema = showSchema;
	}

	private static  ArrayList<String> tableCreated=new   ArrayList<String>();
	
	public UcanaccessTestBase() {
		this(defaultFileFormat);
	}
	
	public UcanaccessTestBase(FileFormat accVer) {
		super();
		this.fileFormat = accVer;
		//TODO look for a cleaner way to run test suites under a specific Locale
		//java.util.Locale.setDefault(new java.util.Locale("tr"));
	}
	

	
	public void checkQuery(String expression, Object[][] resulMatrix)
			throws SQLException, IOException {
		Statement st = null;
		ResultSet myRs = null;
		try {
			
			st = ucanaccess.createStatement();
			myRs = st.executeQuery(expression);
			diff(myRs, resulMatrix);
		} finally {
			if (myRs != null)
				myRs.close();
			if (st != null)
				st.close();
		}
	}
	
	
	public void checkQuery(String expression) throws SQLException, IOException {
		Statement st = null;
		ResultSet myRs = null;
		Statement st1 = null;
		ResultSet joRs = null;
		try {
			this.initVerifyConnection();
			st = ucanaccess.createStatement();
			myRs = st.executeQuery(expression);
			st1 = verifyConnection.createStatement();
			joRs = st1.executeQuery(expression);
			diff(myRs, joRs);
		} finally {
			if (myRs != null)
				myRs.close();
			if (st != null)
				st.close();
			if (joRs != null)
				joRs.close();
			if (st1 != null)
				st1.close();
			if (verifyConnection != null)
				verifyConnection.close();
		}
	}
	
	public void checkQuery(String expression, Object... rowResult)
			throws SQLException, IOException {
		checkQuery(expression,new Object[][] {  rowResult  });
	}
	
	 public void executeCreateTable(String createTableStatement ) throws SQLException{
		 executeCreateTable(createTableStatement,0);
	 }
	
    public void executeCreateTable(String createTableStatement,Integer succ) throws SQLException{

    	if (!tableCreated.contains(this.getClass().getName()+succ)) {
			Statement st = null;
			try {
				st =this.ucanaccess.createStatement();
				st.execute(createTableStatement);
				tableCreated.add(this.getClass().getName()+succ);
			} finally {
				if (st != null)
					st.close();
			}
		}
    }
	
	private void diff(ResultSet myRs, Object[][] resulMatrix)
			throws SQLException, IOException {
		ResultSetMetaData mymeta = myRs.getMetaData();
		int mycolmax = mymeta.getColumnCount();
		if (resulMatrix.length > 0)
			assertEquals(mycolmax, resulMatrix[0].length);
		int j = 0;
		while (myRs.next()) {
			for (int i = 0; i < mycolmax; ++i) {
				Object ob1 = myRs.getObject(i + 1);
				assertTrue("matrix with different length was expected: "
						+ resulMatrix.length + " not" + j,
						j < resulMatrix.length);
				Object ob2 = resulMatrix[j][i];
				if (ob1 == null) {
					assertTrue((ob2 == null));
				} else {
					if (ob1 instanceof Blob) {
						Blob blob = (Blob) ob1;
						InputStream bs = blob.getBinaryStream();
						ByteArrayOutputStream bos = new ByteArrayOutputStream();
						byte[] bt = new byte[4096];
						int len;
						while ((len = bs.read(bt)) != -1) {
							bos.write(bt, 0, len);
						}
						bt = bos.toByteArray();
						byte[] btMtx = (byte[]) ob2;
						for (int y = 0; y < btMtx.length; y++) {
							assertEquals(btMtx[y], bt[y]);
						}
					} else {
						if (ob1 instanceof Number && ob2 instanceof Number) {
							BigDecimal ob1b = new BigDecimal(ob1.toString());
							BigDecimal ob2b = new BigDecimal(ob2.toString());
							ob1 = ob1b.doubleValue();
							ob2 = ob2b.doubleValue();
						}
						if (ob1 instanceof Date && ob2 instanceof Date) {
							ob1 = ((Date) ob1).getTime();
							ob2 = ((Date) ob2).getTime();
						}
						assertEquals(ob2, ob1);  // 2016-05-21 [gord] - swapped order so "expected" and "actual" reported correctly
					}
				}
			}
			j++;
		}
		assertEquals("matrix with different length was expected ",
				resulMatrix.length, j);
	}
	
	public void diff(ResultSet myRs, ResultSet joRs) throws SQLException,
			IOException {
		ResultSetMetaData mymeta = myRs.getMetaData();
		int mycolmax = mymeta.getColumnCount();
		ResultSetMetaData jometa = joRs.getMetaData();
		int jocolmax = jometa.getColumnCount();
		assertTrue(jocolmax == mycolmax);
		StringBuffer log = new StringBuffer("{");
		while (next(joRs, myRs)) {
			if (log.length() > 1)
				log.append(",");
			log.append("{");
			for (int i = 0; i < mycolmax; ++i) {
				if (i > 0)
					log.append(",");
				Object ob1 = myRs.getObject(i + 1);
				Object ob2 = joRs.getObject(i + 1);
				log.append(print(ob2));
				if (ob1 == null) {
					assertTrue((ob2 == null));
				} else {
					if (ob1 instanceof Blob) {
						Blob blob = (Blob) ob1;
						InputStream bs = blob.getBinaryStream();
						ByteArrayOutputStream bos = new ByteArrayOutputStream();
						byte[] bt = new byte[4096];
						int len;
						while ((len = bs.read(bt)) != -1) {
							bos.write(bt, 0, len);
						}
						bt = bos.toByteArray();
						byte[] btodbc = (byte[]) ob2;
						for (int y = 0; y < btodbc.length; y++) {
							assertEquals(btodbc[y], bt[y]);
						}
					} 
					else if (ob1 instanceof ComplexBase[]&& ob2 instanceof ComplexBase[]) {
						assertTrue(Arrays.equals((ComplexBase[])ob1, (ComplexBase[])ob2));
					}
					
					else {
						if (ob1 instanceof Number && ob2 instanceof Number) {
							BigDecimal ob1b = new BigDecimal(ob1.toString());
							BigDecimal ob2b = new BigDecimal(ob2.toString());
							ob1 = ob1b.doubleValue();
							ob2 = ob2b.doubleValue();
						}
						if (ob1 instanceof Date && ob2 instanceof Date) {
							ob1 = ((Date) ob1).getTime();
							ob2 = ((Date) ob2).getTime();
						}
						assertEquals(ob1, ob2);
					}
				}
			}
			log.append("}");
		}
		log.append("}");
		
	}
	
	private String print(Object ob2) {
		if (ob2 == null) {
			return null;
		}
		if (ob2 instanceof String) {
			return "\"" + ob2 + "\"";
		}
		return ob2.toString();
	}
	
	public void dump(ResultSet rs) throws SQLException {
		new Main(ucanaccess, null).consoleDump(rs, System.out);
	}
	
	public void dump(String expression) throws SQLException, IOException {
		Statement st = null;
		ResultSet myRs = null;
		try {
			Connection conn = ucanaccess;
			st = conn.createStatement();
			myRs = st.executeQuery(expression);
			dump(myRs);
		} finally {
			if (myRs != null)
				myRs.close();
			if (st != null)
				st.close();
		}
	}
	
	
	public void dumpVerify(String expression) throws SQLException, IOException {
		Statement st = null;
		ResultSet myRs = null;
		try {
			Connection conn = this.verifyConnection;
			st = conn.createStatement();
			myRs = st.executeQuery(expression);
			dump(myRs);
		} finally {
			if (myRs != null)
				myRs.close();
			if (st != null)
				st.close();
		}
	}
	

	
	public String getAccessPath() {
		return null;
	}
	
	public String getAccessTempPath() throws IOException {
		if (fileAccdb == null||!this.getClass().equals(testingClass)) {
			testingClass=this.getClass();
			if (this.getAccessPath() == null) {
				fileAccdb = File.createTempFile("test", ".accdb");
				Database db = DatabaseBuilder.create(this.fileFormat, fileAccdb);
				db.flush();
				db.close();
				System.out.println("Access file version " + this.fileFormat
						+ " created: " + fileAccdb.getAbsolutePath());
			} else {
				fileAccdb = copyResourceInTemp(this.getAccessPath());;
			}
		}
		return fileAccdb.getAbsolutePath();
	}
	
	
	protected File copyResourceInTemp(String resourcePath) throws IOException{
		InputStream is = this.getClass().getClassLoader()
				.getResourceAsStream(resourcePath);
		byte[] buffer = new byte[4096];
		File temp = File.createTempFile("tempJunit", ".accdb");
		System.out.println("Resource file: "+resourcePath+" copied in "+temp.getAbsolutePath());
		FileOutputStream fos = new FileOutputStream(temp);
		int bread;
		while ((bread = is.read(buffer)) != -1) {
			fos.write(buffer, 0, bread);
		}
		fos.flush();
		fos.close();
		is.close();
		return temp;
	}
	
	public int getCount(String sql) throws SQLException, IOException {
		return this.getCount(sql, true);
	}
	
	public int getCount(String sql, boolean equals) throws SQLException,
			IOException {
		//initJdbcOdbcConnection();
		this.initVerifyConnection();
		Statement st = this.verifyConnection.createStatement();
		ResultSet joRs = st.executeQuery(sql);
		joRs.next();
		int count = joRs.getInt(1);
		st = this.ucanaccess.createStatement();
		ResultSet myRs = st.executeQuery(sql);
		myRs.next();
		int myCount = myRs.getInt(1);
		if (equals)
			assertEquals(count, myCount);
		else
			assertFalse(count == myCount);
		return count;
	}
	
	@Override
	public String getName() {
		return super.getName() + " ver " + this.fileFormat;
	}
	
	String getPassword() {
		return password;
	}
	
	protected Connection getUcanaccessConnection() throws SQLException,
			IOException {
		String url=UcanaccessDriver.URL_PREFIX
		+ getAccessTempPath();
		if(this.ignoreCase!=null)url+=";ignoreCase="+this.ignoreCase;
		if(this.inactivityTimeout!=-1)url+=";inactivityTimeout="+this.inactivityTimeout;
		if(this.columnOrder!=null)url+=";columnOrder="+this.columnOrder;
		if(this.showSchema!=null)url+=";showSchema="+this.showSchema;
		url+=append2JdbcURL;
		return DriverManager.getConnection(url, this.user, this.password);
	}
	protected void append2JdbcURL(String s){
		append2JdbcURL+=s;
	}
	
	
	protected void initVerifyConnection() throws SQLException, IOException {
		InputStream is = new FileInputStream(fileAccdb);
		byte[] buffer = new byte[4096];
		File tempVer = File.createTempFile("tempJunit", ".mdb");
		FileOutputStream fos = new FileOutputStream(tempVer);
		int bread;
		while ((bread = is.read(buffer)) != -1) {
			fos.write(buffer, 0, bread);
		}
		fos.flush();
		fos.close();
		is.close();
		this.verifyConnection = DriverManager.getConnection(
				UcanaccessDriver.URL_PREFIX + tempVer.getAbsolutePath(),
				this.user, this.password);
	}
	
	private boolean next(ResultSet joRs, ResultSet myRs) throws SQLException {
		boolean b1 = joRs.next();
		boolean b2 = myRs.next();
		assertEquals(b1, b2);
		return b1;
	}
	
	public void setInactivityTimeout(int inactivityTimeout) {
		this.inactivityTimeout = inactivityTimeout;
	}

	void setPassword(String password) {
		this.password = password;
	}
	
	void setColumnOrder(String columnOrder) {
		this.columnOrder = columnOrder;
	}
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
		try {
			this.ucanaccess = this.getUcanaccessConnection();
		} catch (Error e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected void tearDown() throws Exception {
		if (this.ucanaccess != null)
			this.ucanaccess.close();
	}

	public void setIgnoreCase(boolean ignoreCase) {
		this.ignoreCase=ignoreCase;
		
	}
}
