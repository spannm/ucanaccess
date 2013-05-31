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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class BlobOleTest extends UcanaccessTestBase {
	
	public BlobOleTest() {
		super();
	}
	
	public BlobOleTest(FileFormat accVer) {
		super(accVer);
	}
	
	protected void setUp() throws Exception {
		super.setUp();
			executeCreateTable("CREATE TABLE T2 (id COUNTER primary key, descr TEXT(400), pippo OLE)");
	}
	
	// It only works with JRE 1.6 and later (JDBC 3)
	public void testBlobOLE() throws SQLException, IOException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			Blob blob = super.ucanaccess.createBlob();
			OutputStream out = blob.setBinaryStream(1);
			InputStream is = this.getClass().getClassLoader()
					.getResourceAsStream("net/ucanaccess/test/resources/elisaArt.JPG");
			byte[] ba = new byte[4096];
			int len;
			while ((len = is.read(ba)) != -1) {
				out.write(ba, 0, len);
			}
			out.flush();
			out.close();
			ps = super.ucanaccess
					.prepareStatement("INSERT INTO T2 (descr,pippo)  VALUES( ?,?)");
			ps.setString(1, "TestOle");
			ps.setBlob(2, blob);
			ps.execute();
			Statement st = super.ucanaccess.createStatement();
			rs = st.executeQuery("select Pippo from T2");
			rs.next();
			InputStream isDB = rs.getBinaryStream(1);
			File fl = new File("CopyElisaArt.JPG");
			OutputStream outFile = new FileOutputStream(fl);
			ByteArrayOutputStream outByte = new ByteArrayOutputStream();
			ba = new byte[4096];
			while ((len = isDB.read(ba)) != -1) {
				outFile.write(ba, 0, len);
				outByte.write(ba, 0, len);
			}
			outFile.flush();
			outFile.close();
			System.out.println("CopyElisaArt.JPG was created in "
					+ fl.getAbsolutePath());
			outByte.flush();
			outByte.close();
			checkQuery("select * from T2", new Object[][] { { 1, "TestOle",
					outByte.toByteArray() } });
			ps = super.ucanaccess
					.prepareStatement("UPDATE T2 SET descr=? WHERE  descr=?");
			ps.setString(1, "TestOleOk");
			ps.setString(2, "TestOle");
			ps.executeUpdate();
			checkQuery("select * from T2",  1, "TestOleOk",	outByte.toByteArray() );
			ps = super.ucanaccess
					.prepareStatement("DELETE FROM  t2  WHERE  descr=?");
			ps.setString(1, "TestOleOk");
			ps.executeUpdate();
			checkQuery("select * from T2", new Object[][] {});
		} finally {
			if (rs != null)
				rs.close();
			if (ps != null)
				ps.close();
		}
	}
}
