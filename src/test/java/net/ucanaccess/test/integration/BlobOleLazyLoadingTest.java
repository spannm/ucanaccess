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
package net.ucanaccess.test.integration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2010Test;

@RunWith(Parameterized.class)
public class BlobOleLazyLoadingTest extends AccessVersion2010Test {

	public BlobOleLazyLoadingTest(AccessVersion _accessVersion) {
		super(_accessVersion);
	}

	@Override
	public String getAccessPath() {
		return "testdbs/LazyLoading.accdb";
	}

	// It only works with JRE 1.6 and later (JDBC 3)
	@Test
	public void testBlobOLE() throws SQLException, IOException {
		final long binaryFileSize = 32718;
		byte[] initialBlobBytes = getBlobBytes();
		getLogger().info("BLOB size in backing database before retrieval: {} bytes", initialBlobBytes.length);
		assertTrue(initialBlobBytes.length < binaryFileSize);
		Statement st = ucanaccess.createStatement();
		ResultSet rs = st.executeQuery("SELECT Ole FROM OleTable ORDER BY ID");
		File fl = new File(TEST_DB_TEMP_DIR + "/Copied.jpeg");
		rs.next();
		@SuppressWarnings("unused")
		Object obj = rs.getObject(1);
		InputStream isDB = rs.getBlob(1).getBinaryStream();
		OutputStream outFile = new FileOutputStream(fl);
		byte[] ba = new byte[4096];
		int len;
		while ((len = isDB.read(ba)) != -1) {
			outFile.write(ba, 0, len);
		}
		outFile.flush();
		outFile.close();
		assertEquals(fl.length(), binaryFileSize);
		getLogger().info("file was created in {}.", fl.getAbsolutePath());
		getLogger().info("file size {}.", fl.length());
		fl.delete();
		byte[] finalBlobBytes = getBlobBytes();
		getLogger().info("BLOB size in backing database after retrieval: {} bytes", finalBlobBytes.length);
		if (!Arrays.equals(initialBlobBytes, finalBlobBytes)) {
			getLogger().warn("Simply retrieving BLOB changed byte data in backing database. Problem?");
		}
	}

	private byte[] getBlobBytes() throws SQLException {
		Statement hsqlSt = ucanaccess.getHSQLDBConnection().createStatement();
		ResultSet hsqlRs = hsqlSt.executeQuery("SELECT OLE FROM OLETABLE ORDER BY ID");
		hsqlRs.next();
		byte[] blobBytes = hsqlRs.getBytes(1);
		hsqlRs.close();
		hsqlSt.close();
		return blobBytes;
	}
}
