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
package net.ucanaccess.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

public class UcanaccessBlob implements Blob {
	private Blob blob;
	
	public UcanaccessBlob(Blob blob) {
		super();
		this.blob = blob;
	}
	
	public void free() throws SQLException {
		try {
			blob.free();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public InputStream getBinaryStream() throws SQLException {
		try {
			return blob.getBinaryStream();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public InputStream getBinaryStream(long pos, long length)
			throws SQLException {
		try {
			return blob.getBinaryStream(pos, length);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public byte[] getBytes(long pos, int length) throws SQLException {
		try {
			return blob.getBytes(pos, length);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public long length() throws SQLException {
		try {
			return blob.length();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public long position(Blob pattern, long start) throws SQLException {
		try {
			return blob.position(pattern, start);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public long position(byte[] pattern, long start) throws SQLException {
		try {
			return blob.position(pattern, start);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public OutputStream setBinaryStream(long pos) throws SQLException {
		try {
			return blob.setBinaryStream(pos);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		try {
			return blob.setBytes(pos, bytes);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public int setBytes(long pos, byte[] bytes, int offset, int len)
			throws SQLException {
		try {
			return blob.setBytes(pos, bytes, offset, len);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public void truncate(long len) throws SQLException {
		try {
			blob.truncate(len);
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
}
