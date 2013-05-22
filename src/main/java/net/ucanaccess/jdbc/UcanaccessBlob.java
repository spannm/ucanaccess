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
