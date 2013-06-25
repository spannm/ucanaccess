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

import java.sql.SQLException;

import net.ucanaccess.util.Logger;

public class UcanaccessSQLException extends SQLException {
	public enum ExceptionMessages{
		CONCURRENT_PROCESS_ACCESS,
		INVALID_CREATE_STATEMENT,
		INVALID_INTERVAL_VALUE,
		INVALID_JACKCESS_OPENER,
		INVALID_MONTH_NUMBER, 
		NOT_A_VALID_PASSWORD, 
		ONLY_IN_MEMORY_ALLOWED, 
		UNPARSABLE_DATE,
		COMPLEX_TYPE_UNSUPPORTED, INVALID_PARAMETER
	}
	private static final long serialVersionUID = -1432048647665807662L;
	private Throwable cause;
	public UcanaccessSQLException() {
	}
	
	public UcanaccessSQLException(ExceptionMessages reason) {
		super(Logger.getMessage(reason.name()));
	}
	
	public UcanaccessSQLException(String reason, String SQLState) {
		super(Logger.getMessage(reason), SQLState);
	}
	
	public UcanaccessSQLException(String reason, String SQLState, int vendorCode) {
		super(Logger.getMessage(reason), SQLState, vendorCode);
	}
	
	public UcanaccessSQLException(String reason, String sqlState,
			int vendorCode, Throwable cause) {
		super(Logger.getMessage(reason), sqlState, vendorCode, cause);
	}
	
	public UcanaccessSQLException(String reason, String sqlState, Throwable cause) {
		super(Logger.getMessage(reason), sqlState, cause);
	}
	
	public UcanaccessSQLException(String reason, Throwable cause) {
		super(Logger.getMessage(reason), cause);
	}
	
	public UcanaccessSQLException(Throwable cause) {
		
		super( cause.getMessage());
		this.cause=cause;
		
	}

	public Throwable getCause() {
		return this.cause;
	}
}
