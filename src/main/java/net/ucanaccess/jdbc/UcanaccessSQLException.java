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

import java.sql.SQLException;

import org.hsqldb.error.ErrorCode;


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
		COMPLEX_TYPE_UNSUPPORTED, 
		INVALID_PARAMETER,
		INVALID_TYPES_IN_COMBINATION,
		UNSUPPORTED_TYPE,
		STATEMENT_DDL,
		CLOSE_ON_COMPLETION_STATEMENT, ACCESS_97,
		PARAMETER_NULL,
		TABLE_DOESNT_EXIST
		
	}
	private static final long serialVersionUID = -1432048647665807662L;
	private Throwable cause;
	private int errorCode;
	private String sqlState;
	
	
	public UcanaccessSQLException() {
		
	}
	
	private String versionMessage(String message){
		if(message!=null&&message.startsWith("UCAExc:"))
			return message;
		String version= this.getClass().getPackage().getImplementationVersion();
		version=(version==null)?"3.x.x ":version+" ";
		version="UCAExc:::"+version;
		return version+message;
	}
	
	@Override
	public String getLocalizedMessage() {
		
		return versionMessage(super.getLocalizedMessage());
	}

	@Override
	public String getMessage() {
		return versionMessage(super.getMessage());
	}

	public UcanaccessSQLException(ExceptionMessages reason) {
		super(Logger.getMessage(reason.name()));
		this.sqlState=String.valueOf(UcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR);
		this.errorCode=UcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR;
	}
	
	public UcanaccessSQLException(ExceptionMessages reason,Object... pars) {
		super(Logger.getMessage(reason.name(),pars));
		this.sqlState=String.valueOf(UcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR);
		this.errorCode=UcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR;
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
		super( explaneCause(cause));
		this.cause=cause;
		if(cause instanceof SQLException){
			SQLException se=(SQLException)cause;
			this.errorCode=se.getErrorCode();
			this.sqlState=se.getSQLState();
		}else{
			this.sqlState=String.valueOf(UcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR);
			this.errorCode=UcanaccessErrorCodes.UCANACCESS_GENERIC_ERROR;
		}
	}
	
	public static String explaneCause(Throwable cause){
		if(cause instanceof SQLException){
			SQLException se=(SQLException)cause;
			if(se.getErrorCode()== -ErrorCode.X_42562){
				return cause.getMessage()+" "+Logger.getMessage(ExceptionMessages.INVALID_TYPES_IN_COMBINATION.name());
			}
		}
		return cause.getMessage();
	}

	public Throwable getCause() {
		return this.cause;
	}

	@Override
	public int getErrorCode() {
		return this.errorCode==0?super.getErrorCode():this.errorCode;
	}

	@Override
	public String getSQLState() {
		return this.sqlState==null?super.getSQLState():this.sqlState;
	}
	
	
}
