package net.ucanaccess.jdbc;

import java.sql.SQLWarning;

public class UcanaccessSQLWarning extends  SQLWarning{
	private static final long serialVersionUID = -4626457418782839303L;
	private SQLWarning nextWarning;
	

	public UcanaccessSQLWarning(String message) {
		super(message);
	}

	public SQLWarning getNextWarning() {
		return nextWarning;
	}

	public void setNextWarning(SQLWarning warning) {
		this.nextWarning=warning;
	}

	
	

}
