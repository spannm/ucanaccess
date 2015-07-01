package net.ucanaccess.jdbc;

import java.util.HashMap;

public class NormalizedSQL {
	private String sql;
	private HashMap<String,String> aliases=new HashMap<String,String> ();
	
	
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public HashMap<String,String> getAliases() {
		return aliases;
	}
	public String put(String key, String value) {
		return aliases.put(key, value);
	}
	@Override
	public String toString() {
		return sql;
	}
	
	

}
