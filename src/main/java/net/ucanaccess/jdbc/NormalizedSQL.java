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
