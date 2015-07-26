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



import com.healthmarketscience.jackcess.Database.FileFormat;

public class ConcatTest extends UcanaccessTestBase {
	public ConcatTest() {
		super();
	}
	
	public ConcatTest(FileFormat accVer) {
		super(accVer);
	}
	
	public String getAccessPath() {
		return "net/ucanaccess/test/resources/baddb.accdb";
	}
	
	
	
	protected void setUp() throws Exception {
		this.append2JdbcURL(";concatnulls=true");
	}
	
	
	public void testConcat() throws Exception {
		Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
		this.ucanaccess= getUcanaccessConnection();
		
		checkQuery("select 'aa2'& null from dual",new Object[][]{{null}});
		
		
		
		
	}
}
