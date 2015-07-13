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

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Locale;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class AggregateFunctionsTest extends UcanaccessTestBase {
	private static boolean init;
	

	public AggregateFunctionsTest() {
		super();
		Locale.setDefault(Locale.US);
	}

	public AggregateFunctionsTest(FileFormat accVer) {
		super(accVer);
		Locale.setDefault(Locale.US);
	}

	@Override
	protected void setUp() throws Exception {

		super.setUp();
		if (!init) {
			Statement st = null;
			
				st = super.ucanaccess.createStatement();
				st
						.executeUpdate("CREATE TABLE t235 (id INTEGER,descr text(400), num numeric(12,3), date0 datetime) ");
				st.close();
				st = super.ucanaccess.createStatement();
				st
						.execute("INSERT INTO t235 (id,descr,num,date0)  VALUES( 1234,'Show must go off',-1110.55446,#11/22/2003 10:42:58 PM#)");
				st
				.execute("INSERT INTO t235 (id,descr,num,date0)  VALUES( 12344,'Show must go up and down',-113.55446,#11/22/2003 10:42:58 PM#)");
				st.close();
				init = true;
			

		}

	}

	

	public void testDCount() throws SQLException, IOException, ParseException {
		

		checkQuery("select id  , DCount('*','t235','1=1') from [t235]",
				new Object[][] { { 1234, 2 }, { 12344, 2 } });
		checkQuery("select id as [WW \"SS], DCount('descr','t235','1=1')from t235",
				new Object[][] { { 1234, 2 }, { 12344, 2 } });
		checkQuery("select  DCount('*','t235','1=1') ", 2);

	}

	public void testDSum() throws SQLException, IOException, ParseException {
		checkQuery("select  DSum('id','t235','1=1') ", 13578);
	}

	public void testDMax() throws SQLException, IOException, ParseException {
		checkQuery("select  DMax('id','t235') ", 12344);
	}

	public void testDMin() throws SQLException, IOException, ParseException {
		checkQuery("select  DMin('id','t235') ", 1234);
	}

	public void testDAvg() throws SQLException, IOException, ParseException {
		checkQuery("select  DAvg('id','t235') ", 6789);
	}

	public void testLast() throws SQLException, IOException, ParseException {
		checkQuery("select  last(descr) from t235", "Show must go up and down");
		checkQuery("select  last(NUM) from t235", -113.5540);

	}

	public void testFirst() throws SQLException, IOException, ParseException {
		checkQuery("select  first(descr) from t235", "Show must go off");
		checkQuery("select  first(NUM) from t235", -1110.5540);

	}

	public void testDLast() throws SQLException, IOException, ParseException {
		checkQuery("select  DLast('descr','t235') ", "Show must go up and down");
	}

	public void testDFirst() throws SQLException, IOException, ParseException {
		checkQuery("select  DFIrst('descr','t235') ", "Show must go off");
	}

}
