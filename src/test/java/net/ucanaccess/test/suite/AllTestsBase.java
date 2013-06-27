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
package net.ucanaccess.test.suite;

import net.ucanaccess.test.AccessLikeTest;
import net.ucanaccess.test.AggregateFunctionsTest;
import net.ucanaccess.test.BlobOleTest;
import net.ucanaccess.test.CounterTest;
import net.ucanaccess.test.CreateTableTest;
import net.ucanaccess.test.CrudTest;
import net.ucanaccess.test.DropTableTest;
import net.ucanaccess.test.FunctionsTest;
import net.ucanaccess.test.InsertBigTest;
import net.ucanaccess.test.LoadTypesAccessTest;
import net.ucanaccess.test.MultiThreadAccessTest;
import net.ucanaccess.test.PasswordTest;
import net.ucanaccess.test.TransactionTest;
import net.ucanaccess.test.WorkloadTest;
import net.ucanaccess.test.PivotTest;
import junit.framework.TestSuite;

public class AllTestsBase {
	public static TestSuite suite() throws ClassNotFoundException {
		TestSuite suite = new TestSuite("Test for net.ucanaccess.test");
		suite.addTestSuite(AccessLikeTest.class);
		suite.addTestSuite(CounterTest.class);
		suite.addTestSuite(BlobOleTest.class);
		suite.addTestSuite(CreateTableTest.class);
		suite.addTestSuite(CrudTest.class);
		suite.addTestSuite(DropTableTest.class);
		suite.addTestSuite(FunctionsTest.class);
		suite.addTestSuite(AggregateFunctionsTest.class);
		suite.addTestSuite(LoadTypesAccessTest.class);
		suite.addTestSuite(MultiThreadAccessTest.class);
		suite.addTestSuite(PasswordTest.class);
		suite.addTestSuite(TransactionTest.class);
		suite.addTestSuite(WorkloadTest.class);
		suite.addTestSuite(PivotTest.class);
		suite.addTestSuite(InsertBigTest.class);
		return suite;
	}
}
