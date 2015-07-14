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
package net.ucanaccess.test.suite;


import net.ucanaccess.test.AccessLikeTest;
import net.ucanaccess.test.AddFunctionTest;
import net.ucanaccess.test.AggregateFunctionsTest;
import net.ucanaccess.test.BatchTest;
import net.ucanaccess.test.BlobOleTest;
import net.ucanaccess.test.BooleanTest;
import net.ucanaccess.test.CounterTest;
import net.ucanaccess.test.CreateTableTest;
import net.ucanaccess.test.CrudTest;
import net.ucanaccess.test.DropTableTest;
import net.ucanaccess.test.ExceptionCodeTest;
import net.ucanaccess.test.ExternalResourcesTest;
import net.ucanaccess.test.FunctionsTest;
import net.ucanaccess.test.GeneratedKeysTest1;
import net.ucanaccess.test.InsertBigTest;
import net.ucanaccess.test.LoadTypesAccessTest;
import net.ucanaccess.test.MultiThreadAccessTest;
import net.ucanaccess.test.MultipleGroupByTest;
import net.ucanaccess.test.PasswordTest;
import net.ucanaccess.test.NoRomanCharacterTest;
import net.ucanaccess.test.RegexTest;
import net.ucanaccess.test.Size97Test;
import net.ucanaccess.test.TransactionTest;
import net.ucanaccess.test.WorkloadTest;
import net.ucanaccess.test.PivotTest;
import net.ucanaccess.test.ByteTest;
import net.ucanaccess.test.GeneratedKeysTest;
import net.ucanaccess.test.ColumnOrderTest;
import junit.framework.TestSuite;

public class AllTestsBase {
	public static TestSuite suite() throws ClassNotFoundException {
		 TestSuite suite = new TestSuite("Test for net.ucanaccess.test");
		 suite.addTestSuite(AddFunctionTest.class);
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
		suite.addTestSuite(NoRomanCharacterTest.class);
		suite.addTestSuite(ExternalResourcesTest.class);
		suite.addTestSuite(RegexTest.class);
		suite.addTestSuite(Size97Test.class);
		suite.addTestSuite(BatchTest.class);
		suite.addTestSuite(BooleanTest.class);
		suite.addTestSuite(ByteTest.class);
		suite.addTestSuite(GeneratedKeysTest.class);
		suite.addTestSuite(GeneratedKeysTest1.class);
		suite.addTestSuite(ColumnOrderTest.class);
		suite.addTestSuite(ExceptionCodeTest.class);
		suite.addTestSuite(MultipleGroupByTest.class);
		return suite;
	}
}
