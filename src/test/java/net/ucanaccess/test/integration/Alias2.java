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
package net.ucanaccess.test.integration;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;

@RunWith(Parameterized.class)
public class Alias2 extends AccessVersionDefaultTest {

	public Alias2(AccessVersion _accessVersion) {
		super(_accessVersion);
	}

	@Before
	public void beforeTestCase() throws Exception {
		executeStatements("CREATE TABLE `categories abc` (category_id COUNTER,descr memo) ");
	}

	@Test
	public void testRegex2() throws SQLException, IOException {
		dumpQueryResult("SELECT SUM(category_id) AS `sum(categories abc:category_id)` FROM `categories abc`");
	}

}
