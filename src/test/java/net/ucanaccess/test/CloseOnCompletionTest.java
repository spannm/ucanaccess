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

import java.sql.PreparedStatement;


import net.ucanaccess.jdbc.UcanaccessStatement;

import com.healthmarketscience.jackcess.Database.FileFormat;

public class CloseOnCompletionTest extends UcanaccessTestBase {


	public CloseOnCompletionTest() {
		super();
	}

	public CloseOnCompletionTest(FileFormat accVer) {
		super(accVer);
	}

	
	public void testCloseOnCompletion() throws Exception {
	
		PreparedStatement st = null;
		try {
			st = super.ucanaccess.prepareStatement("CREATE TABLE pluto1 (id varchar(23)) ");
			((UcanaccessStatement) st).closeOnCompletion();

			st.execute();

		} finally {
			if (st != null)
				st.close();
		}

	}

}
