/*
Copyright (c) 2017 Brian Park.

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
package net.ucanaccess.console;

import static org.junit.Assert.assertArrayEquals;

import java.util.List;

import org.junit.Test;

public class MainTest {

	@Test
	public void testTokenize() {
		
		// normal space as token delimiter
		List<String> tokens = Main.tokenize("export -d , -t License License.csv");
		assertListEquals(tokens, "export", "-d", ",", "-t", "License", "License.csv");

		// quotes are not supported yet
		tokens = Main.tokenize("export -d ',' -t \"License and Address\" License.csv");
		assertListEquals(tokens, 
				"export", "-d", "','", "-t", "\"License", "and", "Address\"", "License.csv");
	}

	private static void assertListEquals(List<String> actualList, String... expected) {
		String[] actual = new String[actualList.size()];
		actualList.toArray(actual);
		assertArrayEquals(expected, actual);
	}
}
