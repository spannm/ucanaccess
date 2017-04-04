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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Unit test for {@link Exporter}. */
public class ExporterTest {

	@Test
	public void testToCsv() {
		assertEquals("\"a,b\"", Exporter.toCsv("a,b", ","));
		assertEquals("\"a,,b\"", Exporter.toCsv("a,,b", ","));
		assertEquals("\"a\"\"b\"", Exporter.toCsv("a\"b", ","));
		assertEquals("a b", Exporter.toCsv("a\nb", ","));		
		assertEquals("a'b'c", Exporter.toCsv("a'b'c", ","));
	}
}
