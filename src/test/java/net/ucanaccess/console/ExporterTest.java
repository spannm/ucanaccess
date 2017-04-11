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

import java.sql.ResultSetMetaData;
import java.sql.Types;

import org.junit.Test;

/** Unit test for {@link Exporter}. */
public class ExporterTest {

	@Test
	public void testToCsvReplacingNewlines() {
		boolean preserveNewlines = false;
		assertEquals("\"a,b\"", Exporter.toCsv("a,b", ",", preserveNewlines));
		assertEquals("\"a,,b\"", Exporter.toCsv("a,,b", ",", preserveNewlines));
		assertEquals("\"a\"\"b\"", Exporter.toCsv("a\"b", ",", preserveNewlines));
		assertEquals("a  b", Exporter.toCsv("a\r\nb", ",", preserveNewlines));		
		assertEquals("a\tb", Exporter.toCsv("a\tb", ",", preserveNewlines));		
		assertEquals("a'b'c", Exporter.toCsv("a'b'c", ",", preserveNewlines));
	}

	@Test
	public void testToCsvPreservingNewlines() {
		boolean preserveNewlines = true;
		assertEquals("\"a\r\nb\"", Exporter.toCsv("a\r\nb", ",", preserveNewlines));
	}

	@Test
	public void testToBigQueryType() {
		assertEquals("int64", Exporter.toBigQueryType(Types.INTEGER));
		assertEquals("float64", Exporter.toBigQueryType(Types.DECIMAL));
		assertEquals("timestamp", Exporter.toBigQueryType(Types.TIMESTAMP));
		assertEquals("string", Exporter.toBigQueryType(Types.CHAR));
		assertEquals("string", Exporter.toBigQueryType(Types.VARCHAR));

		// any type not explicitly defined in the switch statement is mapped to a "string".
		assertEquals("string", Exporter.toBigQueryType(Types.BIT));
	}
	
	@Test
	public void testToBigQueryNullable() {
		assertEquals("required", Exporter.toBigQueryNullable(0));
		assertEquals("nullable", Exporter.toBigQueryNullable(1));
		assertEquals("nullable", Exporter.toBigQueryNullable(2));
		assertEquals("nullable", Exporter.toBigQueryNullable(3));
	}
	
	@Test
	public void testToSchemaRow() throws Exception {
		assertEquals("{\"name\": \"MyName\", \"type\": \"int64\", \"mode\": \"nullable\"}",
				Exporter.toSchemaRow("MyName", Types.INTEGER, ResultSetMetaData.columnNullable));
	}
}
