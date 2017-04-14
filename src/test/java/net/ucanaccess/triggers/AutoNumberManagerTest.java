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

package net.ucanaccess.triggers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.ColumnImpl.AutoNumberGenerator;

@RunWith(MockitoJUnitRunner.class)
public class AutoNumberManagerTest {

	@Mock private ColumnImpl column;
	@Mock private AutoNumberGenerator autoNumberGenerator;
	
	@Before
	public void setUp() {
		when(column.getAutoNumberGenerator()).thenReturn(autoNumberGenerator);
		when(autoNumberGenerator.getLast()).thenReturn(1);		
	}
	
	@Test
	public void testGetNext() {
		assertEquals(2, AutoNumberManager.getNext(column));
		assertEquals(3, AutoNumberManager.getNext(column));
		assertEquals(4, AutoNumberManager.getNext(column));
	}

	@Test
	public void testReset() {
		assertEquals(2, AutoNumberManager.getNext(column));
		assertEquals(3, AutoNumberManager.getNext(column));
		AutoNumberManager.reset(column, 0);
		assertEquals(1, AutoNumberManager.getNext(column));
	}

	@Test
	public void testBump() {
		assertEquals(2, AutoNumberManager.getNext(column));
		assertEquals(3, AutoNumberManager.getNext(column));
		AutoNumberManager.bump(column, 0);
		assertEquals(4, AutoNumberManager.getNext(column));
		AutoNumberManager.bump(column, 10);
		assertEquals(11, AutoNumberManager.getNext(column));
	}
	
	@Test
	public void testClear() {
		assertEquals(2, AutoNumberManager.getNext(column));
		assertEquals(3, AutoNumberManager.getNext(column));
		AutoNumberManager.clear();
		assertEquals(2, AutoNumberManager.getNext(column));
	}
}
