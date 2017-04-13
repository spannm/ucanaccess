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
package net.ucanaccess.triggers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.impl.ColumnImpl;

import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.OnReloadReferenceListener;

public class AutoNumberManager {
	// Consider replacing AtomicInteger with a custom wrapper around an 'int' if performance
	// becomes an issue. Never use an Integer here because Integer is an immutable object.
	private static final Map<Column, AtomicInteger> register = new HashMap<Column, AtomicInteger>();
	
	static {
		DBReference.addOnReloadRefListener(new OnReloadReferenceListener() {
			public void onReload() {
				// Must call AutoNumberManager.clear() for proper thread synchronization.
				// Do not call register.clear() directly.
				clear();
			}
		});
	}
	
	/** Clears all AutoNumber column seeds to 0. */
	private static synchronized void clear() {
		register.clear();
	}

	/** Returns the next AutoNumber value, and increments the seed. */
	static synchronized int getNext(Column cl) {
		// Note: This code assumes *sequential* integer AutoNumber values.
		// (Access also supports *random* integer AutoNumber values, but they
		// are not very common.)
		ColumnImpl ci = (ColumnImpl) cl;
		AtomicInteger next = register.get(ci);
		if (next == null) {
			next = new AtomicInteger((Integer) ci.getAutoNumberGenerator().getLast());
			register.put(ci, next);
		}
		return next.incrementAndGet();
	}

	/** Sets the AutoNumber seed to {@code newVal}. */
	public static synchronized void reset(Column cl, int newVal) {
		register.put(cl, new AtomicInteger(newVal));
	}

	/** Bumps the AutoNumber seed to {@code newVal} if it is higher than the existing one. */
	public static synchronized void bump(Column cl, int newVal) {
		ColumnImpl ci = (ColumnImpl) cl;
		AtomicInteger next = register.get(ci); 
		if (next == null) {
			next = new AtomicInteger((Integer) ci.getAutoNumberGenerator().getLast());
			register.put(ci, next);
		}
		if (newVal > next.get()) {
			next.set(newVal);
		}
	}

}
