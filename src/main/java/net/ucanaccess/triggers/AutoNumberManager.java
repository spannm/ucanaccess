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
import java.util.Hashtable;

import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.OnReloadReferenceListener;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.impl.ColumnImpl;

public class AutoNumberManager {
	private static Hashtable<Column,Integer> register=new Hashtable<Column,Integer>();
	static{
		DBReference.addOnReloadRefListener(new OnReloadReferenceListener() {
			public void onReload() {
				register.clear();
			}
		});
	}
	
	
	static synchronized int  getNext(Column cli){
		ColumnImpl cl=(ColumnImpl) cli;
		if(!register.containsKey(cl)){
			register.put(cl, ((Integer) cl.getAutoNumberGenerator()
										.getLast()));
		}
		int next=register.get(cl);
		 register.put(cl,++next);
		 return next;
	}
	
	public static synchronized void reset(Column cl,int newVal){
		register.put(cl, newVal);
	}


}
