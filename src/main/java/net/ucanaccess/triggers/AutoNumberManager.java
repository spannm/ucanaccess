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
package net.ucanaccess.triggers;
import java.util.Hashtable;

import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.OnReloadReferenceListener;

import com.healthmarketscience.jackcess.Column;

public class AutoNumberManager {
	private static Hashtable<Column,Integer> register=new Hashtable<Column,Integer>();
	static{
		DBReference.addOnReloadRefListener(new OnReloadReferenceListener() {
			public void onReload() {
				register.clear();
			}
		});
	}
	
	
	static synchronized int  getNext(Column cl){
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
