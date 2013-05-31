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
package net.ucanaccess.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.healthmarketscience.jackcess.Database.FileFormat;


public final class DBReferenceSingleton {
	private static DBReferenceSingleton singletonObject;
	private Map<String, DBReference> dbRegistry = Collections.synchronizedMap(new HashMap<String, DBReference>());
	
	private DBReferenceSingleton() {
	}
	
	public static DBReferenceSingleton getInstance() {
		if (singletonObject == null) {
			singletonObject = new DBReferenceSingleton();
		}
		return singletonObject;
	}
	
	public DBReference getReference(File ref) {
		return dbRegistry.get(ref.getAbsolutePath());
	}
	
	public boolean loaded(File fl) throws IOException, SQLException {
		return dbRegistry.containsKey(fl.getAbsolutePath());
	}
	
	public DBReference loadReference(File fl,FileFormat ff,JackcessOpenerInterface jko,String pwd) throws IOException, SQLException {
		DBReference ref = new DBReference(fl,  ff,jko,pwd);
		return ref;
	}
	
	public DBReference put(String path, DBReference dbr) {
		return dbRegistry.put(path, dbr);
	}

	public  DBReference remove(String path) throws IOException, SQLException {
		synchronized (UcanaccessDriver.class) {
			return dbRegistry.remove(path);
		}
	}
	
	
}
