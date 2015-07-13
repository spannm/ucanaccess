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
