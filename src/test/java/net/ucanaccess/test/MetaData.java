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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;


import com.healthmarketscience.jackcess.Database.FileFormat;

public class MetaData extends UcanaccessTestBase {
	public MetaData() {
		super();
	}

	public MetaData(FileFormat accVer) {
		super(accVer);
	}

	public String getAccessPath() {
		return "net/ucanaccess/test/resources/noroman.mdb";
	}

	public void testNoRomanCharactersInColumnName() throws Exception {
		dump("SELECT * FROM NOROMAN");
	DatabaseMetaData dbmd=	this.ucanaccess.getMetaData();
	ResultSet rs=dbmd.getColumns(null, null, "NOROMAN", "ID");//noroman tableName
		while(rs.next()){

			System.out.println("TABLE_NAME:"+rs.getString(3)+"="+rs.getString("TABLE_NAME"));
			System.out.println("COLUMN_NAME:"+rs.getString(4)+"="+rs.getString("COLUMN_NAME"));
			System.out.println("DATA_TYPE:"+rs.getInt(5)+"="+rs.getInt("DATA_TYPE"));
		}
	}
}
