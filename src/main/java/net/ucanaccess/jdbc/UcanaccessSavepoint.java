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

import java.sql.SQLException;
import java.sql.Savepoint;

public class UcanaccessSavepoint implements Savepoint {
	private Savepoint wrapped;
	
	public UcanaccessSavepoint(Savepoint wrapped) {
		super();
		this.wrapped = wrapped;
	}
	
	public int getSavepointId() throws SQLException {
		try {
			return wrapped.getSavepointId();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}
	
	public String getSavepointName() throws SQLException {
		try {
			return wrapped.getSavepointName();
		} catch (SQLException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	Savepoint getWrapped() {
		return wrapped;
	}

	
	
}
