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

import java.sql.SQLException;
import java.util.Map;

import net.ucanaccess.converters.Persist2Jet;

import com.healthmarketscience.jackcess.Table;

public abstract class TriggerBase implements org.hsqldb.Trigger {
	public static final Persist2Jet p2a = new Persist2Jet();
	
	public void convertRowTypes(Object[] values, Table table)
			throws SQLException {
		p2a.convertRowTypes(values, table);
	}
	
	protected Map<String, Object> getRowPattern(Object[] values, Table t)
			throws SQLException {
		return p2a.getRowPattern(values, t);
	}
}
