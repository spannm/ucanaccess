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
package net.ucanaccess.commands;

import java.io.IOException;
import java.util.Map;

import com.healthmarketscience.jackcess.Cursor;

public abstract class AbstractCursorCommand implements ICursorCommand {
	public boolean currentRowMatches(Cursor cur, Map<String, Object> currentRow)
			throws IOException {
		Map<String, Object> rowPattern = getRowPattern();
		if (rowPattern.size() != currentRow.size()) {
			return false;
		}
		for (Map.Entry<String, Object> e : currentRow.entrySet()) {
			String columnName = e.getKey();
			if (!cur.getColumnMatcher().matches(cur.getTable(), columnName,
					rowPattern.get(columnName), e.getValue())) {
				return false;
			}
		}
		return true;
	}
	
	public abstract Map<String, Object> getRowPattern();
	
	public void replaceAutoincrement(Map<String, Object> map) {
		this.getRowPattern().putAll(map);
	}
}
