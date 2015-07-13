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
