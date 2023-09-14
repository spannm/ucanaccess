package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.Cursor;

import java.util.Map;

public abstract class AbstractCursorCommand implements ICursorCommand {
    @Override
    public boolean currentRowMatches(Cursor cur, Map<String, Object> currentRow) {
        Map<String, Object> rowPattern = getRowPattern();
        if (rowPattern.size() != currentRow.size()) {
            return false;
        }
        for (Map.Entry<String, Object> e : currentRow.entrySet()) {
            String columnName = e.getKey();
            if (!cur.getColumnMatcher().matches(cur.getTable(), columnName, rowPattern.get(columnName), e.getValue())) {
                return false;
            }
        }
        return true;
    }

    public void replaceAutoincrement(Map<String, Object> map) {
        getRowPattern().putAll(map);
    }
}
