package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.Cursor;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractCursorCommand implements ICursorCommand {
    @Override
    public boolean currentRowMatches(Cursor cur, Map<String, Object> currentRow) throws IOException {
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

    @Override
    public abstract Map<String, Object> getRowPattern();

    public void replaceAutoincrement(Map<String, Object> map) {
        this.getRowPattern().putAll(map);
    }
}
