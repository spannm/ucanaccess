package net.ucanaccess.commands;

import io.github.spannm.jackcess.Column;
import io.github.spannm.jackcess.Table;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.UcanaccessConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class AutoNumberAction implements IFeedbackAction {
    private final Map<String, Object> newAutoValues = new HashMap<>();
    private final Map<String, Object> oldAutoValues = new HashMap<>();
    private final Table               table;

    public AutoNumberAction(Table _table, Object[] memento, Object[] byAccess) throws SQLException {
        table = _table;
        int i = 0;

        for (Column col : _table.getColumns()) {
            if (col.isAutoNumber()) {
                UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
                Connection connHsqldb = conn.getHSQLDBConnection();
                String hsqlColName = SQLConverter.escapeIdentifier(col.getName(), connHsqldb);
                Object cnOld = memento[i];
                Object cnNew = byAccess[i];
                if (cnNew instanceof String) {
                    cnNew = ((String) cnNew).toUpperCase();
                }
                oldAutoValues.put(col.getName(), cnOld);
                newAutoValues.put(col.getName(), cnNew);
                conn.setFeedbackState(true);
                String sql = String.format("UPDATE %s SET %s=? WHERE %s=?",
                    SQLConverter.escapeIdentifier(_table.getName(), connHsqldb), hsqlColName, hsqlColName);

                try (@SuppressWarnings("java:S2077") PreparedStatement ps = connHsqldb.prepareStatement(sql)) {
                    ps.setObject(1, cnNew);
                    ps.setObject(2, cnOld);
                    ps.executeUpdate();

                    conn.setGeneratedKey(cnNew);
                    conn.setFeedbackState(false);
                }
            }
            i++;
        }
    }

    @Override
    public void doAction(ICommand toChange) {
        if (!table.getName().equalsIgnoreCase(toChange.getTableName())) {
            return;
        }
        switch (toChange.getType()) {
        case DELETE:
        case UPDATE:
            AbstractCursorCommand acm = (AbstractCursorCommand) toChange;
            Map<String, Object> old = acm.getRowPattern();
            for (Map.Entry<String, Object> entry : oldAutoValues.entrySet()) {
                if (old.containsKey(entry.getKey()) && old.get(entry.getKey()).equals(entry.getValue())) {
                    old.put(entry.getKey(), newAutoValues.get(entry.getKey()));
                }
            }
            break;
        case COMPOSITE:
            CompositeCommand cc = (CompositeCommand) toChange;
            for (ICommand ic : cc.getComposite()) {
                doAction(ic);
            }
            break;
        default:
            break;
        }
    }
}
