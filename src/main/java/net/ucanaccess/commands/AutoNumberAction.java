package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Table;
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
        PreparedStatement ps = null;
        for (Column cl : _table.getColumns()) {
            if (cl.isAutoNumber()) {
                UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
                Connection connHsqldb = conn.getHSQLDBConnection();
                String cn = SQLConverter.escapeIdentifier(cl.getName(), connHsqldb);
                Object cnOld = memento[i];
                Object cnNew = byAccess[i];
                if (cnNew instanceof String) {
                    cnNew = ((String) cnNew).toUpperCase();
                }
                oldAutoValues.put(cl.getName(), cnOld);
                newAutoValues.put(cl.getName(), cnNew);
                try {
                    conn.setFeedbackState(true);
                    String stmt = "UPDATE " + SQLConverter.escapeIdentifier(_table.getName(), connHsqldb) + " SET " + cn
                            + "=? WHERE " + cn + "=?";
                    ps = connHsqldb.prepareStatement(stmt);
                    ps.setObject(1, cnNew);
                    ps.setObject(2, cnOld);
                    ps.executeUpdate();

                    conn.setGeneratedKey(cnNew);
                    conn.setFeedbackState(false);
                } finally {
                    if (ps != null) {
                        ps.close();
                    }
                }
            }
            ++i;
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
