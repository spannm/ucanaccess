package net.ucanaccess.triggers;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;
import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.converters.UcanaccessTable;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.log.Logger;
import net.ucanaccess.log.LoggerMessageEnum;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public abstract class TriggerBase implements org.hsqldb.Trigger {
    public static final Persist2Jet P2A           = new Persist2Jet();
    public static final String      ESCAPE_PREFIX = "X";

    public void convertRowTypes(Object[] values, Table table) throws SQLException {
        P2A.convertRowTypes(values, table);
    }

    public void checkContext() {
        if (!UcanaccessConnection.hasContext() || UcanaccessConnection.getCtxConnection() == null) {
            for (StackTraceElement el : Thread.currentThread().getStackTrace()) {
                if ("executeQuery".equals(el.getMethodName())) {
                    throw new TriggerException(Logger.getMessage(LoggerMessageEnum.NO_SELECT));
                }
            }
        }

    }

    protected Map<String, Object> getRowPattern(Object[] values, Table t) throws SQLException {
        return P2A.getRowPattern(values, t);
    }

    protected Table getTable(String tableName, UcanaccessConnection conn) throws IOException {
        Table t = conn.getDbIO().getTable(tableName);
        if (t == null && tableName.startsWith(ESCAPE_PREFIX) && SQLConverter.isXescaped(tableName.substring(1))) {
            t = conn.getDbIO().getTable(tableName.substring(1));
            if (t != null) {
                return new UcanaccessTable(t, tableName.substring(1));
            }
        }
        if (t == null) {
            Database db = conn.getDbIO();
            for (String cand : db.getTableNames()) {
                if (SQLConverter.preEscapingIdentifier(cand).equals(tableName)
                        || SQLConverter.escapeIdentifier(cand).equals(tableName)) {
                    t = new UcanaccessTable(db.getTable(cand), cand);
                    break;
                }
            }
        }
        return new UcanaccessTable(t, tableName);
    }

}
