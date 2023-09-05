package net.ucanaccess.triggers;

import com.healthmarketscience.jackcess.Table;
import net.ucanaccess.commands.InsertCommand;
import net.ucanaccess.jdbc.UcanaccessConnection;

public class TriggerInsert extends TriggerBase {
    @Override
    public void fire(int type, String name, String tableName, Object[] oldR, Object[] newR) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        String execId = UcanaccessConnection.getCtxExcId();
        try {
            Table t = getTable(tableName, conn);
            super.convertRowTypes(newR, t);
            InsertCommand c4j = (t == null) ? new InsertCommand(tableName, conn.getDbIO(), newR, execId)
                    : new InsertCommand(t, newR, execId);
            conn.add(c4j);
        } catch (Exception e) {
            throw new TriggerException(e.getMessage());
        }
    }
}
