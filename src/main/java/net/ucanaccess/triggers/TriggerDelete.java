package net.ucanaccess.triggers;

import com.healthmarketscience.jackcess.Table;
import net.ucanaccess.commands.DeleteCommand;
import net.ucanaccess.jdbc.UcanaccessConnection;

public class TriggerDelete extends TriggerBase {
    @Override
    public void fire(int type, String name, String tableName, Object[] oldR, Object[] newR) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        String execId = UcanaccessConnection.getCtxExcId();
        try {
            Table t = getTable(tableName, conn);
            super.convertRowTypes(oldR, t);
            DeleteCommand c4j = new DeleteCommand(t, getRowPattern(oldR, t), execId);
            conn.add(c4j);
        } catch (Exception e) {
            throw new TriggerException(e.getMessage());
        }
    }
}
