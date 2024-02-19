package net.ucanaccess.triggers;

import io.github.spannm.jackcess.Table;
import net.ucanaccess.commands.InsertCommand;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Try;

public class TriggerInsert extends TriggerBase {

    @Override
    public void fire(int type, String name, String tableName, Object[] oldR, Object[] newR) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        String execId = UcanaccessConnection.getCtxExcId();

        Try.catching(() -> {
            Table t = getTable(tableName, conn);
            super.convertRowTypes(newR, t);
            InsertCommand c4j = t == null ? new InsertCommand(tableName, conn.getDbIO(), newR, execId)
                    : new InsertCommand(t, newR, execId);
            conn.add(c4j);
        }).orThrow(ex -> new TriggerException(ex.getMessage()));
    }

}
