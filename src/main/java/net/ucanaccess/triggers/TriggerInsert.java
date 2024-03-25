package net.ucanaccess.triggers;

import io.github.spannm.jackcess.Table;
import net.ucanaccess.commands.InsertCommand;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Try;

public class TriggerInsert extends TriggerBase {

    @Override
    public void fire(int type, String name, String tableName, Object[] oldRow, Object[] newRow) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        String execId = UcanaccessConnection.getCtxExcId();

        Try.catching(() -> {
            Table t = getTable(tableName, conn);
            super.convertRowTypes(newRow, t);
            InsertCommand c4j = t == null ? new InsertCommand(tableName, conn.getDbIO(), newRow, execId)
                    : new InsertCommand(t, newRow, execId);
            conn.add(c4j);
        }).orThrow(ex -> new TriggerException(ex.getMessage()));
    }

}
