package net.ucanaccess.triggers;

import io.github.spannm.jackcess.Table;
import net.ucanaccess.commands.DeleteCommand;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Try;

public class TriggerDelete extends TriggerBase {

    @Override
    public void fire(int type, String name, String tableName, Object[] oldR, Object[] newR) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        String execId = UcanaccessConnection.getCtxExcId();

        Try.catching(() -> {
            Table t = getTable(tableName, conn);
            super.convertRowTypes(oldR, t);
            DeleteCommand c4j = new DeleteCommand(t, getRowPattern(oldR, t), execId);
            conn.add(c4j);
        }).orThrow(ex -> new TriggerException(ex.getMessage()));
    }

}
