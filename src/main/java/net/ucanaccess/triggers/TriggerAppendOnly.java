package net.ucanaccess.triggers;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import net.ucanaccess.complex.Version;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;
import org.hsqldb.types.JavaObjectData;

import java.time.LocalDateTime;

public class TriggerAppendOnly extends TriggerBase {

    @Override
    public void fire(int type, String name, String tableName, Object[] oldR, Object[] newR) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        if (conn.isFeedbackState()) {
            return;
        }
        try {
            Table t = getTable(tableName, conn);
            if (t == null) {
                throw new UcanaccessSQLException(ExceptionMessages.TABLE_DOES_NOT_EXIST, tableName);
            }
            int i = 0;
            for (Column col : t.getColumns()) {
                if (col.isAppendOnly()) {
                    ColumnImpl verCol = (ColumnImpl) col.getVersionHistoryColumn();
                    LocalDateTime upTime = LocalDateTime.now();
                    String val = newR[i] == null ? null : newR[i].toString();
                    if (type == org.hsqldb.trigger.Trigger.INSERT_BEFORE_ROW) {
                        newR[verCol.getColumnNumber()] = new JavaObjectData(new Version[] {new Version(val, upTime)});
                    } else if (type == org.hsqldb.trigger.Trigger.UPDATE_BEFORE_ROW && (oldR[i] != null || newR[i] != null)) {
                        if (oldR[i] == null && newR[i] != null || oldR[i] != null && newR[i] == null
                            || !oldR[i].equals(newR[i])) {
                            Version[] oldV = (Version[]) ((JavaObjectData) oldR[verCol.getColumnNumber()]).getObject();

                            Version[] newV = new Version[oldV.length + 1];
                            for (int j = 0; j < oldV.length; j++) {
                                newV[j + 1] = oldV[j];
                            }
                            newV[0] = new Version(val, upTime);
                            newR[verCol.getColumnNumber()] = new JavaObjectData(newV);
                        }
                    }
                }
                ++i;
            }
        } catch (Exception _ex) {
            throw new TriggerException(_ex.getMessage());
        }
    }
}
