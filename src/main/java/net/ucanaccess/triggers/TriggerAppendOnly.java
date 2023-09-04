package net.ucanaccess.triggers;

import java.time.LocalDateTime;
import org.hsqldb.Trigger;
import org.hsqldb.types.JavaObjectData;

import net.ucanaccess.complex.Version;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.ColumnImpl;

public class TriggerAppendOnly extends TriggerBase {
    @Override
    public void fire(int type, String name, String tableName, Object[] oldR, Object[] newR) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        if (conn.isFeedbackState()) {
            return;
        }
        try {
            Table t = this.getTable(tableName, conn);
            if (t == null) {
                throw new RuntimeException(Logger.getMessage("TABLE_DOESNT_EXIST") + " :" + tableName);
            }
            int i = 0;
            for (Column cl : t.getColumns()) {
                if (cl.isAppendOnly()) {
                    ColumnImpl verCol = (ColumnImpl) cl.getVersionHistoryColumn();
                    LocalDateTime upTime = LocalDateTime.now();
                    String val = newR[i] == null ? null : newR[i].toString();
                    if (type == Trigger.INSERT_BEFORE_ROW) {
                        newR[verCol.getColumnNumber()] = new JavaObjectData(new Version[] { new Version(val, upTime) });
                    } else if (type == Trigger.UPDATE_BEFORE_ROW && (oldR[i] != null || newR[i] != null)) {
                        if ((oldR[i] == null && newR[i] != null) || (oldR[i] != null && newR[i] == null)
                                || (!oldR[i].equals(newR[i]))) {
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
        } catch (Exception e) {
            throw new TriggerException(e.getMessage());
        }
    }
}
