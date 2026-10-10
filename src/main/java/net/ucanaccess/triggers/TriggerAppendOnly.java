package net.ucanaccess.triggers;

import io.github.spannm.jackcess.Column;
import io.github.spannm.jackcess.Table;
import io.github.spannm.jackcess.impl.ColumnImpl;
import net.ucanaccess.complex.Version;
import net.ucanaccess.exception.TableNotFoundException;
import net.ucanaccess.jdbc.UcanaccessConnection;
import org.hsqldb.types.JavaObjectData;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;

public class TriggerAppendOnly extends TriggerBase {

    @Override
    public void fire(int type, String name, String tableName, Object[] oldR, Object[] newR) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.requireCtxConnection();
        if (conn.isFeedbackState()) {
            return;
        }
        try {
            Table t = Optional.ofNullable(getTable(tableName, conn)).orElseThrow(() -> new TableNotFoundException(tableName));
            int i = 0;
            for (Column col : t.getColumns()) {
                if (col.isAppendOnly()) {
                    ColumnImpl verCol = (ColumnImpl) col.getVersionHistoryColumn();
                    LocalDateTime upTime = LocalDateTime.now(ZoneId.systemDefault());
                    String val = newR[i] == null ? null : newR[i].toString();
                    if (INSERT_BEFORE_ROW == type) {
                        newR[verCol.getColumnNumber()] = new JavaObjectData(new Version[] {new Version(val, upTime)});
                    } else if (UPDATE_BEFORE_ROW == type && !Objects.equals(oldR[i], newR[i])) {
                        Version[] oldV = (Version[]) ((JavaObjectData) oldR[verCol.getColumnNumber()]).getObject();

                        Version[] newV = new Version[oldV.length + 1];
                        if (oldV.length > 0) {
                            System.arraycopy(oldV, 0, newV, 1, oldV.length);
                        }
                        newV[0] = new Version(val, upTime);
                        newR[verCol.getColumnNumber()] = new JavaObjectData(newV);
                    }
                }
                ++i;
            }
        } catch (Exception ex) {
            throw new TriggerException(ex.getMessage());
        }
    }
}
