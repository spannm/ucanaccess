package net.ucanaccess.triggers;

import com.healthmarketscience.jackcess.Table;
import net.ucanaccess.commands.UpdateCommand;
import net.ucanaccess.jdbc.BlobKey;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import org.hsqldb.SessionInterface;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.types.BlobData;

import java.util.Map;

public class TriggerUpdate extends TriggerBase {
    @Override
    public void fire(int type, String name, String tableName, Object[] oldR, Object[] newR) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        if (conn.isFeedbackState()) {
            return;
        }
        String execId = UcanaccessConnection.getCtxExcId();
        try {
            Table t = getTable(tableName, conn);
            getBlobs(oldR);
            getBlobs(newR);
            super.convertRowTypes(oldR, t);
            super.convertRowTypes(newR, t);
            if (valuesChanged(oldR, newR)) {
                Map<String, Object> rowPattern = getRowPattern(oldR, t);
                UpdateCommand c4j = new UpdateCommand(t, rowPattern, newR, execId);
                conn.add(c4j);
            }
        } catch (Exception e) {
            throw new TriggerException(e.getMessage());
        }
    }

    public boolean valuesChanged(Object[] oldR, Object[] newR) {
        if (oldR.length != newR.length) {
            return true;
        }
        for (int i = 0; i < oldR.length; ++i) {
            if (oldR[i] == null ^ newR[i] == null) {
                return true;
            }
            if (oldR[i] != null && newR[i] != null && !oldR[i].equals(newR[i])) {
                return true;
            }
        }
        return false;
    }

    private void getBlobs(Object[] values) throws UcanaccessSQLException {
        for (int i = 0; i < values.length; ++i) {
            Object value = values[i];
            if (value instanceof BlobData) {
                BlobData bd = (BlobData) value;
                JDBCConnection hsqlConn = (JDBCConnection) UcanaccessConnection.getCtxConnection()
                        .getHSQLDBConnection();
                SessionInterface si = hsqlConn.getSession();
                long length = bd.length(si);
                byte[] bt = (byte[]) ((BlobData) value).getBytes(si, 0, (int) length);
                if (bt.length == 0) {
                    values[i] = bt;
                } else {
                    BlobKey bk = BlobKey.getBlobKey(bt);
                    if (bk == null) {
                        values[i] = bt;
                    } else {
                        values[i] = bk.getOleBlob(UcanaccessConnection.getCtxConnection().getDbIO());
                    }
                }

            }
        }
    }

}
