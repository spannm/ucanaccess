package net.ucanaccess.commands;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Table;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.BlobKey;
import net.ucanaccess.jdbc.UcanaccessConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class BlobAction implements IFeedbackAction {
    private final Table        table;
    private boolean            containsBlob;
    private final Set<BlobKey> keys = new HashSet<>();

    public BlobAction(Table _table, Object[] newValues) {
        table = _table;

        if (!BlobKey.hasPrimaryKey(_table)) {
            return;
        }
        Index pk = _table.getPrimaryKeyIndex();
        HashSet<String> hsKey = new HashSet<>();
        for (Index.Column icl : pk.getColumns()) {
            hsKey.add(icl.getName());
        }
        HashSet<String> hsBlob = new HashSet<>();
        int i = 0;
        Map<String, Object> keyMap = new HashMap<>();
        for (Column cl : _table.getColumns()) {
            if (cl.getType().equals(DataType.OLE) && newValues[i] != null) {
                containsBlob = true;
                hsBlob.add(cl.getName());
            }
            if (hsKey.contains(cl.getName())) {
                keyMap.put(cl.getName(), newValues[i]);
            }
            ++i;
        }
        for (String cln : hsBlob) {
            keys.add(new BlobKey(keyMap, table.getName(), cln));
        }

    }

    @Override
    public void doAction(ICommand toChange) throws SQLException {
        if (containsBlob) {
            UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
            Connection connHsqldb = conn.getHSQLDBConnection();

            for (BlobKey bkey : keys) {
                String sql = "UPDATE " + SQLConverter.escapeIdentifier(table.getName(), connHsqldb) + " SET "
                        + SQLConverter.escapeIdentifier(bkey.getColumnName(), connHsqldb) + "=? WHERE ";
                StringBuilder sb = new StringBuilder();
                String and = "";
                List<Object> values = new ArrayList<>();
                for (Map.Entry<String, Object> me : bkey.getKey().entrySet()) {
                    sb.append(and).append(SQLConverter.escapeIdentifier(me.getKey(), connHsqldb)).append(" = ?");
                    values.add(me.getValue());
                    and = " AND ";
                }
                sql += sb.toString();
                conn.setFeedbackState(true);

                conn.setFeedbackState(true);
                try (PreparedStatement ps = connHsqldb.prepareStatement(sql)) {
                    ps.setObject(1, bkey.getBytes());
                    int j = 2;
                    for (Object value : values) {
                        ps.setObject(j, value);
                        ++j;
                    }
                    ps.executeUpdate();
                    conn.setFeedbackState(false);
                }
            }
        }
    }

}
