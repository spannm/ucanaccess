package net.ucanaccess.commands;

import io.github.spannm.jackcess.Column;
import io.github.spannm.jackcess.DataType;
import io.github.spannm.jackcess.Index;
import io.github.spannm.jackcess.Table;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.BlobKey;
import net.ucanaccess.jdbc.UcanaccessConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

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
        for (Column col : _table.getColumns()) {
            if (col.getType().equals(DataType.OLE) && newValues[i] != null) {
                containsBlob = true;
                hsBlob.add(col.getName());
            }
            if (hsKey.contains(col.getName())) {
                keyMap.put(col.getName(), newValues[i]);
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

                List<Object> values = new ArrayList<>(bkey.getKey().values());

                String sql = String.format("UPDATE %s SET %s=? WHERE %s",
                    SQLConverter.escapeIdentifier(table.getName(), connHsqldb),
                    SQLConverter.escapeIdentifier(bkey.getColumnName(), connHsqldb),
                    bkey.getKey().keySet().stream().map(k -> SQLConverter.escapeIdentifier(k, connHsqldb) + " = ?").collect(Collectors.joining(" AND ")));

                conn.setFeedbackState(true);

                conn.setFeedbackState(true);
                try (@SuppressWarnings("java:S2077") PreparedStatement ps = connHsqldb.prepareStatement(sql)) {
                    ps.setObject(1, bkey.getBytes());
                    int i = 2;
                    for (Object value : values) {
                        ps.setObject(i, value);
                        i++;
                    }
                    ps.executeUpdate();
                    conn.setFeedbackState(false);
                }
            }
        }
    }

}
