package net.ucanaccess.jdbc;

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.util.OleBlob;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.util.Try;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlobKey implements Serializable {
    private static final long   serialVersionUID = -8580858159403159903L;
    public static final int     MAX_SIZE         = 4096;

    private Map<String, Object> key;
    private final String        tableName;
    private final String        columnName;

    public BlobKey(Map<String, Object> _key, String _tableName, String _columnName) {
        key = _key;
        tableName = _tableName;
        columnName = _columnName;
    }

    public BlobKey(Table _table, String _columnName, Row _row) {
        tableName = _table.getName();
        columnName = _columnName;
        if (hasPrimaryKey(_table)) {
            List<? extends Index.Column> cl = _table.getPrimaryKeyIndex().getColumns();
            Map<String, Object> keyMap = new HashMap<>();
            for (Index.Column c : cl) {
                keyMap.put(c.getName(), _row.get(c.getName()));
            }
            key = keyMap;
        }
    }

    public static boolean hasPrimaryKey(Table _table) {
        for (Index idx : _table.getIndexes()) {
            if (idx.isPrimaryKey()) {
                return true;
            }
        }
        return false;
    }

    public OleBlob getOleBlob(Database _db) throws UcanaccessSQLException {
        return Try.catching(() -> {
            Table t = _db.getTable(tableName);
            Cursor c = CursorBuilder.createPrimaryKeyCursor(t);
            return c.findFirstRow(key) ? c.getCurrentRow().getBlob(columnName) : null;
        }).orThrow(UcanaccessSQLException::new);
    }

    public byte[] getBytes() throws UcanaccessSQLException {
        return Try.withResources(ByteArrayOutputStream::new, bais -> {
            ObjectOutputStream oos = new ObjectOutputStream(bais);
            oos.writeObject(this);
            oos.flush();
            return bais.toByteArray();
        }).orThrow(UcanaccessSQLException::new);
    }

    public static BlobKey getBlobKey(byte[] _bytes) {
        return Try.withResources(() -> new ByteArrayInputStream(_bytes), bais -> {
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object obj = ois.readObject();
            return obj instanceof BlobKey ? (BlobKey) obj : null;
        }).orIgnore();
    }

    public static BlobKey getBlobKey(InputStream _is) {
        return Try.catching(() -> {
            byte[] bt = new byte[MAX_SIZE];
            int readBytes = _is.read(bt);
            return readBytes > 0 ? getBlobKey(bt) : null;
        }).orIgnore();
    }

    public Map<String, Object> getKey() {
        return key;
    }

    public String getColumnName() {
        return columnName;
    }

}
