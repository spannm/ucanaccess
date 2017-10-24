/*
Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package net.ucanaccess.converters;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import net.ucanaccess.commands.InsertCommand;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.complex.UnsupportedValue;
import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.OnReloadReferenceListener;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessDatabaseMetadata;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;
import net.ucanaccess.util.HibernateSupport;

import org.hsqldb.HsqlDateTime;
import org.hsqldb.SessionInterface;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimestampData;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.RelationshipBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;

public class Persist2Jet {
    private static HashMap<String, List<String>> columnNamesCache = new HashMap<String, List<String>>();
    static {
        DBReference.addOnReloadRefListener(new OnReloadReferenceListener() {

            @Override
            public void onReload() {
                columnNamesCache.clear();
            }
        });
    }

    public Map<String, Object> getRowPattern(Object[] varr, Table t) throws SQLException {
        String ntn = SQLConverter.basicEscapingIdentifier(t.getName()).toUpperCase();
        LinkedHashMap<String, Object> vl = new LinkedHashMap<String, Object>();
        int i = 0;
        Iterator<String> it = getColumnNames(ntn).iterator();
        while (it.hasNext()) {
            vl.put(it.next(), varr[i++]);
        }
        if (i == 0) {
            throw new SQLException("Cannot read table's metadata");
        }
        return escapeIdentifiers(vl, t);
    }

    public Object[] getValues(Map<String, Object> rowPattern, Table t) throws SQLException {
        Object[] values = new Object[rowPattern.size()];
        int i = 0;
        for (Object obj : rowPattern.values()) {
            values[i++] = obj;
        }
        return values;
    }

    private List<String> getColumnNames(String ntn) throws SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        ntn = UcanaccessDatabaseMetadata.normalizeName(ntn);
        String pref = conn.getDbIO().getFile().getAbsolutePath();
        Connection conq = conn.getHSQLDBConnection();
        String key = pref + ntn;
        if (!columnNamesCache.containsKey(key)) {
            ArrayList<String> ar = new ArrayList<String>();
            ResultSet rs = conq.getMetaData().getColumns(null, "PUBLIC", ntn, null);
            TreeMap<Integer, String> tm = new TreeMap<Integer, String>();
            while (rs.next()) {
                String cbase = rs.getString("COLUMN_NAME");
                Integer i = rs.getInt("ORDINAL_POSITION");
                tm.put(i, cbase.toUpperCase());

            }
            ar.addAll(tm.values());
            columnNamesCache.put(key, ar);
        }
        return columnNamesCache.get(key);
    }

    private List<String> getColumnNamesCreate(String ntn) throws SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        ArrayList<String> ar = new ArrayList<String>();
        ResultSet rs = conn.getMetaData().getColumns(null, "PUBLIC", ntn, null);
        while (rs.next()) {
            String cbase = rs.getString("COLUMN_NAME");
            ar.add(cbase);
        }
        return ar;
    }

    public void convertRowTypes(Object[] values, Table t) throws SQLException {
        try {
            List<? extends Column> columns = t.getColumns();
            Iterator<? extends Column> it = columns.iterator();
            for (int i = 0; i < values.length; ++i) {
                Object value = values[i];
                Column column = it.next();

                if (value != null) {
                    if (value instanceof TimestampData) {
                        if (column.getType().equals(DataType.SHORT_DATE_TIME)) {
                            TimestampData ts = (TimestampData) value;
                            TimeZone zone = TimeZone.getDefault();
                            GregorianCalendar cal = new GregorianCalendar(zone);
                            long millis = HsqlDateTime.convertMillisToCalendar(cal, ts.getSeconds() * 1000);
                            java.sql.Timestamp val = new java.sql.Timestamp(millis);
                            val.setNanos(ts.getNanos());
                            values[i] = val;
                        }
                    }
                    if (value instanceof BlobData) {
                        BlobData bd = (BlobData) value;
                        JDBCConnection hsqlConn =
                                (JDBCConnection) UcanaccessConnection.getCtxConnection().getHSQLDBConnection();
                        SessionInterface si = hsqlConn.getSession();
                        long length = bd.length(si);
                        values[i] = ((BlobData) value).getBytes(si, 0, (int) length);
                    }
                    if (value instanceof JavaObjectData) {
                        JavaObjectData jod = (JavaObjectData) value;
                        Object obj = jod.getObject();
                        if (obj instanceof ComplexBase[] && !(obj instanceof UnsupportedValue[])) {
                            values[i] = obj;
                        } else {
                            throw new UcanaccessSQLException(ExceptionMessages.UNSUPPORTED_TYPE);
                        }
                    }

                    if (column.getType().equals(DataType.BYTE)) {
                        int vl = (Integer) value;
                        if (vl < 0 || vl > 256) {
                            throw new SQLException("Data out of range");
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    private LinkedHashMap<String, Object> escapeIdentifiers(LinkedHashMap<String, Object> map, Table t) {
        List<? extends Column> colums = t.getColumns();
        LinkedHashMap<String, Object> vl = new LinkedHashMap<String, Object>();
        for (Column cl : colums) {
            String key = cl.getName();
            String keyu = key.toUpperCase();
            String ekey = map.containsKey(keyu) ? keyu : SQLConverter.escapeIdentifier(key).toUpperCase();
            if (!map.containsKey(ekey) && map.containsKey(ekey.substring(1, ekey.length() - 1))) {
                ekey = ekey.substring(1, ekey.length() - 1);
            }
            vl.put(key, map.get(ekey));
        }
        return vl;
    }

    private String getNormalizedName(String name, Map<String, String> columnMap) {
        if (columnMap == null) {
            return name;
        }
        return columnMap.containsKey(name) ? columnMap.get(name) : name;
    }

    private ColumnBuilder getColumn(ResultSet rs, int seq, String tableName, Map<String, String> columnMap,
            String[] types) throws SQLException {
        String name = rs.getString("COLUMN_NAME");
        String nname = getNormalizedName(name, columnMap);
        ColumnBuilder cb = new ColumnBuilder(nname);
        short length = (short) rs.getInt("COLUMN_SIZE");
        byte scale = (byte) rs.getInt("DECIMAL_DIGITS");
        DataType dt = null;
        if (length == 0 && types != null) {
            if (types[seq].equalsIgnoreCase(AccessType.MEMO.name())
                    || types[seq].equalsIgnoreCase(AccessType.HYPERLINK.name())) {
                dt = DataType.MEMO;
                cb.setType(dt);
                if (types[seq].equalsIgnoreCase(AccessType.HYPERLINK.name())) {
                    cb.setHyperlink(true);
                }
            }
            if (types[seq].equalsIgnoreCase(AccessType.TEXT.name())) {
                dt = DataType.TEXT;
                cb.setType(dt);
            }
        }

        if (types != null && seq < types.length && types[seq] != null
                && (types[seq].equalsIgnoreCase(AccessType.LONG.name())
                        || types[seq].equalsIgnoreCase(AccessType.BYTE.name())
                        || types[seq].equalsIgnoreCase(AccessType.CURRENCY.name())
                        || types[seq].equalsIgnoreCase(AccessType.INTEGER.name())
                        || types[seq].equalsIgnoreCase(AccessType.SINGLE.name())
                        || types[seq].equalsIgnoreCase(AccessType.DOUBLE.name())
                        || types[seq].equalsIgnoreCase(AccessType.YESNO.name())
                        || types[seq].equalsIgnoreCase(AccessType.DATETIME.name())
                        || types[seq].equalsIgnoreCase(AccessType.COUNTER.name())
                        || types[seq].equalsIgnoreCase(AccessType.AUTOINCREMENT.name()))) {
            dt = TypesMap.map2Jackcess(AccessType.valueOf(types[seq].toUpperCase(Locale.US)));
            cb.setType(dt);
            cb.setLengthInUnits((short) dt.getFixedSize());
        }

        if (dt == null) {
            if (types != null && seq < types.length && types[seq] != null
                    && types[seq].equalsIgnoreCase(AccessType.NUMERIC.name())) {
                dt = DataType.NUMERIC;
            } else {
                dt = DataType.fromSQLType(rs.getInt("DATA_TYPE"), length);
            }
            cb.setType(dt);
            if (length > 0 && dt.equals(DataType.TEXT)) {
                cb.setLengthInUnits(length);
            }
            if (scale > 0) {
                cb.setScale(scale);
                if (length > 0) {
                    cb.setPrecision(length);
                }
            }
        }

        if (types != null && seq < types.length) {
            if (types[seq].equalsIgnoreCase(AccessType.COUNTER.name())
                    || types[seq].equalsIgnoreCase(AccessType.AUTOINCREMENT.name())) {
                cb.setAutoNumber(true);
                cb.putProperty(PropertyMap.REQUIRED_PROP, false); // re: Ticket #2
            }
            if (types[seq].equalsIgnoreCase(AccessType.GUID.name())) {
                cb.setType(DataType.GUID);
                cb.setAutoNumber(true);
            }
        }
        return cb;
    }

    private ColumnBuilder getColumn(String tableName, Map<String, String> columnMap, String[] types)
            throws SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        String columnName = columnMap.keySet().iterator().next();
        ResultSet rs = conn.getHSQLDBConnection().getMetaData().getColumns(null, "PUBLIC", tableName.toUpperCase(),
                SQLConverter.preEscapingIdentifier(columnName));

        if (rs.next()) {
            return getColumn(rs, 0, tableName, columnMap, types);

        }
        return null;
    }

    private Collection<ColumnBuilder> getColumns(String tableName, Map<String, String> columnMap, String[] types)
            throws SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        TreeMap<Integer, ColumnBuilder> ordm = new TreeMap<Integer, ColumnBuilder>();
        ResultSet rs = conn.getHSQLDBConnection().getMetaData().getColumns(null, "PUBLIC",
                tableName.toUpperCase(Locale.US), null);
        while (rs.next()) {
            int seq = rs.getInt("ORDINAL_POSITION") - 1;
            ordm.put(seq, getColumn(rs, seq, tableName, columnMap, types));
        }
        return ordm.values();
    }

    private List<IndexBuilder> getIndexBuilders(String tableName, Map<String, String> columnMap) throws SQLException {
        ArrayList<IndexBuilder> arcl = new ArrayList<IndexBuilder>();
        addIndexBuildersSimple(tableName, columnMap, arcl);
        return arcl;
    }

    private void checkPK(List<IndexBuilder> arcl, IndexBuilder ibpk) {
        if (ibpk == null) {
            return;
        }
        Iterator<IndexBuilder> itib = arcl.iterator();
        List<IndexBuilder.Column> clspk = ibpk.getColumns();
        ArrayList<String> columnNamesPK = new ArrayList<String>();
        for (IndexBuilder.Column clpk : clspk) {
            columnNamesPK.add(clpk.getName().toUpperCase());
        }
        while (itib.hasNext()) {
            IndexBuilder ib = itib.next();
            List<IndexBuilder.Column> cls = ib.getColumns();
            if (cls.size() != clspk.size()) {
                continue;
            }
            boolean clsPK = true;
            for (IndexBuilder.Column cl : cls) {
                if (!columnNamesPK.contains(cl.getName().toUpperCase())) {
                    clsPK = false;
                    break;
                }
            }
            if (clsPK) {
                itib.remove();
            }
        }
    }

    private IndexBuilder getIndexBuilderPK(String tableName, Map<String, String> columnMap) throws SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        ResultSet pkrs = conn.getMetaData().getPrimaryKeys(null, "PUBLIC", tableName.toUpperCase());
        IndexBuilder indpk = null;
        while (pkrs.next()) {
            if (indpk == null) {
                String indexName = IndexBuilder.PRIMARY_KEY_NAME;
                indpk = new IndexBuilder(indexName);
                indpk.setPrimaryKey();

            }
            indpk.addColumns(getNormalizedName(pkrs.getString("COLUMN_NAME"), columnMap));
        }
        return indpk;
    }

    private void addIndexBuildersSimple(String tableName, Map<String, String> columnMap, ArrayList<IndexBuilder> arcl)
            throws SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        ResultSet idxrs = conn.getMetaData().getIndexInfo(null, "PUBLIC", tableName, false, false);
        HashMap<String, IndexBuilder> hi = new HashMap<String, IndexBuilder>();
        for (IndexBuilder ib : arcl) {
            hi.put(ib.getName(), ib);
        }
        while (idxrs.next()) {
            String colName = getNormalizedName(idxrs.getString("COLUMN_NAME"), columnMap);
            String indexName = idxrs.getString("INDEX_NAME");
            boolean unique = !idxrs.getBoolean("NON_UNIQUE");
            String ad = idxrs.getString("ASC_OR_DESC");
            boolean asc = ad == null || ad.equals("A");
            if (!hi.containsKey(indexName)) {
                IndexBuilder ib = new IndexBuilder(indexName);
                if (unique) {
                    ib.setUnique();
                }
                arcl.add(ib);
                hi.put(indexName, ib);
            }
            IndexBuilder toIdx = hi.get(indexName);
            toIdx.addColumns(asc, colName);
        }
    }

    private void saveColumnsDefaults(String[] defaults, Boolean[] required, Column cl, int j) throws IOException {

        PropertyMap map = cl.getProperties();
        if (defaults != null && j < defaults.length && defaults[j] != null) {
            map.put(PropertyMap.DEFAULT_VALUE_PROP, DataType.TEXT, defaults[j]);
        }

        if (required != null && j < required.length && required[j] != null && !cl.isAutoNumber()) {
            map.put(PropertyMap.REQUIRED_PROP, DataType.BOOLEAN, required[j]);
        }

        map.save();
    }

    private void saveColumnsDefaults(String[] defaults, Boolean[] required, Table table) throws IOException {
        List<? extends Column> cols = table.getColumns();
        int j = 0;

        if (defaults != null || required != null) {
            for (Column cl : cols) {
                saveColumnsDefaults(defaults, required, cl, j);
                j++;
            }
        }

    }

    private String escape4Hsqldb(String tn) {

        if ((tn.startsWith("[") && tn.endsWith("]")) || (tn.startsWith("`") && tn.endsWith("`"))) {
            tn = tn.substring(1, tn.length() - 1);
            return SQLConverter.preEscapingIdentifier(tn);
        }
        return tn;
    }

    private String escape4Access(String tn) {

        if ((tn.startsWith("[") && tn.endsWith("]")) || (tn.startsWith("`") && tn.endsWith("`"))) {
            return tn.substring(1, tn.length() - 1);
        }
        return tn;
    }

    private String getUcaMetadataTypeName(int colIdx, ColumnBuilder cb, String[] types) {
        String ucaMetadataTypeName = cb.getType().name();
        if ((types != null) && (colIdx < types.length)) {
            if (types[colIdx].toUpperCase(Locale.US).equals("HYPERLINK")) {
                ucaMetadataTypeName = types[colIdx].toUpperCase(Locale.US);
            }
        }
        return ucaMetadataTypeName;
    }

    public void createTable(String tableName, Map<String, String> columnMap, String[] types, String[] defaults,
            Boolean[] notNulls) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        String tn = escape4Access(tableName);
        String ntn = escape4Hsqldb(tableName);
        Metadata mtd = new Metadata(conn.getHSQLDBConnection());
        TableBuilder tb = new TableBuilder(tn);
        int idTable = mtd.newTable(tn, ntn, Metadata.Types.TABLE);
        Collection<ColumnBuilder> lcb = getColumns(ntn, columnMap, types);
        tb.addColumns(lcb);
        int colIdx = 0;
        for (ColumnBuilder cb : lcb) {
            mtd.newColumn(cb.getName(), SQLConverter.preEscapingIdentifier(cb.getName()),
                    getUcaMetadataTypeName(colIdx, cb, types), idTable);
            colIdx++;
        }

        List<IndexBuilder> arcl = getIndexBuilders(ntn, columnMap);

        IndexBuilder ibpk = getIndexBuilderPK(ntn, columnMap);

        checkPK(arcl, ibpk);
        if (ibpk != null) {
            arcl.add(ibpk);
        }
        for (IndexBuilder ixb : arcl) {
            tb.addIndex(ixb);
        }
        Table table = tb.toTable(db);
        saveColumnsDefaults(defaults, notNulls, table);
        LoadJet lj = new LoadJet(conn.getHSQLDBConnection(), db);
        lj.loadDefaultValues(table);
        createForeignKeys(tableName);
        Statement st = null;
        try {
            st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
            List<String> clns = this.getColumnNamesCreate(tn);
            while (rs.next()) {
                Object[] record = new Object[clns.size()];
                int i = 0;
                for (String columnName : clns) {
                    record[i++] = rs.getObject(columnName);
                }
                new InsertCommand(table, record, null).persist();
            }
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    public void dropTable(String tableName) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        tableName = this.escape4Access(tableName);
        Table t = db.getTable(tableName);

        if (t == null) {
            return;
        }
        Metadata mt = new Metadata(conn.getHSQLDBConnection());
        mt.dropTable(t.getName());
        if (!HibernateSupport.isActive()) {
            Cursor c = t.getDefaultCursor();
            while (c.getNextRow() != null) {
                c.deleteCurrentRow();
            }
        }
        DatabaseImpl dbi = (DatabaseImpl) db;
        Table cat = dbi.getSystemCatalog();
        Map<String, Object> row;
        Cursor catc = cat.getDefaultCursor();
        while ((row = catc.getNextRow()) != null) {
            String name = (String) row.get("Name");
            if (name != null && name.equalsIgnoreCase(tableName)) {
                Integer id = (Integer) row.get("Id");
                Table tsa = db.getSystemTable("MSysACEs");
                HashMap<String, Object> rowtsa = new HashMap<String, Object>();
                rowtsa.put("ObjectId", id);
                Cursor cur = tsa.getDefaultCursor();
                if (cur.findNextRow(rowtsa)) {
                    cur.deleteCurrentRow();
                }
                catc.deleteCurrentRow();
                Table srs = db.getSystemTable("MSysRelationships");
                Cursor srsc = srs.getDefaultCursor();
                while ((row = srsc.getNextRow()) != null) {
                    String szObject = (String) row.get("szObject");
                    String szReferencedObject = (String) row.get("szReferencedObject");
                    if ((szObject != null && szObject.equalsIgnoreCase(tableName))
                            || (szReferencedObject != null && szReferencedObject.equalsIgnoreCase(tableName))) {
                        srsc.deleteCurrentRow();
                    }
                }
            }
        }

        conn.reloadDbIO();

    }

    public void renameTable(String oldTableName, String newTableName) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        oldTableName = this.escape4Access(oldTableName);
        String tn = this.escape4Access(newTableName);
        String ntn = escape4Hsqldb(newTableName);
        Table t = db.getTable(oldTableName);

        if (t == null) {
            return;
        }
        Metadata mt = new Metadata(conn.getHSQLDBConnection());
        mt.rename(t.getName(), tn, ntn);
        DatabaseImpl dbi = (DatabaseImpl) db;
        Table cat = dbi.getSystemCatalog();
        Map<String, Object> row;
        Cursor catc = cat.getDefaultCursor();
        while ((row = catc.getNextRow()) != null) {
            String name = (String) row.get("Name");
            if (name != null && name.equalsIgnoreCase(oldTableName)) {
                Integer id = (Integer) row.get("Id");
                HashMap<String, Object> rowtsa = new HashMap<String, Object>();
                rowtsa.put("ObjectId", id);
                Row r = catc.getCurrentRow();
                r.put("Name", tn);
                catc.updateCurrentRowFromMap(r);
                Table srs = db.getSystemTable("MSysRelationships");
                Cursor srsc = srs.getDefaultCursor();

                while ((row = srsc.getNextRow()) != null) {
                    String szObject = (String) row.get("szObject");
                    String szReferencedObject = (String) row.get("szReferencedObject");
                    boolean updated = false;
                    if (szObject != null && szObject.equalsIgnoreCase(oldTableName)) {
                        row.put("szObject", tn);
                        updated = true;
                    }

                    if (szReferencedObject != null && szReferencedObject.equalsIgnoreCase(oldTableName)) {
                        row.put("szReferencedObject", tn);
                        updated = true;
                    }
                    if (updated) {
                        srsc.updateCurrentRowFromMap(row);
                    }
                }
            }
        }

        conn.reloadDbIO();

    }

    public void addColumn(String tableName, String columnName, Map<String, String> columnMap, String[] types,
            String[] defaults, Boolean[] notNulls) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        String tn = escape4Access(tableName);
        String ntn = escape4Hsqldb(tableName);
        Metadata mtd = new Metadata(conn.getHSQLDBConnection());
        ColumnBuilder cb = this.getColumn(ntn, columnMap, types);
        Table t = db.getTable(tn);
        Column cl = cb.addToTable(t);
        int idTable = mtd.getTableId(ntn.toUpperCase());
        mtd.newColumn(cb.getName(), SQLConverter.preEscapingIdentifier(cb.getName()),
                getUcaMetadataTypeName(0, cb, types), idTable);
        saveColumnsDefaults(defaults, notNulls, cl, 0);
        updateNewColumn2Defaut(tableName, columnName, t, cl);
        setHsqldbNotNull(tableName, columnName, types[0], cl);
        conn.reloadDbIO();
    }

    private void setHsqldbNotNull(String tableName, String columnName, String type, Column cl)
            throws SQLException, IOException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Boolean req = (Boolean) cl.getProperties().getValue(PropertyMap.REQUIRED_PROP);
        req = req != null && req;
        Statement stNN = null;
        try {
            if (req) {
                stNN = conn.getHSQLDBConnection().createStatement();
                stNN.execute(SQLConverter
                        .convertSQL("ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " SET NOT NULL ")
                        .getSql());
            }
        } finally {
            if (stNN != null) {
                stNN.close();
            }
        }
    }

    private void updateNewColumn2Defaut(String tableName, String columnName, Table t, Column cl)
            throws SQLException, IOException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        LoadJet lj = new LoadJet(conn.getHSQLDBConnection(), conn.getDbIO());
        lj.loadDefaultValues(cl);
        String default4SQL = lj.defaultValue4SQL(cl);
        PreparedStatement ps = null;
        Object defObj = lj.tryDefault(default4SQL);
        conn.setFeedbackState(true);
        if (default4SQL != null) {
            for (Row row : t) {
                row.put(cl.getName(), defObj);
                t.updateRow(row);
            }
            conn.getDbIO().flush();

        }

        if (default4SQL != null || cl.getType().equals(DataType.BOOLEAN)) {
            try {
                defObj = default4SQL == null && cl.getType().equals(DataType.BOOLEAN) ? Boolean.FALSE : defObj;
                ps = conn.getHSQLDBConnection().prepareStatement(
                        SQLConverter.convertSQL("UPDATE " + tableName + " SET " + columnName + "=" + "?").getSql());
                ps.setObject(1, defObj);
                ps.executeUpdate();
            } finally {
                if (ps != null) {
                    ps.close();
                }
            }
        }

        conn.setFeedbackState(false);
    }

    public void createIndex(String tableName, String indexName) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        String ntn = escape4Hsqldb(tableName);
        String idn = escape4Hsqldb(indexName);
        String tn = escape4Access(tableName);
        String in = escape4Access(indexName);
        Table t = db.getTable(tn);

        ResultSet idxrs =
                conn.getHSQLDBConnection().getMetaData().getIndexInfo(null, "PUBLIC", ntn.toUpperCase(), false, false);
        boolean asc = false;
        ArrayList<String> cols = new ArrayList<String>();
        IndexBuilder ib = new IndexBuilder(in);
        while (idxrs.next()) {
            String dbIdxName = idxrs.getString("INDEX_NAME");
            if (dbIdxName.equalsIgnoreCase(idn)) {

                boolean unique = !idxrs.getBoolean("NON_UNIQUE");

                if (unique) {
                    ib.setUnique();
                }
                String colName = idxrs.getString("COLUMN_NAME");
                Metadata mt = new Metadata(conn);
                colName = mt.getColumnName(ntn, colName);
                String ad = idxrs.getString("ASC_OR_DESC");
                asc = ad == null || ad.equals("A");
                cols.add(colName);

            }
        }
        ib.addColumns(asc, cols.toArray(new String[cols.size()])).addToTable(t);
    }

    public void createPrimaryKey(String tableName) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        String ntn = escape4Hsqldb(tableName);
        String tn = escape4Access(tableName);
        Table t = db.getTable(tn);
        ResultSet pkrs = conn.getHSQLDBConnection().getMetaData().getPrimaryKeys(null, null, ntn.toUpperCase());
        ArrayList<String> cols = new ArrayList<String>();
        IndexBuilder ib = new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME);
        ib.setPrimaryKey();
        while (pkrs.next()) {
            String colName = pkrs.getString("COLUMN_NAME");
            Metadata mt = new Metadata(conn);
            colName = mt.getColumnName(ntn, colName);
            cols.add(colName);
        }
        ib.addColumns(cols.toArray(new String[cols.size()])).addToTable(t);
    }

    public void createForeignKey(String tableName, String referencedTable) throws IOException, SQLException {
        createForeignKey(tableName, referencedTable, null);
    }

    public void createForeignKey(String tableName, String referencedTable, String relationshipName)
            throws IOException, SQLException {
        String ntn = escape4Hsqldb(tableName);
        String rntn = escape4Hsqldb(referencedTable);
        String tn = escape4Access(tableName);
        String rtn = escape4Access(referencedTable);
        String relName = null;
        if (relationshipName != null) {
            relName = escape4Access(relationshipName);
        }
        createForeignKey(ntn, rntn, tn, rtn, relName);
    }

    public void createForeignKeys(String tableName) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        String ntn = escape4Hsqldb(tableName);
        String tn = escape4Access(tableName);
        ResultSet fkrs = conn.getHSQLDBConnection().getMetaData().getImportedKeys(null, null, ntn.toUpperCase());
        HashSet<String> hs = new HashSet<String>();
        while (fkrs.next()) {
            hs.add(fkrs.getString("PKTABLE_NAME"));
        }
        Metadata mt = new Metadata(conn);
        for (String rntn : hs) {
            createForeignKey(ntn, rntn, tn, mt.getTableName(rntn), null);
        }
    }

    private void createForeignKey(String tn4Hsqldb, String refTn4Hsqldb, String tn4Access, String refTn4Access,
            String relationshipName) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        Table t = db.getTable(tn4Access);
        Table rt = db.getTable(refTn4Access);
        RelationshipBuilder rb = new RelationshipBuilder(rt, t);
        rb.setName(relationshipName);
        rb.setReferentialIntegrity();
        ResultSet fkrs = conn.getHSQLDBConnection().getMetaData().getCrossReference(null, null,
                refTn4Hsqldb.toUpperCase(), null, null, tn4Hsqldb.toUpperCase());
        Metadata mt = new Metadata(conn);
        while (fkrs.next()) {
            String colName = fkrs.getString("FKCOLUMN_NAME");
            colName = mt.getColumnName(tn4Hsqldb, colName);
            String rcolName = fkrs.getString("PKCOLUMN_NAME");
            rcolName = mt.getColumnName(refTn4Hsqldb, rcolName);
            rb.addColumns(rcolName, colName);
            short dr = fkrs.getShort("DELETE_RULE");
            short ur = fkrs.getShort("UPDATE_RULE");
            switch (dr) {
            case DatabaseMetaData.importedKeyCascade:
                rb.setCascadeDeletes();
                break;
            case DatabaseMetaData.importedKeySetNull:
                rb.setCascadeNullOnDelete();
                break;
            }
            if (ur == DatabaseMetaData.importedKeyCascade) {
                rb.setCascadeUpdates();
            }

        }
        rb.toRelationship(db);

    }

    public void dropForeignKey(String relationshipName) throws IOException, SQLException {
        relationshipName = escape4Access(relationshipName);
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        Table tbl = db.getSystemTable("MSysRelationships");
        IndexCursor crsr = CursorBuilder.createCursor(tbl.getIndex("szRelationship"));
        Row r = crsr.findRowByEntry(relationshipName);
        if (r == null) {
            // System.out.printf("Relationship [%s] not found.%n", relationshipName);
        } else {
            while (r != null) {
                tbl.deleteRow(r);
                r = crsr.findRowByEntry(relationshipName);
            }
            tbl = db.getSystemTable("MSysObjects");
            crsr = CursorBuilder.createCursor(tbl.getIndex("ParentIdName"));
            Map<String, Object> rowPattern = new HashMap<String, Object>();
            rowPattern.put("Name", "Relationships");
            rowPattern.put("Type", 3);
            if (crsr.findFirstRow(rowPattern)) {
                Integer relationshipsId = crsr.getCurrentRow().getInt("Id");
                r = crsr.findRowByEntry(relationshipsId, relationshipName);
                if (r != null) {
                    Integer relationshipId = r.getInt("Id");
                    tbl.deleteRow(r);
                    tbl = db.getSystemTable("MSysACEs");
                    crsr = CursorBuilder.createCursor(tbl.getIndex("ObjectId"));
                    r = crsr.findRowByEntry(relationshipId);
                    while (r != null) {
                        tbl.deleteRow(r);
                        r = crsr.findRowByEntry(relationshipId);
                    }
                }
            }
        }
    }

}
