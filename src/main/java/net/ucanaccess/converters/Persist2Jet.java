package net.ucanaccess.converters;

import static net.ucanaccess.type.SqlConstants.*;

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import net.ucanaccess.commands.InsertCommand;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.complex.UnsupportedValue;
import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.exception.UnsupportedTypeException;
import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessDatabaseMetadata;
import net.ucanaccess.jdbc.UcanaccessStatement;
import net.ucanaccess.type.ObjectType;
import net.ucanaccess.util.HibernateSupport;
import org.hsqldb.SessionInterface;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimestampData;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class Persist2Jet {
    private static final Map<String, List<String>> COL_NAMES_CACHE = new HashMap<>();
    static {
        DBReference.addOnReloadRefListener(COL_NAMES_CACHE::clear);
    }

    public Map<String, Object> getRowPattern(Object[] varr, Table t) throws SQLException {
        String ntn = SQLConverter.basicEscapingIdentifier(t.getName()).toUpperCase();
        Map<String, Object> vl = new LinkedHashMap<>();
        int i = 0;
        for (String s : getColumnNames(ntn)) {
            vl.put(s, varr[i++]);
        }
        if (i == 0) {
            throw new SQLException("Cannot read table's metadata");
        }
        return escapeIdentifiers(vl, t);
    }

    public Object[] getValues(Map<String, Object> rowPattern, Table t) {
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
        if (!COL_NAMES_CACHE.containsKey(key)) {
            ResultSet rs = conq.getMetaData().getColumns(null, PUBLIC, ntn, null);
            Map<Integer, String> tm = new TreeMap<>();
            while (rs.next()) {
                String cbase = rs.getString(COLUMN_NAME);
                Integer i = rs.getInt(ORDINAL_POSITION);
                tm.put(i, cbase.toUpperCase());

            }
            List<String> ar = new ArrayList<>(tm.values());
            COL_NAMES_CACHE.put(key, ar);
        }
        return COL_NAMES_CACHE.get(key);
    }

    private List<String> getColumnNamesCreate(String ntn) throws SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        List<String> ar = new ArrayList<>();
        ResultSet rs = conn.getMetaData().getColumns(null, PUBLIC, ntn, null);
        while (rs.next()) {
            String cbase = rs.getString(COLUMN_NAME);
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
                    if (value instanceof TimestampData && column.getType().equals(DataType.SHORT_DATE_TIME)) {
                        TimestampData ts = (TimestampData) value;
                        LocalDateTime val = LocalDateTime.of(1970, 1, 1, 0, 0)
                                .plusSeconds(ts.getSeconds())
                                .plusNanos(ts.getNanos());
                        values[i] = val;
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
                            throw new UnsupportedTypeException(Optional.ofNullable(obj).map(o -> o.getClass().getName()).orElse("null"));
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
        } catch (Exception _ex) {
            throw new SQLException(_ex);
        }
    }

    private Map<String, Object> escapeIdentifiers(Map<String, Object> map, Table t) {
        List<? extends Column> colums = t.getColumns();
        Map<String, Object> vl = new LinkedHashMap<>();
        for (Column col : colums) {
            String key = col.getName();
            String keyu = key.toUpperCase();
            String ekey = map.containsKey(keyu) ? keyu : SQLConverter.escapeIdentifier(key).toUpperCase();
            if (!map.containsKey(ekey) && map.containsKey(ekey.substring(1, ekey.length() - 1))) {
                ekey = ekey.substring(1, ekey.length() - 1);
            }
            vl.put(key, map.get(ekey));
        }
        return vl;
    }

    private String getNormalizedName(String _name, Map<String, String> _columnMap) {
        return _columnMap == null ? _name : _columnMap.getOrDefault(_name, _name);
    }

    private ColumnBuilder getColumn(ResultSet _rs, int _seq, Map<String, String> _columnMap, String[] _types) throws SQLException, IOException {
        String name = _rs.getString(COLUMN_NAME);
        String nname = getNormalizedName(name, _columnMap);
        ColumnBuilder cb = new ColumnBuilder(nname);
        short length = (short) _rs.getInt(COLUMN_SIZE);
        byte scale = (byte) _rs.getInt(DECIMAL_DIGITS);
        DataType dt = null;
        if (length == 0 && _types != null) {
            if (_types[_seq].equalsIgnoreCase(AccessType.MEMO.name())
                    || _types[_seq].equalsIgnoreCase(AccessType.HYPERLINK.name())) {
                dt = DataType.MEMO;
                cb.setType(dt);
                if (_types[_seq].equalsIgnoreCase(AccessType.HYPERLINK.name())) {
                    cb.setHyperlink(true);
                }
            }
            if (_types[_seq].equalsIgnoreCase(AccessType.TEXT.name())) {
                dt = DataType.TEXT;
                cb.setType(dt);
            }
        }

        if (_types != null && _seq < _types.length && _types[_seq] != null
                && (_types[_seq].equalsIgnoreCase(AccessType.LONG.name())
                        || _types[_seq].equalsIgnoreCase(AccessType.BYTE.name())
                        || _types[_seq].equalsIgnoreCase(AccessType.CURRENCY.name())
                        || _types[_seq].equalsIgnoreCase(AccessType.INTEGER.name())
                        || _types[_seq].equalsIgnoreCase(AccessType.SINGLE.name())
                        || _types[_seq].equalsIgnoreCase(AccessType.DOUBLE.name())
                        || _types[_seq].equalsIgnoreCase(AccessType.YESNO.name())
                        || _types[_seq].equalsIgnoreCase(AccessType.DATETIME.name())
                        || _types[_seq].equalsIgnoreCase(AccessType.COUNTER.name())
                        || _types[_seq].equalsIgnoreCase(AccessType.AUTOINCREMENT.name()))) {
            dt = TypesMap.map2Jackcess(AccessType.valueOf(_types[_seq].toUpperCase(Locale.US)));
            cb.setType(dt);
            cb.setLengthInUnits((short) dt.getFixedSize());
        }

        if (dt == null) {
            if (_types != null && _seq < _types.length && _types[_seq] != null
                    && _types[_seq].equalsIgnoreCase(AccessType.NUMERIC.name())) {
                dt = DataType.NUMERIC;
            } else {
                dt = DataType.fromSQLType(
                        _rs.getInt(DATA_TYPE),
                        length,
                        UcanaccessConnection.getCtxConnection().getDbIO().getFileFormat());
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

        if (_types != null && _seq < _types.length) {
            if (_types[_seq].equalsIgnoreCase(AccessType.COUNTER.name())
                    || _types[_seq].equalsIgnoreCase(AccessType.AUTOINCREMENT.name())) {
                cb.setAutoNumber(true);
                cb.putProperty(PropertyMap.REQUIRED_PROP, false); // re: Ticket #2
            }
            if (_types[_seq].equalsIgnoreCase(AccessType.GUID.name())) {
                cb.setType(DataType.GUID);
                cb.setAutoNumber(true);
            }
        }
        return cb;
    }

    private ColumnBuilder getColumn(String _tableName, Map<String, String> _columnMap, String[] _types)
            throws SQLException, IOException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        String columnName = _columnMap.keySet().iterator().next();
        ResultSet rs = conn.getHSQLDBConnection().getMetaData().getColumns(null, PUBLIC, _tableName.toUpperCase(),
                SQLConverter.preEscapingIdentifier(columnName));

        if (rs.next()) {
            return getColumn(rs, 0, _columnMap, _types);

        }
        return null;
    }

    private Collection<ColumnBuilder> getColumns(String tableName, Map<String, String> columnMap, String[] types)
            throws SQLException, IOException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Map<Integer, ColumnBuilder> ordm = new TreeMap<>();
        String tableNamePattern = tableName.toUpperCase(Locale.US).replace("_", "\\_");
        ResultSet rs = conn.getHSQLDBConnection().getMetaData().getColumns(null, PUBLIC, tableNamePattern, null);
        while (rs.next()) {
            int seq = rs.getInt(ORDINAL_POSITION) - 1;
            ordm.put(seq, getColumn(rs, seq, columnMap, types));
        }
        return ordm.values();
    }

    private List<IndexBuilder> getIndexBuilders(String tableName, Map<String, String> columnMap) throws SQLException {
        List<IndexBuilder> arcl = new ArrayList<>();
        addIndexBuildersSimple(tableName, columnMap, arcl);
        return arcl;
    }

    private void checkPK(List<IndexBuilder> _arcl, IndexBuilder _ibpk) {
        if (_ibpk == null) {
            return;
        }
        Iterator<IndexBuilder> itib = _arcl.iterator();
        List<IndexBuilder.Column> clspk = _ibpk.getColumns();
        List<String> columnNamesPK = new ArrayList<>();
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
        ResultSet pkrs = conn.getMetaData().getPrimaryKeys(null, PUBLIC, tableName.toUpperCase());
        IndexBuilder indpk = null;
        while (pkrs.next()) {
            if (indpk == null) {
                String indexName = IndexBuilder.PRIMARY_KEY_NAME;
                indpk = new IndexBuilder(indexName);
                indpk.setPrimaryKey();

            }
            indpk.addColumns(getNormalizedName(pkrs.getString(COLUMN_NAME), columnMap));
        }
        return indpk;
    }

    private void addIndexBuildersSimple(String tableName, Map<String, String> columnMap, List<IndexBuilder> arcl)
            throws SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        ResultSet idxrs = conn.getMetaData().getIndexInfo(null, PUBLIC, tableName, false, false);
        Map<String, IndexBuilder> hi = new HashMap<>();
        for (IndexBuilder ib : arcl) {
            hi.put(ib.getName(), ib);
        }
        while (idxrs.next()) {
            String colName = getNormalizedName(idxrs.getString(COLUMN_NAME), columnMap);
            String indexName = idxrs.getString(INDEX_NAME);
            boolean unique = !idxrs.getBoolean(NON_UNIQUE);
            String ad = idxrs.getString(ASC_OR_DESC);
            boolean asc = ad == null || "A".equals(ad);
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

    private void saveColumnsDefaults(String[] _defaults, Boolean[] _required, Column _cl, int _j) throws IOException {

        PropertyMap map = _cl.getProperties();
        if (_defaults != null && _j < _defaults.length && _defaults[_j] != null) {
            map.put(PropertyMap.DEFAULT_VALUE_PROP, DataType.TEXT, _defaults[_j]);
        }

        if (_required != null && _j < _required.length && _required[_j] != null && !_cl.isAutoNumber()) {
            map.put(PropertyMap.REQUIRED_PROP, DataType.BOOLEAN, _required[_j]);
        }

        map.save();
    }

    private void saveColumnsDefaults(String[] defaults, Boolean[] required, Table table) throws IOException {
        List<? extends Column> cols = table.getColumns();
        int j = 0;

        if (defaults != null || required != null) {
            for (Column col : cols) {
                saveColumnsDefaults(defaults, required, col, j);
                j++;
            }
        }

    }

    private String escape4Hsqldb(String tn) {

        if (tn.startsWith("[") && tn.endsWith("]") || tn.startsWith("`") && tn.endsWith("`")) {
            tn = tn.substring(1, tn.length() - 1);
            return SQLConverter.preEscapingIdentifier(tn);
        }
        return tn;
    }

    private String escape4Access(String tn) {

        if (tn.startsWith("[") && tn.endsWith("]") || tn.startsWith("`") && tn.endsWith("`")) {
            return tn.substring(1, tn.length() - 1);
        }
        return tn;
    }

    private String getUcaMetadataTypeName(int colIdx, ColumnBuilder cb, String[] types) {
        String ucaMetadataTypeName = cb.getType().name();
        if (types != null && colIdx < types.length) {
            if (types[colIdx].toUpperCase(Locale.US).equals("HYPERLINK")) {
                ucaMetadataTypeName = types[colIdx].toUpperCase(Locale.US);
            }
        }
        return ucaMetadataTypeName;
    }

    public void createTable(String tableName, Map<String, String> columnMap, String[] types, String[] defaults,
            Boolean[] notNulls) throws IOException, SQLException {

        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        final Database db = conn.getDbIO();
        String tn = escape4Access(tableName);
        String ntn = escape4Hsqldb(tableName);
        Metadata mtd = new Metadata(conn.getHSQLDBConnection());
        TableBuilder tb = new TableBuilder(tn);
        int idTable = mtd.newTable(tn, ntn, ObjectType.TABLE);
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
        try (UcanaccessStatement st = conn.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
            List<String> clns = getColumnNamesCreate(tn);
            while (rs.next()) {
                Object[] rec = new Object[clns.size()];
                int i = 0;
                for (String columnName : clns) {
                    rec[i++] = rs.getObject(columnName);
                }
                new InsertCommand(table, rec, null).persist();
            }
        }
    }

    public void dropTable(String tableName) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        tableName = escape4Access(tableName);
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
                Map<String, Object> rowtsa = new HashMap<>();
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
                    if (szObject != null && szObject.equalsIgnoreCase(tableName)
                            || szReferencedObject != null && szReferencedObject.equalsIgnoreCase(tableName)) {
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
        oldTableName = escape4Access(oldTableName);
        String tn = escape4Access(newTableName);
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
                Map<String, Object> rowtsa = new HashMap<>();
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

    public void addColumn(String _tableName, String _columnName, Map<String, String> _columnMap, String[] _types,
        String[] _defaults, Boolean[] _notNulls) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        String tn = escape4Access(_tableName);
        String ntn = escape4Hsqldb(_tableName);
        Metadata mtd = new Metadata(conn.getHSQLDBConnection());
        ColumnBuilder cb = getColumn(ntn, _columnMap, _types);
        if (cb == null) {
            return;
        }
        Table t = db.getTable(tn);
        Column col = cb.addToTable(t);
        int idTable = mtd.getTableId(ntn.toUpperCase());
        mtd.newColumn(cb.getName(), SQLConverter.preEscapingIdentifier(cb.getName()),
            getUcaMetadataTypeName(0, cb, _types), idTable);
        saveColumnsDefaults(_defaults, _notNulls, col, 0);
        updateNewColumn2Defaut(_tableName, _columnName, t, col);
        setHsqldbNotNull(_tableName, _columnName, col);
        conn.reloadDbIO();
    }

    private void setHsqldbNotNull(String _tableName, String _columnName, Column _cl) throws SQLException, IOException {
        boolean req = Optional.ofNullable(_cl.getProperties().getValue(PropertyMap.REQUIRED_PROP))
            .map(Boolean.class::cast).orElse(false);
        if (req) {
            UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
            try (Statement stNN = conn.getHSQLDBConnection().createStatement()) {
                stNN.execute(SQLConverter.convertSQL(
                    "ALTER TABLE " + _tableName + " ALTER COLUMN " + _columnName + " SET NOT NULL ").getSql());
            }
        }
    }

    private void updateNewColumn2Defaut(String _tableName, String _columnName, Table t, Column _col)
        throws SQLException, IOException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        LoadJet lj = new LoadJet(conn.getHSQLDBConnection(), conn.getDbIO());
        lj.loadDefaultValues(_col);
        String default4SQL = lj.defaultValue4SQL(_col);

        Object defObj = lj.tryDefault(default4SQL);
        conn.setFeedbackState(true);
        if (default4SQL != null) {
            for (Row row : t) {
                row.put(_col.getName(), defObj);
                t.updateRow(row);
            }
            conn.getDbIO().flush();

        }

        if (default4SQL != null || _col.getType().equals(DataType.BOOLEAN)) {
            defObj = default4SQL == null && _col.getType().equals(DataType.BOOLEAN) ? Boolean.FALSE : defObj;
            try (PreparedStatement ps = conn.getHSQLDBConnection().prepareStatement(
                SQLConverter.convertSQL("UPDATE " + _tableName + " SET " + _columnName + "= ?").getSql())) {
                ps.setObject(1, defObj);
                ps.executeUpdate();
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

        ResultSet idxrs = conn.getHSQLDBConnection().getMetaData().getIndexInfo(null, PUBLIC, ntn.toUpperCase(), false, false);
        boolean asc = false;
        List<String> cols = new ArrayList<>();
        IndexBuilder ib = new IndexBuilder(in);
        while (idxrs.next()) {
            String dbIdxName = idxrs.getString(INDEX_NAME);
            if (dbIdxName.equalsIgnoreCase(idn)) {

                boolean unique = !idxrs.getBoolean(NON_UNIQUE);

                if (unique) {
                    ib.setUnique();
                }
                String colName = idxrs.getString(COLUMN_NAME);
                Metadata mt = new Metadata(conn);
                colName = mt.getColumnName(ntn, colName);
                String ad = idxrs.getString(ASC_OR_DESC);
                asc = ad == null || "A".equals(ad);
                cols.add(colName);

            }
        }
        ib.addColumns(asc, cols.toArray(new String[0])).addToTable(t);
    }

    public void createPrimaryKey(String tableName) throws IOException, SQLException {
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        String ntn = escape4Hsqldb(tableName);
        String tn = escape4Access(tableName);
        Table t = db.getTable(tn);
        ResultSet pkrs = conn.getHSQLDBConnection().getMetaData().getPrimaryKeys(null, null, ntn.toUpperCase());
        List<String> cols = new ArrayList<>();
        IndexBuilder ib = new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME);
        ib.setPrimaryKey();
        while (pkrs.next()) {
            String colName = pkrs.getString(COLUMN_NAME);
            Metadata mt = new Metadata(conn);
            colName = mt.getColumnName(ntn, colName);
            cols.add(colName);
        }
        ib.addColumns(cols.toArray(new String[0])).addToTable(t);
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
        Set<String> hs = new HashSet<>();
        while (fkrs.next()) {
            hs.add(fkrs.getString(PKTABLE_NAME));
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
            /*
             * The following check was added for ticket #16.
             *
             * Limitation: If the user wants to create more than one relationship between the same two tables
             * then all of the relationships must be explicitly named.
             */
            if (relationshipName == null || fkrs.getString(FK_NAME).equalsIgnoreCase(tn4Hsqldb + "_" + relationshipName)) {
                String colName = fkrs.getString(FKCOLUMN_NAME);
                colName = mt.getColumnName(tn4Hsqldb, colName);
                String rcolName = fkrs.getString(PKCOLUMN_NAME);
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
                default:
                    break;
                }
                if (ur == DatabaseMetaData.importedKeyCascade) {
                    rb.setCascadeUpdates();
                }
            }
        }
        rb.toRelationship(db);

    }

    public void dropForeignKey(String relationshipName) throws IOException {
        relationshipName = escape4Access(relationshipName);
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        Database db = conn.getDbIO();
        Table tbl = db.getSystemTable("MSysRelationships");
        IndexCursor crsr = CursorBuilder.createCursor(tbl.getIndex("szRelationship"));
        Row r = crsr.findRowByEntry(relationshipName);
        if (r != null) {
            while (r != null) {
                tbl.deleteRow(r);
                r = crsr.findRowByEntry(relationshipName);
            }
            tbl = db.getSystemTable("MSysObjects");
            crsr = CursorBuilder.createCursor(tbl.getIndex("ParentIdName"));
            Map<String, Object> rowPattern = new HashMap<>();
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
