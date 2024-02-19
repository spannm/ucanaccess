package net.ucanaccess.commands;

import io.github.spannm.jackcess.Database;
import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.converters.Metadata;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.converters.SQLConverter.DDLType;
import net.ucanaccess.exception.InvalidCreateStatementException;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessStatement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class DDLCommandEnlist {
    private String[]            types;
    private String[]            defaults;
    private Boolean[]           notNulls;
    private Map<String, String> columnMap = new HashMap<>();

    private void enlistCreateTable(String _sql, DDLType _ddlType) throws SQLException {
        String tn = _ddlType.getDBObjectName();
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        String execId = UcanaccessConnection.getCtxExcId();
        Connection hsqlConn = ac.getHSQLDBConnection();
        Database db = ac.getDbIO();
        LoadJet lfa = new LoadJet(hsqlConn, db);
        String ntn = tn;
        if (tn.startsWith("[") && tn.endsWith("]") || tn.startsWith("`") && tn.endsWith("`")) {
            ntn = SQLConverter.escapeIdentifier(tn.substring(1, tn.length() - 1));
        }

        lfa.synchronisationTriggers(ntn, true, true);
        CreateTableCommand c4io;
        if (_ddlType.equals(DDLType.CREATE_TABLE)) {
            parseTypesFromCreateStatement(_sql);
            c4io = new CreateTableCommand(tn, execId, columnMap, types, defaults, notNulls);
        } else {
            try (UcanaccessStatement st = ac.createStatement()) {
                ResultSet rs = st.executeQuery(_ddlType.getSelect(_sql));
                ResultSetMetaData rsmd = rs.getMetaData();
                Metadata mt = new Metadata(ac.getHSQLDBConnection());
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (rsmd.getColumnName(i).equals(rsmd.getColumnLabel(i))) {
                        columnMap.put(mt.getEscapedColumnName(rsmd.getTableName(i), rsmd.getColumnName(i)),
                                rsmd.getColumnLabel(i));
                    } else {
                        columnMap.put(SQLConverter.preEscapingIdentifier(rsmd.getColumnLabel(i)),
                                rsmd.getColumnLabel(i));
                    }
                }
                c4io = new CreateTableCommand(tn, execId, columnMap);
            } catch (Exception _ignored) {
                c4io = new CreateTableCommand(tn, execId);
            }

        }

        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    public void enlistDDLCommand(String sql, DDLType ddlType) throws SQLException {
        switch (ddlType) {
        case CREATE_TABLE:
        case CREATE_TABLE_AS_SELECT:
            enlistCreateTable(sql, ddlType);
            break;
        case DROP_TABLE:
            enlistDropTable(ddlType);
            break;
        case ALTER_RENAME:
            enlistAlterRename(ddlType);
            break;
        case ADD_COLUMN:
            enlistAddColumn(sql, ddlType);
            break;
        case CREATE_INDEX:
            enlistCreateIndex(ddlType);
            break;
        case CREATE_PRIMARY_KEY:
            enlistCreatePrimaryKey(ddlType);
            break;
        case CREATE_FOREIGN_KEY:
            enlistCreateForeignKey(ddlType);
            break;
        case DROP_FOREIGN_KEY:
            enlistDropForeignKey(ddlType);
            break;
        default:
            break;
        }
    }

    private void enlistCreateForeignKey(DDLType _ddlType) throws SQLException {
        String tableName = _ddlType.getDBObjectName();
        String relationshipName = _ddlType.getSecondDBObjectName();
        String referencedTable = _ddlType.getThirdDBObjectName();
        String execId = UcanaccessConnection.getCtxExcId();
        CreateForeignKeyCommand c4io =
                new CreateForeignKeyCommand(tableName, referencedTable, execId, relationshipName);
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void enlistDropForeignKey(DDLType ddlType) throws SQLException {
        String relationshipName = ddlType.getSecondDBObjectName();
        String execId = UcanaccessConnection.getCtxExcId();
        DropForeignKeyCommand c4io = new DropForeignKeyCommand(execId, relationshipName);
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void enlistCreatePrimaryKey(DDLType ddlType) throws SQLException {
        String tableName = ddlType.getDBObjectName();
        String execId = UcanaccessConnection.getCtxExcId();
        CreatePrimaryKeyCommand c4io = new CreatePrimaryKeyCommand(tableName, execId);
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void enlistCreateIndex(DDLType ddlType) throws SQLException {
        String indexName = ddlType.getDBObjectName();
        String tableName = ddlType.getSecondDBObjectName();
        String execId = UcanaccessConnection.getCtxExcId();
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        CreateIndexCommand c4io = new CreateIndexCommand(indexName, tableName, execId);
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void enlistAddColumn(String sql, DDLType ddlType) throws SQLException {
        String tableName = ddlType.getDBObjectName();
        String columnName = ddlType.getSecondDBObjectName();
        String columnDefinition = ddlType.getColumnDefinition();
        String execId = UcanaccessConnection.getCtxExcId();
        List<String> typeList = new ArrayList<>();
        List<String> defaultList = new ArrayList<>();
        columnMap = new HashMap<>();
        List<Boolean> notNullList = new ArrayList<>();
        String tknt = columnName + columnDefinition;
        parseColumnTypes(typeList, defaultList, notNullList, tknt);
        check4OutOfPlacedNotNull(sql);
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        AddColumnCommand c4io = new AddColumnCommand(tableName, columnName, execId, columnMap, types,
                defaults, notNulls);
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void check4OutOfPlacedNotNull(String sql) {
        if (Pattern.compile(SQLConverter.NOT_NULL).matcher(sql).find()
            && notNulls.length > 0 && (notNulls[0] == null || !notNulls[0])) {
            notNulls[0] = true;
        }

    }

    private void enlistDropTable(DDLType ddlType) throws SQLException {
        String tn = ddlType.getDBObjectName();
        String execId = UcanaccessConnection.getCtxExcId();
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        DropTableCommand c4io = new DropTableCommand(tn, execId);
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void enlistAlterRename(DDLType ddlType) throws SQLException {
        String oldTn = ddlType.getDBObjectName();
        String newTn = ddlType.getSecondDBObjectName();
        String execId = UcanaccessConnection.getCtxExcId();
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        AlterRenameCommand c4io = new AlterRenameCommand(oldTn, newTn, execId);
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private String[] checkEscaped(String _ll, String _rl, String[] _colDecls, String _tknt) {
        if (_colDecls[0].startsWith(_ll) && _tknt.indexOf(_rl, 1) > 0) {
            for (int k = 0; k < _colDecls.length; k++) {
                if (_colDecls[k].endsWith(_rl)) {
                    String[] colDecls0 = new String[_colDecls.length - k];
                    colDecls0[0] = _tknt.substring(1, _tknt.substring(1).indexOf(_rl) + 1);
                    System.arraycopy(_colDecls, 1 + k, colDecls0, 1, colDecls0.length - 1);
                    _colDecls = colDecls0;
                    break;
                }
            }
        }
        return _colDecls;
    }

    private void parseColumnTypes(List<String> _typeList, List<String> _defaultList, List<Boolean> _notNullList, String _tknt) {

        String[] colDecls = _tknt.split("[\\s\n\r]+");
        colDecls = checkEscaped("[", "]", colDecls, _tknt);
        colDecls = checkEscaped("`", "`", colDecls, _tknt);
        String escaped = SQLConverter.isListedAsKeyword(colDecls[0].toUpperCase()) ? colDecls[0].toUpperCase()
                : SQLConverter.basicEscapingIdentifier(colDecls[0]);
        columnMap.put(escaped, colDecls[0]);

        boolean reset = false;
        if (_tknt.matches("[\\s\n\r]*\\d+[\\s\n\r]*\\).*")) {
            reset = true;
            _tknt = _tknt.substring(_tknt.indexOf(')') + 1).trim();
            colDecls = _tknt.split("[\\s\n\r]+");
        }

        if (!reset && colDecls.length < 2) {
            return;
        }
        boolean decDef = false;
        if (!reset) {
            if (colDecls[1] != null && colDecls[1].toUpperCase().startsWith("NUMERIC(")) {
                colDecls[1] = "NUMERIC";
                decDef = true;
            }
            _typeList.add(colDecls[1]);

        }

        if ((colDecls.length > 2 || reset && colDecls.length == 2)
                && "not".equalsIgnoreCase(colDecls[colDecls.length - 2])
                && "null".equalsIgnoreCase(colDecls[colDecls.length - 1])) {
            _notNullList.add(true);
        } else if (!decDef) {
            _notNullList.add(false);
        }

        if (!decDef) {
            _defaultList.add(value(SQLConverter.getDDLDefault(_tknt)));
        }

        types = _typeList.toArray(new String[0]);
        defaults = _defaultList.toArray(new String[0]);
        notNulls = _notNullList.toArray(new Boolean[0]);
    }

    // getting AUTOINCREMENT and GUID
    private void parseTypesFromCreateStatement(String _sql) throws SQLException {
        _sql = _sql.replaceAll("([\\s\n\r]+)((?i)DECIMAL)([\\s\n\r]*\\()", "$1NUMERIC(")
                .replaceAll("([\\s\n\r]+)((?i)NUMERIC)([\\s\n\r]*\\()", "$1NUMERIC(");
        int startDecl = _sql.indexOf('(');
        int endDecl = _sql.lastIndexOf(')');

        if (startDecl >= endDecl) {
            throw new InvalidCreateStatementException(_sql);
        }
        String decl = _sql.substring(startDecl + 1, endDecl);
        String[] tokens = decl.split(",");

        List<String> typeList = new ArrayList<>();
        List<String> defaultList = new ArrayList<>();
        columnMap = new HashMap<>();
        List<Boolean> notNullList = new ArrayList<>();
        for (String token : tokens) {
            String tknt = token.trim();
            parseColumnTypes(typeList, defaultList, notNullList, tknt);

        }
    }

    private String value(String _value) {
        if (_value == null) {
            return null;
        }
        if (_value.startsWith("\"") && _value.endsWith("\"")) {
            return _value.substring(1, _value.length() - 1).replace("\"\"", "\"");
        }
        if (_value.startsWith("'") && _value.endsWith("'")) {
            return _value.substring(1, _value.length() - 1).replace("''", "'");
        }
        return _value;
    }

}
