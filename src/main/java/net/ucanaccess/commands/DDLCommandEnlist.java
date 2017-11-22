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
package net.ucanaccess.commands;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.Database;

import net.ucanaccess.converters.LoadJet;
import net.ucanaccess.converters.Metadata;
import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.converters.SQLConverter.DDLType;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;

public class DDLCommandEnlist {
    private String[]            types;
    private String[]            defaults;
    private Boolean[]           notNulls;
    private Map<String, String> columnMap = new HashMap<String, String>();

    private void enlistCreateTable(String sql, DDLType ddlType) throws SQLException {
        String tn = ddlType.getDBObjectName();
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        String execId = UcanaccessConnection.getCtxExcId();
        Connection hsqlConn = ac.getHSQLDBConnection();
        Database db = ac.getDbIO();
        LoadJet lfa = new LoadJet(hsqlConn, db);
        String ntn = tn;
        if ((tn.startsWith("[") && tn.endsWith("]")) || (tn.startsWith("`") && tn.endsWith("`"))) {
            ntn = SQLConverter.escapeIdentifier(tn.substring(1, tn.length() - 1));
        }

        lfa.synchronisationTriggers(ntn, true, true);
        CreateTableCommand c4io;
        if (ddlType.equals(DDLType.CREATE_TABLE)) {
            parseTypesFromCreateStatement(sql);
            c4io = new CreateTableCommand(tn, execId, this.columnMap, this.types, this.defaults, this.notNulls);
        } else {
            try {
                Statement st = ac.createStatement();
                ResultSet rs = st.executeQuery(ddlType.getSelect(sql));
                ResultSetMetaData rsmd = rs.getMetaData();
                Metadata mt = new Metadata(ac.getHSQLDBConnection());
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (rsmd.getColumnName(i).equals(rsmd.getColumnLabel(i))) {
                        this.columnMap.put(mt.getEscapedColumnName(rsmd.getTableName(i), rsmd.getColumnName(i)),
                                rsmd.getColumnLabel(i));
                    } else {
                        this.columnMap.put(SQLConverter.preEscapingIdentifier(rsmd.getColumnLabel(i)),
                                rsmd.getColumnLabel(i));
                    }
                }
                c4io = new CreateTableCommand(tn, execId, this.columnMap);
            } catch (Exception ignore) {
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
            enlistDropTable(sql, ddlType);
            break;
        case ALTER_RENAME:
            enlistAlterRename(sql, ddlType);
            break;
        case ADD_COLUMN:
            enlistAddColumn(sql, ddlType);
            break;
        case CREATE_INDEX:
            enlistCreateIndex(sql, ddlType);
            break;
        case CREATE_PRIMARY_KEY:
            enlistCreatePrimaryKey(sql, ddlType);
            break;
        case CREATE_FOREIGN_KEY:
            enlistCreateForeignKey(sql, ddlType);
            break;
        case DROP_FOREIGN_KEY:
            enlistDropForeignKey(sql, ddlType);
            break;
        default:
            break;
        }
    }

    private void enlistCreateForeignKey(String sql, DDLType ddlType) throws SQLException {
        String tableName = ddlType.getDBObjectName();
        String relationshipName = ddlType.getSecondDBObjectName();
        String referencedTable = ddlType.getThirdDBObjectName();
        String execId = UcanaccessConnection.getCtxExcId();
        CreateForeignKeyCommand c4io =
                new CreateForeignKeyCommand(tableName, referencedTable, execId, relationshipName);
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void enlistDropForeignKey(String sql, DDLType ddlType) throws SQLException {
        String relationshipName = ddlType.getSecondDBObjectName();
        String execId = UcanaccessConnection.getCtxExcId();
        DropForeignKeyCommand c4io = new DropForeignKeyCommand(execId, relationshipName);
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void enlistCreatePrimaryKey(String sql, DDLType ddlType) throws SQLException {
        String tableName = ddlType.getDBObjectName();
        String execId = UcanaccessConnection.getCtxExcId();
        CreatePrimaryKeyCommand c4io = new CreatePrimaryKeyCommand(tableName, execId);
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void enlistCreateIndex(String sql, DDLType ddlType) throws SQLException {
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
        List<String> typeList = new ArrayList<String>();
        List<String> defaultList = new ArrayList<String>();
        this.columnMap = new HashMap<String, String>();
        List<Boolean> notNullList = new ArrayList<Boolean>();
        String tknt = columnName + columnDefinition;
        this.parseColumnTypes(typeList, defaultList, notNullList, tknt);
        check4OutOfPlacedNotNull(sql);
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        AddColumnCommand c4io = new AddColumnCommand(tableName, columnName, execId, this.columnMap, this.types,
                this.defaults, this.notNulls);
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void check4OutOfPlacedNotNull(String sql) {
        if (Pattern.compile(SQLConverter.NOT_NULL).matcher(sql).find()) {
            if (this.notNulls.length > 0 && (this.notNulls[0] == null || !this.notNulls[0])) {
                this.notNulls[0] = true;
            }
        }

    }

    private void enlistDropTable(String sql, DDLType ddlType) throws SQLException {
        String tn = ddlType.getDBObjectName();
        String execId = UcanaccessConnection.getCtxExcId();
        UcanaccessConnection ac = UcanaccessConnection.getCtxConnection();
        DropTableCommand c4io = new DropTableCommand(tn, execId);
        ac.add(c4io);
        if (!ac.getAutoCommit()) {
            ac.commit();
        }
    }

    private void enlistAlterRename(String sql, DDLType ddlType) throws SQLException {
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

    private String[] checkEscaped(String ll, String rl, String[] colDecls, String tknt) {
        if (colDecls[0].startsWith(ll) && tknt.substring(1).indexOf(rl) > 0) {
            for (int k = 0; k < colDecls.length; k++) {
                if (colDecls[k].endsWith(rl)) {
                    String[] colDecls0 = new String[colDecls.length - k];
                    colDecls0[0] = tknt.substring(1, tknt.substring(1).indexOf(rl) + 1);
                    for (int y = 1; y < colDecls0.length; y++) {
                        colDecls0[y] = colDecls[y + k];
                    }
                    colDecls = colDecls0;
                    break;
                }
            }
        }
        return colDecls;
    }

    private void parseColumnTypes(List<String> typeList, List<String> defaultList,
            List<Boolean> notNullList, String tknt) {

        String[] colDecls = tknt.split("[\\s\n\r]+");
        colDecls = checkEscaped("[", "]", colDecls, tknt);
        colDecls = checkEscaped("`", "`", colDecls, tknt);
        String escaped = (SQLConverter.isListedAsKeyword(colDecls[0].toUpperCase())) ? colDecls[0].toUpperCase()
                : SQLConverter.basicEscapingIdentifier(colDecls[0]);
        columnMap.put(escaped, colDecls[0]);

        boolean reset = false;
        if (tknt.matches("[\\s\n\r]*\\d+[\\s\n\r]*\\).*")) {
            reset = true;
            tknt = tknt.substring(tknt.indexOf(")") + 1).trim();
            colDecls = tknt.split("[\\s\n\r]+");
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
            typeList.add(colDecls[1]);

        }

        if ((colDecls.length > 2 || (reset & colDecls.length == 2))
                && "not".equalsIgnoreCase(colDecls[colDecls.length - 2])
                && "null".equalsIgnoreCase(colDecls[colDecls.length - 1])) {
            notNullList.add(true);
        } else if (!decDef) {
            notNullList.add(false);
        }

        if (!decDef) {
            defaultList.add(value(SQLConverter.getDDLDefault(tknt)));
        }

        this.types = typeList.toArray(new String[typeList.size()]);
        this.defaults = defaultList.toArray(new String[defaultList.size()]);
        this.notNulls = notNullList.toArray(new Boolean[notNullList.size()]);
    }

    // getting AUTOINCREMENT and GUID
    private void parseTypesFromCreateStatement(String sql) throws SQLException {
        sql = sql.replaceAll("([\\s\n\r]+)((?i)DECIMAL)([\\s\n\r]*\\()", "$1NUMERIC(")
                .replaceAll("([\\s\n\r]+)((?i)NUMERIC)([\\s\n\r]*\\()", "$1NUMERIC(");
        int startDecl = sql.indexOf('(');
        int endDecl = sql.lastIndexOf(')');

        if (startDecl >= endDecl) {
            throw new UcanaccessSQLException(ExceptionMessages.INVALID_CREATE_STATEMENT);
        }
        String decl = sql.substring(startDecl + 1, endDecl);
        String[] tokens = decl.split(",");

        List<String> typeList = new ArrayList<String>();
        List<String> defaultList = new ArrayList<String>();
        this.columnMap = new HashMap<String, String>();
        List<Boolean> notNullList = new ArrayList<Boolean>();
        for (int j = 0; j < tokens.length; ++j) {
            String tknt = tokens[j].trim();
            parseColumnTypes(typeList, defaultList, notNullList, tknt);

        }
    }

    private String value(String value) {
        if (value == null) {
            return null;
        }
        if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1).replaceAll("\"\"", "\"");
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            return (value.substring(1, value.length() - 1).replaceAll("''", "'"));
        }
        return value;
    }

}
