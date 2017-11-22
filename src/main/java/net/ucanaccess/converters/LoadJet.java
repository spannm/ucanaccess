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
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.PropertyMap.Property;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.ColumnImpl.AutoNumberGenerator;
import com.healthmarketscience.jackcess.impl.IndexData;
import com.healthmarketscience.jackcess.impl.IndexImpl;
import com.healthmarketscience.jackcess.impl.query.QueryFormat;
import com.healthmarketscience.jackcess.impl.query.QueryImpl;
import com.healthmarketscience.jackcess.query.Query;

import org.hsqldb.error.ErrorCode;

import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.ext.FunctionType;
import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.util.Logger;
import net.ucanaccess.util.Logger.Messages;

public class LoadJet {
    private static int namingCounter = 0;

    private final class FunctionsLoader {
        private Set<String> functionsDefinition = new HashSet<String>();

        private void addAggregates() {
            functionsDefinition.add(getAggregate("LONGVARCHAR", "last"));
            functionsDefinition.add(getAggregate("DECIMAL(100,10)", "last"));
            functionsDefinition.add(getAggregate("BOOLEAN", "last"));
            functionsDefinition.add(getAggregate("LONGVARCHAR", "first"));
            functionsDefinition.add(getAggregate("DECIMAL(100,10)", "first"));
            functionsDefinition.add(getAggregate("BOOLEAN", "first"));
            functionsDefinition.add(getLastTimestamp());
            functionsDefinition.add(getFirstTimestamp());

        }

        private String getLastTimestamp() {
            return "CREATE AGGREGATE FUNCTION last(IN val TIMESTAMP, IN flag boolean, INOUT ts TIMESTAMP, INOUT counter INT) "
                    + "RETURNS TIMESTAMP " + "CONTAINS SQL " + "BEGIN ATOMIC " + "IF flag THEN " + "RETURN ts; "
                    + "ELSE " + "IF counter IS NULL THEN SET counter = 0; END IF; " + "SET counter = counter + 1; "
                    + "SET ts = val;" + "RETURN NULL; " + "END IF; " + "END ";
        }

        private String getFirstTimestamp() {
            return "CREATE AGGREGATE FUNCTION First(IN val TIMESTAMP, IN flag boolean, INOUT ts TIMESTAMP , INOUT counter INT) "
                    + "RETURNS TIMESTAMP " + "CONTAINS SQL " + "BEGIN ATOMIC " + "IF flag THEN " + "RETURN ts; "
                    + "ELSE " + "IF counter IS NULL THEN SET counter = 0; END IF; " + "SET counter = counter + 1; "
                    + " IF counter = 1 THEN  " + " SET ts = val; END IF; " + "RETURN NULL; " + "END IF; " + "END ";
        }

        private void addFunction(String functionName, String methodName, String returnType, String... parTypes) {
            StringBuffer funDef = new StringBuffer();
            if (DBReference.is2xx()) {
                funDef.append("CREATE FUNCTION ").append(functionName).append("(");
                String comma = "";
                for (int i = 0; i < parTypes.length; i++) {
                    funDef.append(comma).append("par").append(i).append(" ").append(parTypes[i]);
                    comma = ",";
                }
                funDef.append(")");
                funDef.append(" RETURNS ");
                funDef.append(returnType);
                funDef.append("  LANGUAGE JAVA DETERMINISTIC NO SQL  EXTERNAL NAME 'CLASSPATH:");
                funDef.append(methodName).append("'");
            } else {
                funDef.append("CREATE ALIAS ").append(functionName).append(" FOR \"").append(methodName).append("\"");
            }
            functionsDefinition.add(funDef.toString());
        }

        private void addFunctions(Class<?> clazz, boolean cswitch) throws SQLException {
            Method[] mths = clazz.getDeclaredMethods();
            Map<String, String> tmap = TypesMap.getAccess2HsqlTypesMap();
            for (Method mth : mths) {
                Annotation[] ants = mth.getAnnotations();
                for (Annotation ant : ants) {
                    if (ant.annotationType().equals(FunctionType.class)) {
                        FunctionType ft = (FunctionType) ant;
                        String methodName = clazz.getName() + "." + mth.getName();
                        String functionName = ft.functionName();
                        if (functionName == null) {
                            functionName = methodName;
                        }
                        AccessType[] acts = ft.argumentTypes();
                        AccessType ret = ft.returnType();
                        String retTypeName = ret.name();
                        String returnType = tmap.containsKey(retTypeName) ? tmap.get(retTypeName) : retTypeName;
                        if (AccessType.TEXT.equals(ret)) {
                            returnType += "(255)";
                        }
                        String[] args = new String[acts.length];
                        for (int i = 0; i < args.length; i++) {
                            String typeName = acts[i].name();
                            args[i] = tmap.containsKey(typeName) ? tmap.get(typeName) : typeName;
                            if (AccessType.TEXT.equals(acts[i])) {
                                args[i] += "(255)";
                            }
                        }
                        if (ft.namingConflict()) {
                            SQLConverter.addWAFunctionName(functionName);
                            functionName += "WA";
                        }
                        addFunction(functionName, methodName, returnType, args);
                    }
                }
            }
            createFunctions();
            if (cswitch) {
                createSwitch();
            }
        }

        private void resetDefault() throws SQLException {
            Class<?> clazz = Functions.class;
            Method[] mths = clazz.getDeclaredMethods();
            for (Method mth : mths) {
                Annotation[] ants = mth.getAnnotations();
                for (Annotation ant : ants) {
                    if (ant.annotationType().equals(FunctionType.class)) {
                        FunctionType ft = (FunctionType) ant;
                        String functionName = ft.functionName();

                        if (ft.namingConflict()) {
                            SQLConverter.addWAFunctionName(functionName);
                        }

                    }
                }
            }

        }

        private void createFunctions() throws SQLException {
            for (String functionDef : functionsDefinition) {

                try {
                    exec(functionDef, true);
                } catch (SQLException e) {
                    e.printStackTrace();
                    Logger.logParametricWarning(Messages.FUNCTION_ALREADY_ADDED, functionDef);
                }
            }

            functionsDefinition.clear();
        }

        private void createSwitch() throws SQLException {
            DataType[] dtypes = new DataType[] { DataType.BINARY, DataType.BOOLEAN, DataType.SHORT_DATE_TIME,
                    DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.MONEY, DataType.NUMERIC,
                    DataType.COMPLEX_TYPE, DataType.MEMO };
            for (DataType dtype : dtypes) {
                String type = " " + TypesMap.map2hsqldb(dtype) + " ";

                for (int i = 1; i < 10; i++) {
                    StringBuffer header = new StringBuffer("CREATE FUNCTION SWITCH(  ");
                    StringBuffer body = new StringBuffer("(CASE ");
                    String comma = "";
                    for (int j = 0; j < i; j++) {
                        body.append("  WHEN B").append(j).append(" THEN V").append(j);
                        header.append(comma).append("B").append(j).append(" BOOLEAN ,").append("V").append(j)
                                .append(type);
                        comma = ",";
                    }
                    body.append(" END)");
                    header.append(") RETURNS").append(type).append(" RETURN").append(body);
                    try {
                        exec(header.toString(), true);
                    } catch (SQLException e) {
                        Logger.logParametricWarning(Messages.FUNCTION_ALREADY_ADDED, header.toString());
                    }
                }
            }

        }

        private String getAggregate(String type, String fun) {
            String createLast =
                    "CREATE AGGREGATE FUNCTION " + fun + "(IN val " + type + ", IN flag BOOLEAN, INOUT register  "
                            + type + ", INOUT counter INT) " + "  RETURNS  " + type + "  NO SQL  LANGUAGE JAVA "
                            + "  EXTERNAL NAME 'CLASSPATH:net.ucanaccess.converters.FunctionsAggregate." + fun + "'";
            return createLast;
        }

        private void loadMappedFunctions() throws SQLException {
            addFunctions(Functions.class, true);
            addAggregates();
            createFunctions();
        }
    }

    private final class LogsFlusher {
        private void dumpList(List<String> logs) {
            dumpList(logs, false);
        }

        private void dumpList(List<String> logs, boolean cr) {
            String comma = "";
            StringBuffer sb = new StringBuffer();
            String crs = cr ? System.getProperty("line.separator") : "";
            for (String log : logs) {
                sb.append(comma).append(log).append(crs);
                comma = ", ";
            }
            Logger.log(sb.toString());
            logs.clear();
        }
    }

    private final class TablesLoader {
        private static final int    HSQL_FK_ALREADY_EXISTS   = -ErrorCode.X_42528;      // -5528;
        private static final int    HSQL_UK_ALREADY_EXISTS   = -ErrorCode.X_42522;      // -5522
        private static final int    HSQL_NOT_NULL            = -ErrorCode.X_23502;
        private static final int    HSQL_FK_VIOLATION        = -ErrorCode.X_23503;
        private static final int    HSQL_UK_VIOLATION        = -ErrorCode.X_23505;
        private static final String SYSTEM_SCHEMA            = "SYS";
        private List<String>        unresolvedTables         = new ArrayList<String>();
        private List<String>        calculatedFieldsTriggers = new ArrayList<String>();
        private LinkedList<String>  loadingOrder             = new LinkedList<String>();
        private Set<Column>         alreadyIndexed           = new HashSet<Column>();
        private Set<String>         readOnlyTables           = new HashSet<String>();

        private String commaSeparated(List<? extends Index.Column> columns, boolean escape) throws SQLException {
            String comma = "";
            StringBuffer sb = new StringBuffer(" (");
            for (Index.Column cd : columns) {
                String cl = escape ? escapeIdentifier(cd.getColumn().getName()) : cd.getColumn().getName();
                sb.append(comma).append(cl);
                comma = ",";
            }
            return sb.append(") ").toString();
        }

        private String schema(String name, boolean systemTable) {
            if (systemTable) {
                return SYSTEM_SCHEMA + "." + name;
            }
            return name;
        }

        private DataType getReturnType(Column cl) throws IOException {
            if (cl.getProperties().get(PropertyMap.EXPRESSION_PROP) == null
                    || cl.getProperties().get(PropertyMap.RESULT_TYPE_PROP) == null) {
                return null;
            }
            byte pos = (Byte) cl.getProperties().get(PropertyMap.RESULT_TYPE_PROP).getValue();
            return DataType.fromByte(pos);

        }

        private String getHsqldbColumnType(Column cl) throws IOException {
            String htype;
            DataType dtyp = cl.getType();
            DataType rtyp = getReturnType(cl);
            boolean calcType = false;
            if (rtyp != null) {
                dtyp = rtyp;
                calcType = true;
            }

            if (dtyp.equals(DataType.TEXT)) {
                int ln = ff1997 ? cl.getLength() : cl.getLengthInUnits();
                htype = "VARCHAR(" + ln + ")";
            } else if (dtyp.equals(DataType.NUMERIC) && (cl.getScale() > 0 || calcType)) {
                if (calcType) {
                    htype = "NUMERIC(100 ,4)";
                } else {
                    htype = "NUMERIC(" + (cl.getPrecision() > 0 ? cl.getPrecision() : 100) + "," + cl.getScale() + ")";
                }
            } else if (dtyp.equals(DataType.FLOAT) && calcType) {
                htype = "NUMERIC(" + (cl.getPrecision() > 0 ? cl.getPrecision() : 100) + "," + 4 + ")";
            } else {
                htype = TypesMap.map2hsqldb(dtyp);
            }
            return htype;
        }

        private String getCalculatedFieldTrigger(String ntn, Column cl, boolean isCreate)
                throws IOException, SQLException {
            DataType dt = getReturnType(cl);
            String fun = null;
            if (isNumeric(dt)) {
                fun = "formulaToNumeric";
            } else if (isBoolean(dt)) {
                fun = "formulaToBoolean";
            } else if (isDate(dt)) {
                fun = "formulaToDate";
            } else if (isTextual(dt)) {
                fun = "formulaToText";
            }
            String call = fun == null ? "%s" : fun + "(%s,'" + dt.name() + "')";
            String ecl = procedureEscapingIdentifier(cl.getName()).replace("%", "%%");
            String trg = isCreate
                    ? "CREATE TRIGGER expr%d before insert ON " + ntn + " REFERENCING NEW  AS newrow  FOR EACH ROW "
                            + " BEGIN  ATOMIC " + " SET newrow." + ecl + " = " + call + "; END "
                    : "CREATE TRIGGER expr%d before update ON " + ntn
                            + " REFERENCING NEW  AS newrow OLD AS OLDROW FOR EACH ROW " + " BEGIN  ATOMIC IF %s THEN "
                            + " SET newrow." + ecl + " = " + call + "; ELSEIF newrow." + ecl + " <> oldrow." + ecl
                            + " THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '"
                            + Logger.getMessage(Messages.TRIGGER_UPDATE_CF_ERR.name()) + cl.getName().replace("%", "%%")
                            + "'" + ";  END IF ; END ";

            return trg;
        }

        private boolean isNumeric(DataType dt) {
            return typeGroup(dt, DataType.NUMERIC, DataType.MONEY, DataType.DOUBLE, DataType.FLOAT, DataType.LONG,
                    DataType.INT, DataType.BYTE);
        }

        private boolean isDate(DataType dt) {
            return typeGroup(dt, DataType.SHORT_DATE_TIME);
        }

        private boolean isBoolean(DataType dt) {
            return typeGroup(dt, DataType.BOOLEAN);
        }

        private boolean isTextual(DataType dt) {
            return typeGroup(dt, DataType.MEMO, DataType.TEXT);
        }

        private boolean typeGroup(DataType dt, DataType... gr) {
            for (DataType el : gr) {
                if (el.equals(dt)) {
                    return true;
                }
            }
            return false;
        }

        private void createSyncrTable(Table t, boolean systemTable) throws SQLException, IOException {
            createSyncrTable(t, systemTable, true);
        }

        private void createSyncrTable(Table t, boolean systemTable, boolean constraints)
                throws SQLException, IOException {
            String tn = t.getName();
            if (tn.equalsIgnoreCase("DUAL")) {
                SQLConverter.setDualUsedAsTableName(true);
            }

            String ntn = SQLConverter.preEscapingIdentifier(tn);

            int seq = metadata.newTable(tn, ntn, Metadata.Types.TABLE);
            ntn = SQLConverter.completeEscaping(ntn);
            ntn = SQLConverter.checkLang(ntn, conn);
            ntn = schema(ntn, systemTable);

            StringBuffer sbC = new StringBuffer("CREATE  CACHED TABLE ").append(ntn).append("(");

            List<? extends Column> lc = t.getColumns();
            String comma = "";
            for (Column cl : lc) {
                if ("USER".equalsIgnoreCase(cl.getName())) {
                    Logger.logParametricWarning(Messages.USER_AS_COLUMNNAME, t.getName());
                }
                String expr = getExpression(cl);
                if (expr != null && constraints) {
                    String tgrI = getCalculatedFieldTrigger(ntn, cl, true);
                    String tgrU = getCalculatedFieldTrigger(ntn, cl, false);
                    calculatedFieldsTriggers
                            .add(String.format(tgrI, namingCounter++, SQLConverter.convertFormula(expr)));
                    String uc = getUpdateConditions(cl);
                    if (uc.length() > 0) {
                        calculatedFieldsTriggers
                                .add(String.format(tgrU, namingCounter++, uc, SQLConverter.convertFormula(expr)));
                    }

                }

                String htype = getHsqldbColumnType(cl);
                String cn = SQLConverter.preEscapingIdentifier(cl.getName());
                String ctype = cl.getType().name();
                if (cl.isAutoNumber()) {
                    ColumnImpl cli = (ColumnImpl) cl;
                    AutoNumberGenerator ang = cli.getAutoNumberGenerator();
                    if (ang.getType().equals(DataType.LONG)) {
                        ctype = "COUNTER";
                    }

                } else if (cl.isHyperlink()) {
                    ctype = "HYPERLINK";
                }
                metadata.newColumn(cl.getName(), cn, ctype, seq);
                if (expr != null && constraints) {
                    metadata.calculatedField(t.getName(), cl.getName());
                }
                cn = SQLConverter.completeEscaping(cn);
                cn = SQLConverter.checkLang(cn, conn);
                sbC.append(comma).append(cn).append(" ").append(htype);

                PropertyMap pm = cl.getProperties();
                Object required = pm.getValue(PropertyMap.REQUIRED_PROP);
                if (constraints && required != null && required instanceof Boolean && ((Boolean) required)) {
                    sbC.append(" NOT NULL ");
                }
                comma = ",";
            }
            sbC.append(")");
            exec(sbC.toString(), true);

        }

        private String getExpression(Column cl) throws IOException {
            PropertyMap map = cl.getProperties();
            Property exprp = map.get(PropertyMap.EXPRESSION_PROP);

            if (exprp != null) {
                Table tl = cl.getTable();
                String expr = SQLConverter.convertPowOperator((String) exprp.getValue());
                for (Column cl1 : tl.getColumns()) {
                    expr = expr.replaceAll("\\[(?i)(" + Pattern.quote(cl1.getName()) + ")\\]", "newrow.$0");
                }
                return expr;
            }
            return null;
        }

        private String getUpdateConditions(Column cl) throws IOException, SQLException {
            PropertyMap map = cl.getProperties();
            Property exprp = map.get(PropertyMap.EXPRESSION_PROP);

            if (exprp != null) {
                Set<String> setu = SQLConverter.getFormulaDependencies(exprp.getValue().toString());

                if (setu.size() > 0) {
                    String or = "";
                    StringBuffer cw = new StringBuffer();
                    for (String dep : setu) {
                        dep = escapeIdentifier(dep);
                        cw.append(or).append("oldrow.").append(dep).append("<>").append("newrow.").append(dep);
                        or = " OR ";
                    }

                    return cw.toString();
                }

            }
            return " FALSE ";
        }

        private String procedureEscapingIdentifier(String name) throws SQLException {
            return SQLConverter.procedureEscapingIdentifier(escapeIdentifier(name));
        }

        private void setDefaultValue(Column cl) throws SQLException, IOException {
            String tn = cl.getTable().getName();
            String ntn = escapeIdentifier(tn);
            List<String> arTrigger = new ArrayList<String>();
            setDefaultValue(cl, ntn, arTrigger);
            for (String trigger : arTrigger) {
                exec(trigger, true);
            }
        }

        private String defaultValue4SQL(Object defaulT, DataType dt) throws SQLException, IOException {
            if (defaulT == null) {
                return null;
            }
            String default4SQL = SQLConverter.convertSQL(" " + defaulT.toString()).getSql();
            if (default4SQL.trim().startsWith("=")) {
                default4SQL = default4SQL.trim().substring(1);
            }
            if (dt.equals(DataType.BOOLEAN)
                    && ("=yes".equalsIgnoreCase(default4SQL) || "yes".equalsIgnoreCase(default4SQL))) {
                default4SQL = "true";
            }
            if (dt.equals(DataType.BOOLEAN)
                    && ("=no".equalsIgnoreCase(default4SQL) || "no".equalsIgnoreCase(default4SQL))) {
                default4SQL = "false";
            }
            if ((dt.equals(DataType.MEMO) || dt.equals(DataType.TEXT))
                    && (!defaulT.toString().startsWith("\"") || !defaulT.toString().endsWith("\""))

            ) {
                default4SQL = "'" + default4SQL.replaceAll("'", "''") + "'";
            }
            return default4SQL;
        }

        private void setDefaultValue(Column cl, String ntn, List<String> arTrigger) throws IOException, SQLException {
            PropertyMap pm = cl.getProperties();
            String ncn = procedureEscapingIdentifier(cl.getName());
            Object defaulT = pm.getValue(PropertyMap.DEFAULT_VALUE_PROP);
            if (defaulT != null) {
                String default4SQL = defaultValue4SQL(defaulT, cl.getType());
                String guidExp = "GenGUID()";
                if (!guidExp.equals(defaulT)) {
                    boolean defaultIsFunction =
                            defaulT.toString().trim().endsWith(")") && defaulT.toString().indexOf("(") > 0;
                    if (defaultIsFunction) {
                        metadata.columnDef(cl.getTable().getName(), cl.getName(), defaulT.toString());
                    }
                    Object defFound = default4SQL;
                    boolean isNull = (default4SQL + "").equalsIgnoreCase("null");
                    if (!isNull && (defFound = tryDefault(default4SQL)) == null) {

                        Logger.logParametricWarning(Messages.UNKNOWN_EXPRESSION, "" + defaulT, cl.getName(),
                                cl.getTable().getName());
                    } else {
                        if (defFound != null && !defaultIsFunction) {
                            metadata.columnDef(cl.getTable().getName(), cl.getName(), defFound.toString());
                        }
                        if (cl.getType() == DataType.TEXT && defaulT.toString().startsWith("'")
                                && defaulT.toString().endsWith("'")
                                && defaulT.toString().length() > cl.getLengthInUnits()) {
                            Logger.logParametricWarning(Messages.DEFAULT_VALUES_DELIMETERS, "" + defaulT, cl.getName(),
                                    cl.getTable().getName(), "" + cl.getLengthInUnits());
                        }
                        arTrigger.add("CREATE TRIGGER DEFAULT_TRIGGER" + (namingCounter++) + " BEFORE INSERT ON " + ntn
                                + "  REFERENCING NEW ROW AS NEW FOR EACH ROW IF NEW." + ncn + " IS NULL THEN "
                                + "SET NEW." + ncn + "= " + default4SQL + " ; END IF");

                    }
                }
            }

        }

        private void setDefaultValues(Table t) throws SQLException, IOException {
            String tn = t.getName();
            String ntn = escapeIdentifier(tn);
            List<? extends Column> lc = t.getColumns();
            List<String> arTrigger = new ArrayList<String>();
            for (Column cl : lc) {
                setDefaultValue(cl, ntn, arTrigger);
            }
            for (String trigger : arTrigger) {
                exec(trigger, true);
            }
        }

        private int countFKs() throws IOException {
            int i = 0;
            for (String tn : this.loadingOrder) {
                UcanaccessTable table = new UcanaccessTable(dbIO.getTable(tn), tn);
                if (!this.unresolvedTables.contains(tn)) {
                    for (Index idxi : table.getIndexes()) {
                        // riw
                        IndexImpl idx = (IndexImpl) idxi;
                        if (idx.isForeignKey() && !idx.getReference().isPrimaryTable()) {
                            i++;
                        }
                    }
                }
            }
            return i;
        }

        private boolean reorder() throws IOException, SQLException {
            int maxIteration = countFKs() + 1;

            for (int i = 0; i < maxIteration; i++) {
                boolean change = false;
                List<String> loadingOrder0 = new ArrayList<String>();
                loadingOrder0.addAll(this.loadingOrder);
                for (String tn : loadingOrder0) {
                    UcanaccessTable table = new UcanaccessTable(dbIO.getTable(tn), tn);
                    if (!this.unresolvedTables.contains(tn)) {
                        for (Index idxi : table.getIndexes()) {
                            // riw
                            IndexImpl idx = (IndexImpl) idxi;
                            if (idx.isForeignKey() && !idx.getReference().isPrimaryTable() && !tryReorder(idx)) {
                                change = true;
                            }
                        }
                    }
                }

                if (!change) {
                    return true;
                }
            }

            return false;
        }

        private boolean tryReorder(Index idxi) throws IOException {
            IndexImpl idx = (IndexImpl) idxi;
            String ctn = idx.getTable().getName();
            String rtn = idx.getReferencedIndex().getTable().getName();
            int ict = this.loadingOrder.indexOf(ctn);
            int irt = this.loadingOrder.indexOf(rtn);
            if (ict < irt) {
                this.loadingOrder.remove(ctn);
                this.loadingOrder.add(irt, ctn);
                return false;
            }
            return true;
        }

        private void loadForeignKey(Index idxi, String ctn) throws IOException, SQLException {
            IndexImpl idx = (IndexImpl) idxi;
            String rtn = idx.getReferencedIndex().getTable().getName();
            List<IndexData.ColumnDescriptor> cls = idx.getColumns();
            if (cls.size() == 1) {
                this.alreadyIndexed.add(cls.get(0).getColumn());
            }
            String ntn = escapeIdentifier(ctn);
            if (ntn == null) {
                return;
            }
            String nin = escapeIdentifier(ctn + "_" + idx.getName());
            String colsIdx = commaSeparated(cls, true);
            String colsIdxRef = commaSeparated(idx.getReferencedIndex().getColumns(), true);

            StringBuffer ci = new StringBuffer("ALTER TABLE ").append(ntn);
            ci.append(" ADD CONSTRAINT ").append(nin);
            String nrt = escapeIdentifier(rtn);

            if (nrt == null) {
                return;
            }
            ci.append(" FOREIGN KEY ").append(colsIdx).append(" REFERENCES ").append(nrt).append(colsIdxRef);

            if (idx.getReference().isCascadeDeletes()) {
                ci.append(" ON DELETE CASCADE ");
            }
            if (idx.getReference().isCascadeUpdates()) {
                ci.append(" ON UPDATE CASCADE ");
            }
            try {
                exec(ci.toString(), true);
            } catch (SQLException e) {
                if (e.getErrorCode() == HSQL_FK_ALREADY_EXISTS) {
                    Logger.log(e.getMessage());
                } else {
                    throw e;
                }
            }
            loadedIndexes.add("FK on " + ntn + " Columns:" + commaSeparated(cls, false) + " References " + nrt
                    + " Columns:" + commaSeparated(idx.getReferencedIndex().getColumns(), false));
        }

        private void loadIndex(Index idx, String tn) throws IOException, SQLException {
            String ntn = escapeIdentifier(tn);
            if (ntn == null) {
                return;
            }
            String nin = idx.getName();
            nin = escapeIdentifier(tn + "_" + nin);
            boolean uk = idx.isUnique();
            boolean pk = idx.isPrimaryKey();
            if (!uk && !pk && idx.getColumns().size() == 1) {
                Column cl = idx.getColumns().get(0).getColumn();
                if (this.alreadyIndexed.contains(cl)) {
                    return;
                }
            }
            if (uk && idx.getColumns().size() == 1) {
                Column cl = idx.getColumns().get(0).getColumn();
                DataType dt = cl.getType();
                if (dt.equals(DataType.COMPLEX_TYPE)) {
                    return;
                }
            }

            StringBuffer ci = new StringBuffer("ALTER TABLE ").append(ntn);
            String colsIdx = commaSeparated(idx.getColumns(), true);
            if (pk) {
                ci.append(" ADD PRIMARY KEY ").append(colsIdx);
            } else if (uk) {
                ci.append(" ADD CONSTRAINT ").append(nin);
                ci.append(" UNIQUE ").append(colsIdx);

            } else {
                ci = new StringBuffer("CREATE INDEX ").append(nin).append(" ON ").append(ntn).append(colsIdx);
            }
            try {
                exec(ci.toString(), true);
            } catch (SQLException e) {
                if (HSQL_UK_ALREADY_EXISTS == e.getErrorCode()) {
                    return;
                }
                if (idx.isUnique()) {
                    for (Index.Column cd : idx.getColumns()) {
                        if (cd.getColumn().getType().equals(DataType.COMPLEX_TYPE)) {
                            return;
                        }
                    }
                }
                Logger.logWarning(e.getMessage());
                return;
            } catch (Exception e) {

                Logger.logWarning(e.getMessage());
                return;
            }
            String pre = pk ? "Primary Key " : uk ? "Index Unique " : "Index";
            loadedIndexes.add(pre + " on " + tn + " Columns:" + commaSeparated(idx.getColumns(), false));

        }

        private void createTable(Table t) throws SQLException, IOException {
            createTable(t, false);
        }

        private void dropTable(Table t, boolean systemTable) throws SQLException {
            String tn = t.getName();

            String ntn = schema(escapeIdentifier(tn), systemTable);
            exec("DROP TABLE " + ntn + " CASCADE ", false);
            metadata.dropTable(tn);
        }

        private void makeTableReadOnly(Table t, boolean systemTable) throws SQLException {
            String tn = t.getName();
            this.readOnlyTables.add(t.getName());
            String ntn = schema(escapeIdentifier(tn), systemTable);
            exec("SET TABLE " + ntn + " READONLY TRUE ", false);
        }

        private void recreate(Table t, boolean systemTable, Row record, int errorCode)
                throws SQLException, IOException {
            String type = "";
            switch (errorCode) {
            case HSQL_FK_VIOLATION:
                type = "Foreign Key";
                break;
            case HSQL_NOT_NULL:
                type = "Not Null";
                break;
            case HSQL_UK_VIOLATION:
                type = "Unique";
                break;
            default:
                break;
            }
            Logger.logParametricWarning(Messages.CONSTRAINT, type, t.getName(), record.toString(), t.getName());

            dropTable(t, systemTable);
            createSyncrTable(t, systemTable, false);
            if (errorCode != HSQL_FK_VIOLATION) {
                loadTableFKs(t.getName(), false);
            }
            loadTableData(t, systemTable);
            makeTableReadOnly(t, systemTable);

        }

        private void createTable(Table t, boolean systemTable) throws SQLException, IOException {
            String tn = t.getName();
            if (tn.indexOf(" ") > 0) {
                SQLConverter.addWhiteSpacedTableNames(tn);
            }
            String ntn = SQLConverter.escapeIdentifier(tn);// clean
            if (ntn == null) {
                return;
            }
            createSyncrTable(t, systemTable);
        }

        private boolean hasAppendOnly(Table t) {

            for (Column c : t.getColumns()) {
                if (c.isAppendOnly()) {
                    return true;
                }
            }
            return false;
        }

        private void loadTableData(Table t, boolean systemTable) throws IOException, SQLException {
            PreparedStatement ps = null;
            try {
                int i = 0;
                int step = 2000;
                Iterator<Row> it = t.iterator();

                while (it.hasNext()) {
                    Row row = it.next();
                    List<Object> values = new ArrayList<Object>();
                    if (row == null) {
                        continue;
                    }
                    if (ps == null) {
                        ps = sqlInsert(t, row, systemTable);
                    }
                    for (Object obj : row.values()) {
                        values.add(value(obj));
                    }
                    execInsert(ps, values);

                    if ((i > 0 && i % step == 0) || !it.hasNext()) {
                        try {
                            ps.executeBatch();
                        } catch (SQLException e) {
                            int ec = e.getErrorCode();
                            if (ec == HSQL_NOT_NULL || ec == HSQL_FK_VIOLATION || ec == HSQL_UK_VIOLATION) {
                                if (ec == HSQL_FK_VIOLATION) {
                                    Logger.logWarning(e.getMessage());
                                }
                                recreate(t, systemTable, row, e.getErrorCode());
                            } else {
                                throw e;
                            }
                        }
                        conn.commit();
                    }
                    i++;

                }
                if (i != t.getRowCount()) {
                    Logger.logParametricWarning(Messages.ROW_COUNT, t.getName(), String.valueOf(t.getRowCount()),
                            String.valueOf(i));

                }
            } finally {
                if (ps != null) {
                    ps.close();
                }
            }
        }

        private void loadTableFKs(String tn, boolean autoref) throws IOException, SQLException {
            if (this.readOnlyTables.contains(tn)) {
                return;
            }
            Table t = dbIO.getTable(tn);
            UcanaccessTable table = new UcanaccessTable(t, tn);
            if (t != null) {
                for (Index idxi : table.getIndexes()) {
                    // riw
                    IndexImpl idx = (IndexImpl) idxi;
                    if (idx.isForeignKey() && !idx.getReference().isPrimaryTable()) {
                        boolean isAuto = idx.getTable().getName().equals(idx.getReferencedIndex().getTable().getName());
                        if ((autoref && isAuto) || (!autoref && !isAuto)) {
                            loadForeignKey(idx, tn);
                        }
                    }
                }
            }
        }

        private void createCalculatedFieldsTriggers() {
            for (String trigger : calculatedFieldsTriggers) {
                try {
                    exec(trigger, false);

                } catch (SQLException e) {
                    Logger.logWarning(e.getMessage());
                }
            }
        }

        private void loadTableIndexesUK(String tn) throws IOException, SQLException {
            Table t = dbIO.getTable(tn);
            UcanaccessTable table = new UcanaccessTable(t, tn);
            if (t != null) {
                for (Index idx : table.getIndexes()) {
                    if (!idx.isForeignKey() && (idx.isPrimaryKey() || idx.isUnique())) {
                        loadIndex(idx, tn);
                    }
                }
            }

        }

        private void loadTableIndexesNotUK(String tn) throws IOException, SQLException {
            Table t = dbIO.getTable(tn);
            UcanaccessTable table = new UcanaccessTable(t, tn);
            if (!skipIndexes && t != null) {
                for (Index idx : table.getIndexes()) {
                    if (!idx.isForeignKey() && !idx.isPrimaryKey() && !idx.isUnique()) {
                        loadIndex(idx, tn);
                    }
                }
            }

        }

        private void createTables() throws SQLException, IOException {

            metadata.createMetadata();
            for (String tn : dbIO.getTableNames()) {
                UcanaccessTable t = null;
                Table t2 = null;
                try {
                    t2 = dbIO.getTable(tn);
                    t = new UcanaccessTable(t2, tn);
                } catch (Exception e) {
                    Logger.logWarning(e.getMessage());
                    this.unresolvedTables.add(tn);
                }
                if (t2 != null && t != null && !tn.startsWith("~")) {
                    createTable(t);
                    this.loadingOrder.add(t.getName());
                }
            }
        }

        private void createIndexesUK() throws SQLException, IOException {
            for (String tn : dbIO.getTableNames()) {
                if (!this.unresolvedTables.contains(tn)) {
                    this.loadTableIndexesUK(tn);
                    conn.commit();
                }
            }
        }

        private void createIndexesNotUK() throws SQLException, IOException {
            for (String tn : dbIO.getTableNames()) {
                if (!this.unresolvedTables.contains(tn)) {
                    this.loadTableIndexesNotUK(tn);
                    conn.commit();
                }
            }
        }

        private void createFKs() throws SQLException, IOException {
            for (String tn : dbIO.getTableNames()) {
                if (!this.unresolvedTables.contains(tn)) {
                    this.loadTableFKs(tn, false);
                    conn.commit();
                }
            }

        }

        private void createAutoFKs() throws SQLException, IOException {
            for (String tn : dbIO.getTableNames()) {
                if (!this.unresolvedTables.contains(tn)) {
                    try {
                        this.loadTableFKs(tn, true);
                    } catch (SQLException e) {
                        UcanaccessTable t = new UcanaccessTable(dbIO.getTable(tn), tn);
                        makeTableReadOnly(t, false);
                    }
                    conn.commit();
                }
            }

        }

        private void loadTablesData() throws SQLException, IOException {
            for (String tn : this.loadingOrder) {
                if (!this.unresolvedTables.contains(tn)) {
                    UcanaccessTable t = new UcanaccessTable(dbIO.getTable(tn), tn);
                    this.loadTableData(t, false);
                    conn.commit();

                }
            }
        }

        private void createTriggers() throws IOException, SQLException {

            for (String tn : this.loadingOrder) {
                if (!this.unresolvedTables.contains(tn) && !this.readOnlyTables.contains(tn)) {
                    UcanaccessTable t = new UcanaccessTable(dbIO.getTable(tn), tn);
                    createSyncrTriggers(t);
                }
            }
            createCalculatedFieldsTriggers();
        }

        private void createSystemTables() throws SQLException, IOException {
            if (sysSchema) {
                createSystemSchema();
                for (String tn : dbIO.getSystemTableNames()) {
                    UcanaccessTable t = null;
                    try {
                        t = new UcanaccessTable(dbIO.getSystemTable(tn), tn);

                        if (t != null) {
                            createTable(t, true);
                            loadTableData(t, true);
                            exec("SET TABLE " + schema(SQLConverter.escapeIdentifier(t.getName()), true)
                                    + " READONLY TRUE ", false);
                            exec("GRANT SELECT  ON " + schema(SQLConverter.escapeIdentifier(t.getName()), true)
                                    + " TO PUBLIC ", false);
                        }
                    } catch (Exception ignore) {
                    }
                }
            }
        }

        private void loadTables() throws SQLException, IOException {
            createTables();
            createIndexesUK();
            boolean reorder = reorder();
            if (reorder) {
                createFKs();
            }
            createIndexesNotUK();
            loadTablesData();
            createTriggers();
            if (!reorder) {
                createFKs();
            }
            createAutoFKs();
            createSystemTables();
        }

        private void createSystemSchema() throws SQLException {
            exec("CREATE SCHEMA " + SYSTEM_SCHEMA + " AUTHORIZATION DBA", false);
        }

        private void createSyncrTriggers(Table t) throws SQLException, IOException {
            setDefaultValues(t);
            String ntn = escapeIdentifier(t.getName());
            triggersGenerator.synchronisationTriggers(ntn, hasAutoNumberColumn(t), hasAppendOnly(t));
            loadedTables.add(t.getName());
        }

        private PreparedStatement sqlInsert(Table t, Map<String, Object> row, boolean systemTable)
                throws IOException, SQLException {
            String tn = t.getName();
            String ntn = schema(escapeIdentifier(tn), systemTable);
            String comma = "";
            StringBuffer sbI = new StringBuffer(" INSERT INTO ").append(ntn).append(" (");
            StringBuffer sbE = new StringBuffer(" VALUES( ");
            Set<String> se = row.keySet();
            comma = "";
            for (String cn : se) {
                sbI.append(comma).append(escapeIdentifier(cn));
                sbE.append(comma).append(" ? ");
                comma = ",";
            }
            sbI.append(") ");
            sbE.append(")");
            sbI.append(sbE);

            return conn.prepareStatement(sbI.toString());
        }

        private Object value(Object value) throws SQLException {
            if (value == null) {
                return null;
            }
            if (value instanceof Float) {
                if (value.equals(Float.NaN)) {
                    return value;
                }
                BigDecimal bd = new BigDecimal(value.toString());
                return bd;
            }
            if (value instanceof Date && !(value instanceof Timestamp)) {
                Timestamp ts = new Timestamp(((Date) value).getTime());
                return ts;
            }
            if (value instanceof ComplexValueForeignKey) {
                try {
                    return ComplexBase.convert((ComplexValueForeignKey) value);
                } catch (IOException e) {
                    throw new UcanaccessSQLException(e);
                }
            }
            if (value instanceof Byte) {
                return SQLConverter.asUnsigned((Byte) value);
            }
            return value;
        }

    }

    private final class TriggersLoader {
        private static final String DEFAULT_TRIGGERS_PACKAGE = "net.ucanaccess.triggers";

        private void loadTrigger(String tableName, String namePrefix, String when, String className)
                throws SQLException {
            String q0 = DBReference.is2xx() ? "" : " QUEUE 0  ";
            String triggerName = namePrefix + "_" + (tableName);
            // .replaceAll(" ", "_"));
            triggerName = escapeIdentifier(triggerName);
            exec("CREATE TRIGGER " + triggerName + "  " + when + " ON " + tableName + "   FOR EACH ROW	" + q0
                    + "   CALL \"" + className + "\" ", true);
        }

        private void loadTriggerNP(String tableName, String namePrefix, String when, String className)
                throws SQLException {
            loadTrigger(tableName, namePrefix, when, DEFAULT_TRIGGERS_PACKAGE + "." + className);
        }

        private void synchronisationTriggers(String tableName, boolean hasAutoNumberColumn, boolean hasAutoAppendOnly)
                throws SQLException {
            loadTriggerNP(tableName, "genericInsert", "AFTER INSERT", "TriggerInsert");
            loadTriggerNP(tableName, "genericUpdate", "AFTER UPDATE", "TriggerUpdate");
            loadTriggerNP(tableName, "genericDelete", "AFTER DELETE", "TriggerDelete");
            if (hasAutoAppendOnly) {
                loadTriggerNP(tableName, "appendOnly", "BEFORE INSERT", "TriggerAppendOnly");
                loadTriggerNP(tableName, "appendOnly_upd", "BEFORE UPDATE", "TriggerAppendOnly");
            }
            if (hasAutoNumberColumn) {
                loadTriggerNP(tableName, "autonumber", "BEFORE INSERT", "TriggerAutoNumber");
                loadTriggerNP(tableName, "autonumber_validate", "BEFORE UPDATE", "TriggerAutoNumber");
            }
        }
    }

    private final class ViewsLoader {
        private Map<String, String> notLoaded             = new HashMap<String, String>();
        private Map<String, String> notLoadedProcedure    = new HashMap<String, String>();
        private static final int    OBJECT_ALREADY_EXISTS = -ErrorCode.X_42504;
        private static final int    OBJECT_NOT_FOUND      = -ErrorCode.X_42501;
        private static final int    UNEXPECTED_TOKEN      = -ErrorCode.X_42581;

        private boolean loadView(Query q) throws SQLException {
            return loadView(q, null);
        }

        private void registerQueryColumns(Query q, int seq) throws SQLException {
            QueryImpl qi = (QueryImpl) q;
            for (QueryImpl.Row row : qi.getRows()) {

                if (QueryFormat.COLUMN_ATTRIBUTE.equals(row.attribute)) {
                    String name = row.name1;

                    if (name == null) {
                        int beginIndex = Math.max(row.expression.lastIndexOf('['), row.expression.lastIndexOf('.'));

                        if (beginIndex < 0 || beginIndex == row.expression.length() - 1
                                || row.expression.endsWith(")")) {
                            continue;
                        }
                        name = row.expression.substring(beginIndex + 1);
                        if (name.endsWith("]")) {
                            name = name.substring(0, name.length() - 1);
                        }
                        if (name.contentEquals("*")) {
                            String table = row.expression.substring(0, beginIndex);
                            List<String> result = metadata.getColumnNames(table);
                            if (result != null) {
                                for (String column : result) {
                                    metadata.newColumn(column, SQLConverter.preEscapingIdentifier(column), null, seq);
                                }
                                // return;
                            }

                        }
                    }

                    metadata.newColumn(name, SQLConverter.preEscapingIdentifier(name), null, seq);

                }
            }
        }

        private boolean loadView(Query q, String queryWKT) throws SQLException {
            String qnn = SQLConverter.preEscapingIdentifier(q.getName());
            if (qnn == null) {
                return false;
            }
            int seq = metadata.newTable(q.getName(), qnn, Metadata.Types.VIEW);
            registerQueryColumns(q, seq);
            qnn = SQLConverter.completeEscaping(qnn, false);
            qnn = SQLConverter.checkLang(qnn, conn, false);
            if (qnn.indexOf(" ") > 0) {
                SQLConverter.addWhiteSpacedTableNames(q.getName());
            }

            String querySQL = queryWKT == null ? q.toSQLString() : queryWKT;
            Pivot pivot = null;
            boolean isPivot = q.getType().equals(Query.Type.CROSS_TAB);
            if (isPivot) {
                pivot = new Pivot(conn);

                if (!pivot.parsePivot(querySQL) || (querySQL = pivot.toSQL(q.getName())) == null) {
                    this.notLoaded.put(q.getName(), "cannot load this query");

                    return false;
                }

            }
            querySQL = new DFunction(conn, querySQL).toSQL();
            StringBuffer sb = new StringBuffer("CREATE VIEW ").append(qnn).append(" AS ").append(querySQL);
            String v = null;
            try {
                v = SQLConverter.convertSQL(sb.toString(), true).getSql();

                if (v.trim().endsWith(";")) {
                    v = v.trim().substring(0, v.length() - 1);
                }
                exec(v, false);
                loadedQueries.add(q.getName());
                this.notLoaded.remove(q.getName());
                if (pivot != null) {
                    pivot.registerPivot(SQLConverter.preEscapingIdentifier(q.getName()));
                }
                return true;
            } catch (Exception e) {
                if (e instanceof SQLSyntaxErrorException) {
                    if (queryWKT == null && ((SQLSyntaxErrorException) e).getErrorCode() == OBJECT_ALREADY_EXISTS) {
                        return loadView(q, solveAmbiguous(querySQL));
                    } else {
                        SQLSyntaxErrorException sqle = (SQLSyntaxErrorException) e;
                        if (sqle.getErrorCode() == OBJECT_NOT_FOUND || sqle.getErrorCode() == UNEXPECTED_TOKEN) {
                            ParametricQuery pq = new ParametricQuery(conn, (QueryImpl) q);
                            pq.setIssueWithParameterName(sqle.getErrorCode() == UNEXPECTED_TOKEN);
                            pq.createSelect();
                            if (pq.loaded()) {
                                loadedQueries.add(q.getName());
                                this.notLoaded.remove(q.getName());
                                return true;
                            }

                        }
                    }
                }

                String cause = UcanaccessSQLException.explaneCause(e);

                this.notLoaded.put(q.getName(), ": " + cause);

                if (!err) {
                    Logger.log("Error occured at the first loading attempt of " + q.getName());
                    Logger.log("Converted view was :" + v);
                    Logger.log("Error message was :" + e.getMessage());
                    err = true;
                }
                return false;
            }
        }

        private String solveAmbiguous(String sql) {
            try {
                sql = sql.replaceAll("[\n\r]", " ");
                Pattern pt = Pattern.compile("(.*)[\n\r\\s]*(?i)SELECT([\n\r\\s].*[\n\r\\s])(?i)FROM([\n\r\\s])(.*)");
                Matcher mtc = pt.matcher(sql);
                if (mtc.find()) {
                    String select = mtc.group(2);
                    String pre = mtc.group(1) == null ? "" : mtc.group(1);
                    String[] splitted = select.split(",", -1);
                    StringBuffer sb = new StringBuffer(pre + " select ");
                    List<String> lkl = new LinkedList<String>();

                    for (String s : splitted) {
                        int j = s.lastIndexOf(".");

                        Pattern aliasPt = Pattern.compile("[\\s\n\r]+(?i)AS[\\s\n\r]+");
                        boolean alias = aliasPt.matcher(s).find();
                        if (j < 0 || alias) {
                            lkl.add(s);
                        } else {
                            String k = s.substring(j + 1);
                            if (lkl.contains(k)) {
                                int idx = lkl.indexOf(k);
                                String old = lkl.get(lkl.indexOf(k));
                                lkl.remove(old);
                                lkl.add(idx, splitted[idx] + " AS [" + splitted[idx].trim() + "]");
                                lkl.add(s + " AS [" + s.trim() + "]");
                            } else {
                                lkl.add(k);
                            }
                        }
                    }
                    String comma = "";
                    for (String s : lkl) {
                        sb.append(comma).append(s);

                        comma = ",";
                    }
                    sb.append(" FROM ").append(mtc.group(4));

                    return sb.toString();
                } else {
                    return sql;
                }
            } catch (Exception e) {
                return sql;
            }
        }

        private void loadViews() throws SQLException, IOException {
            List<Query> lq = null;
            List<Query> procedures = new ArrayList<Query>();
            try {
                lq = dbIO.getQueries();
                Iterator<Query> it = lq.iterator();
                while (it.hasNext()) {
                    Query q = it.next();
                    if (!q.getType().equals(Query.Type.SELECT) && !q.getType().equals(Query.Type.UNION)
                            && !q.getType().equals(Query.Type.CROSS_TAB)) {
                        procedures.add(q);
                        it.remove();
                    }

                }
                queryPorting(lq);
            } catch (Exception e) {
                this.notLoaded.put("", "");
            }
            loadProcedures(procedures);

        }

        private void loadProcedures(List<Query> procedures) throws SQLException {
            for (Query q : procedures) {
                ParametricQuery pq = new ParametricQuery(conn, (QueryImpl) q);
                if (!q.getType().equals(Query.Type.DATA_DEFINITION)) {
                    pq.createProcedure();
                    if (pq.loaded()) {
                        loadedProcedures.add(pq.getSignature());

                    } else {
                        String msg = pq.getException() == null ? "" : pq.getException().getMessage();
                        this.notLoadedProcedure.put(q.getName(), msg);

                    }

                }
            }
        }

        private void queryPorting(List<Query> lq) throws SQLException {
            List<String> arn = new ArrayList<String>();
            for (Query q : lq) {
                arn.add(q.getName().toLowerCase());
            }
            boolean heavy = false;
            while (lq.size() > 0) {
                List<Query> arq = new ArrayList<Query>();
                for (Query q : lq) {
                    String qtxt = null;
                    boolean qryGot = true;
                    try {
                        qtxt = q.toSQLString().toLowerCase();
                    } catch (Exception ignore) {
                        qryGot = false;
                    }
                    boolean foundDep = false;
                    if (qryGot && !heavy) {
                        for (String name : arn) {
                            if (qtxt.indexOf(name) != -1) {
                                foundDep = true;
                                break;
                            }
                        }
                    }
                    if (qryGot && !foundDep && loadView(q)) {
                        arq.add(q);
                        arn.remove(q.getName().toLowerCase());
                    }
                }
                if (arq.size() == 0) {
                    if (heavy) {
                        break;
                    } else {
                        heavy = true;
                    }
                }
                lq.removeAll(arq);
            }
            Pivot.clearPrepared();
        }
    }

    private Connection      conn;
    private Database        dbIO;
    private boolean         err;
    private FunctionsLoader functionsLoader   = new FunctionsLoader();
    private List<String>    loadedIndexes     = new ArrayList<String>();
    private List<String>    loadedQueries     = new ArrayList<String>();
    private List<String>    loadedProcedures  = new ArrayList<String>();
    private List<String>    loadedTables      = new ArrayList<String>();
    private LogsFlusher     logsFlusher       = new LogsFlusher();
    private TablesLoader    tablesLoader      = new TablesLoader();
    private TriggersLoader  triggersGenerator = new TriggersLoader();
    private ViewsLoader     viewsLoader       = new ViewsLoader();
    private boolean         sysSchema;
    private boolean         ff1997;
    private boolean         skipIndexes;
    private Metadata        metadata;

    public LoadJet(Connection _conn, Database _dbIo) throws SQLException {
        this.conn = _conn;
        this.dbIO = _dbIo;
        try {
            this.ff1997 = FileFormat.V1997.equals(this.dbIO.getFileFormat());
        } catch (Exception ignore) {
            // Logger.logWarning(e.getMessage());
        }
        this.metadata = new Metadata(_conn);
    }

    public void loadDefaultValues(Table t) throws SQLException, IOException {
        this.tablesLoader.setDefaultValues(t);
    }

    public void loadDefaultValues(Column cl) throws SQLException, IOException {
        this.tablesLoader.setDefaultValue(cl);
    }

    public String defaultValue4SQL(Column cl) throws SQLException, IOException {
        PropertyMap pm = cl.getProperties();
        Object defaulT = pm.getValue(PropertyMap.DEFAULT_VALUE_PROP);
        if (defaulT == null) {
            return null;
        }
        return this.tablesLoader.defaultValue4SQL(defaulT, cl.getType());
    }

    private static boolean hasAutoNumberColumn(Table t) {
        List<? extends Column> lc = t.getColumns();
        for (Column cl : lc) {
            if (cl.isAutoNumber() || DataType.BOOLEAN.equals(cl.getType())) {
                return true;
            }

        }
        return false;
    }

    public void addFunctions(Class<?> clazz) throws SQLException {
        this.functionsLoader.addFunctions(clazz, false);
    }

    private void exec(String expression, boolean logging) throws SQLException {
        Statement st = null;
        try {

            st = conn.createStatement();
            st.executeUpdate(expression);
        } catch (SQLException e) {
            if (logging && e.getErrorCode() != TablesLoader.HSQL_FK_ALREADY_EXISTS) {
                Logger.log("Cannot execute:" + expression + " " + e.getMessage());
            }

            throw e;
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    private void execInsert(PreparedStatement st, List<Object> values) throws SQLException {
        int i = 1;
        for (Object value : values) {
            st.setObject(i++, value);
        }
        // st.execute();
        st.addBatch();
    }

    private String escapeIdentifier(String tn) throws SQLException {
        return SQLConverter.escapeIdentifier(tn, conn);
    }

    public SQLWarning getLoadingWarnings() {
        if (this.viewsLoader.notLoaded.size() == 0 && this.tablesLoader.unresolvedTables.size() == 0) {
            return null;
        }
        SQLWarning sqlw = null;
        for (String s : this.viewsLoader.notLoaded.keySet()) {
            String message = s.length() > 0 ? "Cannot load view " + s + " " + this.viewsLoader.notLoaded.get(s)
                    : "Cannot load views ";
            if (sqlw == null) {
                sqlw = new SQLWarning(message);
            } else {
                sqlw.setNextWarning(new SQLWarning(message));
            }
        }
        for (String s : this.viewsLoader.notLoadedProcedure.keySet()) {
            String message =
                    s.length() > 0 ? "Cannot load procedure " + s + " " + this.viewsLoader.notLoadedProcedure.get(s)
                            : "Cannot load procedures ";
            if (sqlw == null) {
                sqlw = new SQLWarning(message);
            } else {
                sqlw.setNextWarning(new SQLWarning(message));
            }
        }
        for (String s : this.tablesLoader.unresolvedTables) {
            String message = "Cannot resolve table " + s;
            if (sqlw == null) {
                sqlw = new SQLWarning(message);
            } else {
                sqlw.setNextWarning(new SQLWarning(message));
            }
        }
        return sqlw;
    }

    public void resetFunctionsDefault() throws SQLException {
        this.functionsLoader.resetDefault();
    }

    public void loadDB() throws SQLException, IOException {
        try {
            this.functionsLoader.loadMappedFunctions();
            this.tablesLoader.loadTables();
            this.viewsLoader.loadViews();
            conn.commit();
            SQLConverter.cleanEscaped();
        } finally {
            Logger.log("Loaded Tables:");
            logsFlusher.dumpList(this.loadedTables);
            Logger.log("Loaded Queries:");
            logsFlusher.dumpList(this.loadedQueries);
            Logger.log("Loaded Procedures:");
            logsFlusher.dumpList(this.loadedProcedures);
            Logger.log("Loaded Indexes:");
            logsFlusher.dumpList(this.loadedIndexes, true);
            conn.close();
        }
    }

    public void synchronisationTriggers(String tableName, boolean hasAutoNumberColumn, boolean hasAppendOnly)
            throws SQLException {
        this.triggersGenerator.synchronisationTriggers(tableName, hasAutoNumberColumn, hasAppendOnly);
    }

    public Object tryDefault(Object defaulT) throws SQLException {
        Statement st = null;
        try {
            st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT " + defaulT + " FROM   DUAL ");
            if (rs.next()) {
                return rs.getObject(1);
            }
            return null;
        } catch (Exception e) {
            return null;
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    public void setSysSchema(boolean _sysSchema) {
        this.sysSchema = _sysSchema;

    }

    public void setSkipIndexes(boolean _skipIndexes) {
        this.skipIndexes = _skipIndexes;

    }

}
