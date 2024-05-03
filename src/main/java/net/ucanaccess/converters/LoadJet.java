package net.ucanaccess.converters;

import io.github.spannm.jackcess.*;
import io.github.spannm.jackcess.Database.FileFormat;
import io.github.spannm.jackcess.PropertyMap.Property;
import io.github.spannm.jackcess.complex.ComplexValueForeignKey;
import io.github.spannm.jackcess.impl.ColumnImpl;
import io.github.spannm.jackcess.impl.ColumnImpl.AutoNumberGenerator;
import io.github.spannm.jackcess.impl.IndexData;
import io.github.spannm.jackcess.impl.IndexImpl;
import io.github.spannm.jackcess.impl.query.QueryFormat;
import io.github.spannm.jackcess.impl.query.QueryImpl;
import io.github.spannm.jackcess.query.Query;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.ext.FunctionType;
import net.ucanaccess.jdbc.BlobKey;
import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.triggers.*;
import net.ucanaccess.type.ObjectType;
import net.ucanaccess.util.Try;
import org.hsqldb.error.ErrorCode;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings({"java:S1192", "java:S2692"}) // suppress sonarcloud warnings
public class LoadJet {
    private static final AtomicInteger NAMING_COUNTER = new AtomicInteger(0);

    private final Logger               logger;
    private final Connection           conn;
    private final Database             dbIO;
    private boolean                    err;
    private final FunctionsLoader      functionsLoader   = new FunctionsLoader();
    private final List<String>         loadedIndexes     = new ArrayList<>();
    private final List<String>         loadedQueries     = new ArrayList<>();
    private final List<String>         loadedProcedures  = new ArrayList<>();
    private final List<String>         loadedTables      = new ArrayList<>();
    private final TablesLoader         tablesLoader      = new TablesLoader();
    private final TriggersLoader       triggersGenerator = new TriggersLoader();
    private final ViewsLoader          viewsLoader       = new ViewsLoader();
    private boolean                    sysSchema;
    private boolean                    ff1997;
    private boolean                    skipIndexes;
    private final Metadata             metadata;

    public LoadJet(Connection _conn, Database _dbIo) {
        logger = System.getLogger(getClass().getName());
        conn = _conn;
        dbIO = _dbIo;
        try {
            ff1997 = FileFormat.V1997.equals(dbIO.getFileFormat());
        } catch (Exception _ignored) {
            logger.log(Level.WARNING, _ignored.getMessage());
        }
        metadata = new Metadata(_conn);
    }

    public void loadDefaultValues(Table _t) throws SQLException, IOException {
        tablesLoader.setDefaultValues(_t);
    }

    public void loadDefaultValues(Column _cl) throws SQLException, IOException {
        tablesLoader.setDefaultValue(_cl);
    }

    public String defaultValue4SQL(Column _cl) throws IOException {
        PropertyMap pm = _cl.getProperties();
        Object defaulT = pm.getValue(PropertyMap.DEFAULT_VALUE_PROP);
        if (defaulT == null) {
            return null;
        }
        return tablesLoader.defaultValue4SQL(defaulT, _cl.getType());
    }

    private static boolean hasAutoNumberColumn(Table t) {
        List<? extends Column> cols = t.getColumns();
        for (Column col : cols) {
            if (col.isAutoNumber() || DataType.BOOLEAN.equals(col.getType())) {
                return true;
            }
        }
        return false;
    }

    public void addFunctions(Class<?> _clazz) {
        functionsLoader.addFunctions(_clazz, false);
    }

    private void exec(String _expression, boolean _logging) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(_expression);
        } catch (SQLException _ex) {
            if (_logging && _ex.getErrorCode() != TablesLoader.HSQL_FK_ALREADY_EXISTS) {
                logger.log(Level.WARNING, "Cannot execute {0}: {1}", _expression, _ex.getMessage());
            }
            throw _ex;
        }
    }

    private String escapeIdentifier(String tn) {
        return SQLConverter.escapeIdentifier(tn, conn);
    }

    public SQLWarning getLoadingWarnings() {
        if (viewsLoader.notLoaded.isEmpty() && tablesLoader.unresolvedTables.isEmpty()) {
            return null;
        }
        SQLWarning sqlw = null;
        for (String s : viewsLoader.notLoaded.keySet()) {
            String message = s.length() > 0 ? "Cannot load view " + s + ' ' + viewsLoader.notLoaded.get(s)
                    : "Cannot load views ";
            if (sqlw == null) {
                sqlw = new SQLWarning(message);
            } else {
                sqlw.setNextWarning(new SQLWarning(message));
            }
        }
        for (String s : viewsLoader.notLoadedProcedure.keySet()) {
            String message =
                    s.length() > 0 ? "Cannot load procedure " + s + ' ' + viewsLoader.notLoadedProcedure.get(s)
                            : "Cannot load procedures ";
            if (sqlw == null) {
                sqlw = new SQLWarning(message);
            } else {
                sqlw.setNextWarning(new SQLWarning(message));
            }
        }
        for (String s : tablesLoader.unresolvedTables) {
            String message = "Cannot resolve table " + s;
            if (sqlw == null) {
                sqlw = new SQLWarning(message);
            } else {
                sqlw.setNextWarning(new SQLWarning(message));
            }
        }
        return sqlw;
    }

    public void resetFunctionsDefault() {
        functionsLoader.resetDefault();
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    public void loadDB() throws SQLException, IOException {
        try {
            functionsLoader.loadMappedFunctions();
            tablesLoader.loadTables();
            viewsLoader.loadViews();
            conn.commit();
            SQLConverter.cleanEscaped();
        } finally {
            logger.log(Level.DEBUG, "Loaded tables: {0}", loadedTables);
            logger.log(Level.DEBUG, "Loaded queries: {0}", loadedQueries);
            logger.log(Level.DEBUG, "Loaded procedures: {0}", loadedProcedures);
            logger.log(Level.DEBUG, "Loaded indexes: {0}", loadedIndexes);
            conn.close();
        }
    }

    public void synchronisationTriggers(String tableName, boolean hasAutoNumberColumn, boolean hasAppendOnly)
            throws SQLException {
        triggersGenerator.synchronisationTriggers(tableName, hasAutoNumberColumn, hasAppendOnly);
    }

    public Object tryDefault(Object _default) {
        try (Statement st = conn.createStatement()) {
            ResultSet rs = st.executeQuery(String.format("SELECT %s FROM DUAL", _default));
            if (rs.next()) {
                return rs.getObject(1);
            }
            return null;
        } catch (Exception _ex) {
            return null;
        }
    }

    public void setSysSchema(boolean _sysSchema) {
        sysSchema = _sysSchema;
    }

    public void setSkipIndexes(boolean _skipIndexes) {
        skipIndexes = _skipIndexes;
    }

    private final class FunctionsLoader {

        private final Set<String> functionDefinitions = new LinkedHashSet<>();

        private void addAggregates() {
            functionDefinitions.addAll(List.of(
                getAggregate("last", "LONGVARCHAR"),
                getAggregate("last", "DECIMAL(100,10)"),
                getAggregate("last", "BOOLEAN"),
                getAggregate("first", "LONGVARCHAR"),
                getAggregate("first", "DECIMAL(100,10)"),
                getAggregate("first", "BOOLEAN"),
                getLastTimestamp(),
                getFirstTimestamp()));
        }

        private String getLastTimestamp() {
            return "CREATE AGGREGATE FUNCTION last(IN val TIMESTAMP, IN flag boolean, INOUT ts TIMESTAMP, INOUT counter INT) "
                 + "RETURNS TIMESTAMP CONTAINS SQL BEGIN ATOMIC IF flag THEN RETURN ts; "
                 + "ELSE IF counter IS NULL THEN SET counter = 0; END IF; SET counter = counter + 1; "
                 + "SET ts = val; RETURN NULL; END IF; END";
        }

        private String getFirstTimestamp() {
            return "CREATE AGGREGATE FUNCTION First(IN val TIMESTAMP, IN flag boolean, INOUT ts TIMESTAMP , INOUT counter INT) "
                 + "RETURNS TIMESTAMP CONTAINS SQL BEGIN ATOMIC IF flag THEN RETURN ts; "
                 + "ELSE IF counter IS NULL THEN SET counter = 0; END IF; SET counter = counter + 1; "
                 + " IF counter = 1 THEN SET ts = val; END IF; RETURN NULL; END IF; END ";
        }

        private void addFunction(String _functionName, String _javaMethodName, String _returnType, String... _paramTypes) {
            StringBuilder code = new StringBuilder();
            if (DBReference.is2xx()) {
                String parms = IntStream.rangeClosed(1, _paramTypes.length).mapToObj(i -> "par" + i + ' ' + _paramTypes[i - 1]).collect(Collectors.joining(", "));
                code.append("CREATE FUNCTION ").append(_functionName)
                    .append('(').append(parms).append(')')
                    .append(" RETURNS ").append(_returnType)
                    .append(" LANGUAGE JAVA DETERMINISTIC NO SQL EXTERNAL NAME ")
                    .append("'CLASSPATH:").append(_javaMethodName).append("'");
            } else {
                code.append("CREATE ALIAS ")
                    .append(_functionName)
                    .append(" FOR \"").append(_javaMethodName).append("\"");
            }
            functionDefinitions.add(code.toString());
        }

        private void addFunctions(Class<?> _clazz, boolean _cswitch) {
            Map<String, String> tmap = TypesMap.getAccess2HsqlTypesMap();

            for (Method method : _clazz.getDeclaredMethods()) {

                List<FunctionType> functionTypes = Stream.of(method.getAnnotations())
                    .filter(ant -> ant.annotationType().equals(FunctionType.class))
                    .map(FunctionType.class::cast)
                    .collect(Collectors.toList());

                for (FunctionType func : functionTypes) {
                    String methodName = _clazz.getName() + '.' + method.getName();
                    String functionName = Objects.requireNonNullElse(func.functionName(), methodName);
                    AccessType[] acts = func.argumentTypes();
                    AccessType ret = func.returnType();
                    String retTypeName = ret.name();
                    String returnType = tmap.getOrDefault(retTypeName, retTypeName);
                    if (AccessType.TEXT.equals(ret)) {
                        returnType += "(255)";
                    }
                    String[] args = new String[acts.length];
                    for (int i = 0; i < args.length; i++) {
                        String typeName = acts[i].name();
                        args[i] = tmap.getOrDefault(typeName, typeName);
                        if (AccessType.TEXT.equals(acts[i])) {
                            args[i] += "(255)";
                        }
                    }
                    if (func.namingConflict()) {
                        SQLConverter.addWAFunctionName(functionName);
                        functionName += "WA";
                    }
                    addFunction(functionName, methodName, returnType, args);
                }

            }
            createFunctions();
            if (_cswitch) {
                createSwitch();
            }
        }

        private void resetDefault() {
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

        private void createFunctions() {
            for (String functionDef : functionDefinitions) {
                Try.catching(() -> exec(functionDef, true))
                    .orElse(e -> logger.log(Level.WARNING, "Failed to create function {0}: {1}", functionDef, e.toString()));
            }

            functionDefinitions.clear();
        }

        private void createSwitch() {
            List<DataType> dtypes = List.of(
                DataType.BINARY, DataType.BOOLEAN, DataType.SHORT_DATE_TIME,
                DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.MONEY, DataType.NUMERIC,
                DataType.COMPLEX_TYPE, DataType.MEMO);

            for (DataType dtype : dtypes) {
                String type = TypesMap.map2hsqldb(dtype);

                for (int i = 1; i < 10; i++) {
                    StringBuilder header = new StringBuilder("CREATE FUNCTION SWITCH(");
                    StringBuilder body = new StringBuilder(" (CASE");
                    String comma = "";
                    for (int j = 0; j < i; j++) {
                        body.append(" WHEN B").append(j).append(" THEN V").append(j);
                        header.append(comma);
                        comma = ", ";
                        header.append('B').append(j).append(" BOOLEAN").append(comma)
                              .append('V').append(j).append(' ').append(type);
                    }
                    body.append(" END)");
                    header.append(") RETURNS ").append(type).append(" RETURN").append(body);

                    Try.catching(() -> exec(header.toString(), true))
                        .orElse(ex -> logger.log(Level.WARNING, "Failed to create function {0}: {1}", header, ex.toString()));
                }
            }

        }

        private String getAggregate(String _functionName, String _type) {
            return "CREATE AGGREGATE FUNCTION " + _functionName + "(IN val " + _type + ", IN flag BOOLEAN, INOUT register "
                + _type + ", INOUT counter INT) RETURNS " + _type + " NO SQL LANGUAGE JAVA "
                + "EXTERNAL NAME 'CLASSPATH:net.ucanaccess.converters.FunctionsAggregate." + _functionName + "'";
        }

        private void loadMappedFunctions() {
            addFunctions(Functions.class, true);
            addAggregates();
            createFunctions();
        }
    }

    private final class TablesLoader {
        private static final int    HSQL_FK_ALREADY_EXISTS   = -ErrorCode.X_42528;      // -5528;
        private static final int    HSQL_UK_ALREADY_EXISTS   = -ErrorCode.X_42522;      // -5522
        private static final int    HSQL_NOT_NULL            = -ErrorCode.X_23502;
        private static final int    HSQL_FK_VIOLATION        = -ErrorCode.X_23503;
        private static final int    HSQL_UK_VIOLATION        = -ErrorCode.X_23505;
        private static final String SYSTEM_SCHEMA            = "SYS";
        private static final int    DEFAULT_STEP             = 2000;

        private final List<String>  unresolvedTables         = new ArrayList<>();
        private final List<String>  calculatedFieldsTriggers = new ArrayList<>();
        private final List<String>  loadingOrder             = new LinkedList<>();
        private final Set<Column>   alreadyIndexed           = new LinkedHashSet<>();
        private final Set<String>   readOnlyTables           = new LinkedHashSet<>();

        private String commaSeparated(List<? extends Index.Column> columns, boolean escape) throws SQLException {
            String comma = "";
            StringBuilder sb = new StringBuilder(" (");
            for (Index.Column cd : columns) {
                String cl = escape ? escapeIdentifier(cd.getColumn().getName()) : cd.getColumn().getName();
                sb.append(comma).append(cl);
                comma = ",";
            }
            return sb.append(") ").toString();
        }

        private String schema(String name, boolean systemTable) {
            if (systemTable) {
                return SYSTEM_SCHEMA + '.' + name;
            }
            return name;
        }

        private DataType getReturnType(Column _col) throws IOException {
            if (_col.getProperties().get(PropertyMap.EXPRESSION_PROP) == null
                || _col.getProperties().get(PropertyMap.RESULT_TYPE_PROP) == null) {
                return null;
            }
            byte pos = (Byte) _col.getProperties().get(PropertyMap.RESULT_TYPE_PROP).getValue();
            return DataType.fromByte(pos);
        }

        private String getHsqldbColumnType(Column _col) throws IOException {
            String htype;
            DataType dtyp = _col.getType();
            DataType rtyp = getReturnType(_col);
            boolean calcType = false;
            if (rtyp != null) {
                dtyp = rtyp;
                calcType = true;
            }

            if (dtyp.equals(DataType.TEXT)) {
                int ln = ff1997 ? _col.getLength() : _col.getLengthInUnits();
                htype = "VARCHAR(" + ln + ')';
            } else if (dtyp.equals(DataType.NUMERIC) && (_col.getScale() > 0 || calcType)) {
                if (calcType) {
                    htype = "NUMERIC(100 ,4)";
                } else {
                    htype = "NUMERIC(" + (_col.getPrecision() > 0 ? _col.getPrecision() : 100) + ',' + _col.getScale() + ')';
                }
            } else if (dtyp.equals(DataType.FLOAT)) {
                if (calcType) {
                    htype = "NUMERIC(" + (_col.getPrecision() > 0 ? _col.getPrecision() : 100) + ',' + 7 + ')';
                } else {
                    Object dps = null;
                    Object dpso = _col.getProperties().get("DecimalPlaces");
                    if (dpso != null) {
                        dps = _col.getProperties().get("DecimalPlaces").getValue();
                    }
                    byte dp = dps == null ? 7 : (Byte) dps < 0 ? 7 : (Byte) dps;

                    htype = "NUMERIC(" + (_col.getPrecision() > 0 ? _col.getPrecision() : 100) + ',' + dp + ')';
                }
            } else {
                htype = TypesMap.map2hsqldb(dtyp);
            }
            return htype;
        }

        private String getCalculatedFieldTrigger(String _ntn, Column _col, boolean _isCreate)
                throws IOException, SQLException {
            DataType dt = getReturnType(_col);
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
            String ecl = procedureEscapingIdentifier(_col.getName()).replace("%", "%%");

            return _isCreate
                ? "CREATE TRIGGER expr%d before insert ON " + _ntn + " REFERENCING NEW AS newrow FOR EACH ROW "
                    + " BEGIN  ATOMIC  SET newrow." + ecl + " = " + call + "; END "
                : "CREATE TRIGGER expr%d before update ON " + _ntn
                    + " REFERENCING NEW  AS newrow OLD AS OLDROW FOR EACH ROW BEGIN ATOMIC IF %s THEN "
                    + " SET newrow." + ecl + " = " + call + "; ELSEIF newrow." + ecl + " <> oldrow." + ecl
                    + " THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '"
                    + "The following column is not updatable: " + _col.getName().replace("%", "%%")
                    + "';  END IF ; END ";
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

        private void createSyncrTable(Table _t, boolean _systemTable, boolean _constraints) throws SQLException, IOException {

            String tn = _t.getName();
            if ("DUAL".equalsIgnoreCase(tn)) {
                SQLConverter.setDualUsedAsTableName(true);
            }
            StringBuilder check = new StringBuilder();
            String ntn = SQLConverter.preEscapingIdentifier(tn);

            int seq = metadata.newTable(tn, ntn, ObjectType.TABLE);
            ntn = SQLConverter.completeEscaping(ntn);
            ntn = SQLConverter.checkLang(ntn, conn);
            ntn = schema(ntn, _systemTable);

            StringBuilder sbC = new StringBuilder("CREATE CACHED TABLE ").append(ntn).append('(');

            String comma = "";
            for (Column col : _t.getColumns()) {
                if ("USER".equalsIgnoreCase(col.getName())) {
                    logger.log(Level.WARNING, "You should not use the 'user' reserved word as column name in table {0} "
                        + "(it refers to the database user). "
                        + "Escape it in your SQL e.g. SELECT [user] FROM table WHERE [user] = 'Joe'", _t.getName());
                }
                String expr = getExpression(col);
                if (expr != null && _constraints) {
                    String tgrI = getCalculatedFieldTrigger(ntn, col, true);
                    String tgrU = getCalculatedFieldTrigger(ntn, col, false);
                    calculatedFieldsTriggers.add(String.format(tgrI, NAMING_COUNTER.getAndIncrement(), SQLConverter.convertFormula(expr)));
                    String uc = getUpdateConditions(col);
                    if (uc.length() > 0) {
                        calculatedFieldsTriggers.add(String.format(tgrU, NAMING_COUNTER.getAndIncrement(), uc, SQLConverter.convertFormula(expr)));
                    }

                }

                String cn = SQLConverter.preEscapingIdentifier(col.getName());
                String colType = col.getType().name();
                if (col.isAutoNumber()) {
                    ColumnImpl cli = (ColumnImpl) col;
                    AutoNumberGenerator ang = cli.getAutoNumberGenerator();
                    if (ang.getType().equals(DataType.LONG)) {
                        colType = "COUNTER";
                    }

                } else if (col.isHyperlink()) {
                    colType = "HYPERLINK";
                }
                metadata.newColumn(col.getName(), cn, colType, seq);
                if (expr != null && _constraints) {
                    metadata.calculatedField(_t.getName(), col.getName());
                }
                cn = SQLConverter.completeEscaping(cn);
                cn = SQLConverter.checkLang(cn, conn);
                sbC.append(comma).append(cn).append(' ').append(getHsqldbColumnType(col));
                if (DataType.FLOAT.equals(col.getType())) {
                    check.append(", check (3.4028235E+38>=").append(cn).append(" AND -3.4028235E+38<=").append(cn)
                            .append(")");
                }

                PropertyMap pm = col.getProperties();
                Object required = pm.getValue(PropertyMap.REQUIRED_PROP);
                if (_constraints && required instanceof Boolean && (boolean) required) {
                    sbC.append(" NOT NULL ");
                }
                comma = ",";
            }

            sbC.append(check).append(")");
            exec(sbC.toString(), true);

        }

        private String getExpression(Column _col) throws IOException {
            PropertyMap map = _col.getProperties();
            Property exprp = map.get(PropertyMap.EXPRESSION_PROP);

            if (exprp != null) {
                Table tl = _col.getTable();
                String expr = SQLConverter.convertPowOperator((String) exprp.getValue());
                for (Column col : tl.getColumns()) {
                    expr = expr.replaceAll("\\[(?i)(" + Pattern.quote(col.getName()) + ")\\]", "newrow.$0");
                }
                return expr;
            }
            return null;
        }

        private String getUpdateConditions(Column _col) throws IOException, SQLException {
            PropertyMap map = _col.getProperties();
            Property exprp = map.get(PropertyMap.EXPRESSION_PROP);

            if (exprp != null) {
                Set<String> setu = SQLConverter.getFormulaDependencies(exprp.getValue().toString());

                if (!setu.isEmpty()) {
                    String or = "";
                    StringBuilder cw = new StringBuilder();
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

        private void setDefaultValue(Column _col) throws SQLException, IOException {
            String tn = _col.getTable().getName();
            String ntn = escapeIdentifier(tn);
            List<String> arTrigger = new ArrayList<>();
            setDefaultValue(_col, ntn, arTrigger);
            for (String trigger : arTrigger) {
                exec(trigger, true);
            }
        }

        private String defaultValue4SQL(Object defaulT, DataType dt) {
            if (defaulT == null) {
                return null;
            }
            String default4SQL = SQLConverter.convertSQL(" " + defaulT).getSql();
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
                default4SQL = "'" + default4SQL.replace("'", "''") + "'";
            }
            return default4SQL;
        }

        private void setDefaultValue(Column _col, String _ntn, List<String> _arTrigger) throws IOException, SQLException {
            PropertyMap pm = _col.getProperties();
            String ncn = procedureEscapingIdentifier(_col.getName());
            Object defVal = pm.getValue(PropertyMap.DEFAULT_VALUE_PROP);
            if (defVal != null) {
                String default4SQL = defaultValue4SQL(defVal, _col.getType());
                String guidExp = "GenGUID()";
                if (!guidExp.equals(defVal)) {
                    boolean defIsFunction =
                        defVal.toString().trim().endsWith(")") && defVal.toString().indexOf('(') > 0;
                    if (defIsFunction) {
                        metadata.columnDef(_col.getTable().getName(), _col.getName(), defVal.toString());
                    }
                    Object defFound = default4SQL;
                    boolean isNull = (default4SQL + "").equalsIgnoreCase("null");
                    if (!isNull && (defFound = tryDefault(default4SQL)) == null) {

                        logger.log(Level.WARNING, "Unknown expression: {0} (default value of column {1} table {2})",
                            defVal, _col.getName(), _col.getTable().getName());
                    } else {
                        if (defFound != null && !defIsFunction) {
                            metadata.columnDef(_col.getTable().getName(), _col.getName(), defFound.toString());
                        }
                        if (_col.getType() == DataType.TEXT && defVal.toString().startsWith("'")
                            && defVal.toString().endsWith("'")
                            && defVal.toString().length() > _col.getLengthInUnits()) {
                            logger.log(Level.WARNING, "Default values should start and end with a double quote, "
                                + "the single quote is considered as part of the default value {0} "
                                + "(column {1},table {2}). It may result in a data truncation error at run-time due to max column size {3}",
                                defVal, _col.getName(), _col.getTable().getName(), _col.getLengthInUnits());
                        }
                        _arTrigger.add("CREATE TRIGGER DEFAULT_TRIGGER" + NAMING_COUNTER.getAndIncrement() + " BEFORE INSERT ON " + _ntn
                            + " REFERENCING NEW ROW AS NEW FOR EACH ROW IF NEW." + ncn + " IS NULL THEN "
                            + "SET NEW." + ncn + "= " + default4SQL + " ; END IF");
                    }
                }
            }

        }

        private void setDefaultValues(Table t) throws SQLException, IOException {
            String tn = t.getName();
            String ntn = escapeIdentifier(tn);
            List<String> arTrigger = new ArrayList<>();
            for (Column col : t.getColumns()) {
                setDefaultValue(col, ntn, arTrigger);
            }
            for (String trigger : arTrigger) {
                exec(trigger, true);
            }
        }

        private int countFKs() throws IOException {
            int i = 0;
            for (String tn : loadingOrder) {
                UcanaccessTable table = new UcanaccessTable(dbIO.getTable(tn), tn);
                if (!unresolvedTables.contains(tn)) {
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

        private boolean reorder() throws IOException {
            int maxIteration = countFKs() + 1;

            for (int i = 0; i < maxIteration; i++) {
                boolean change = false;
                List<String> loadingOrder0 = new ArrayList<>(loadingOrder);
                for (String tn : loadingOrder0) {
                    UcanaccessTable table = new UcanaccessTable(dbIO.getTable(tn), tn);
                    if (!unresolvedTables.contains(tn)) {
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
            int ict = loadingOrder.indexOf(ctn);
            int irt = loadingOrder.indexOf(rtn);
            if (ict < irt) {
                loadingOrder.remove(ctn);
                loadingOrder.add(irt, ctn);
                return false;
            }
            return true;
        }

        private void loadForeignKey(Index idxi, String ctn) throws IOException, SQLException {
            IndexImpl idx = (IndexImpl) idxi;
            String rtn = idx.getReferencedIndex().getTable().getName();
            List<IndexData.ColumnDescriptor> cls = idx.getColumns();
            if (cls.size() == 1) {
                alreadyIndexed.add(cls.get(0).getColumn());
            }
            String ntn = escapeIdentifier(ctn);
            if (ntn == null) {
                return;
            }
            String nin = escapeIdentifier(ctn + '_' + idx.getName());
            String colsIdx = commaSeparated(cls, true);
            String colsIdxRef = commaSeparated(idx.getReferencedIndex().getColumns(), true);

            StringBuilder ci = new StringBuilder("ALTER TABLE ").append(ntn)
              .append(" ADD CONSTRAINT ").append(nin);
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
            } catch (SQLException _ex) {
                if (_ex.getErrorCode() == HSQL_FK_ALREADY_EXISTS) {
                    logger.log(Level.WARNING, _ex.getMessage());
                } else {
                    throw _ex;
                }
            }
            loadedIndexes.add("FK on " + ntn + " Columns:" + commaSeparated(cls, false) + " References " + nrt
                    + " Columns:" + commaSeparated(idx.getReferencedIndex().getColumns(), false));
        }

        private void loadIndex(Index idx, String tn) throws SQLException {
            String ntn = escapeIdentifier(tn);
            if (ntn == null) {
                return;
            }
            String nin = idx.getName();
            nin = escapeIdentifier(tn + '_' + nin);
            boolean uk = idx.isUnique();
            boolean pk = idx.isPrimaryKey();
            if (!uk && !pk && idx.getColumns().size() == 1) {
                Column col = idx.getColumns().get(0).getColumn();
                if (alreadyIndexed.contains(col)) {
                    return;
                }
            }
            if (uk && idx.getColumns().size() == 1) {
                Column col = idx.getColumns().get(0).getColumn();
                DataType dt = col.getType();
                if (dt.equals(DataType.COMPLEX_TYPE)) {
                    return;
                }
            }

            StringBuilder ci = new StringBuilder("ALTER TABLE ").append(ntn);
            String colsIdx = commaSeparated(idx.getColumns(), true);
            if (pk) {
                ci.append(" ADD PRIMARY KEY ").append(colsIdx);
            } else if (uk) {
                ci.append(" ADD CONSTRAINT ").append(nin)
                  .append(" UNIQUE ").append(colsIdx);

            } else {
                ci = new StringBuilder("CREATE INDEX ").append(nin).append(" ON ").append(ntn).append(colsIdx);
            }
            try {
                exec(ci.toString(), true);
            } catch (SQLException _ex) {
                if (HSQL_UK_ALREADY_EXISTS == _ex.getErrorCode()) {
                    return;
                }
                if (idx.isUnique()) {
                    for (Index.Column cd : idx.getColumns()) {
                        if (cd.getColumn().getType().equals(DataType.COMPLEX_TYPE)) {
                            return;
                        }
                    }
                }
                logger.log(Level.WARNING, _ex.getMessage());
                return;
            } catch (Exception _ex) {

                logger.log(Level.WARNING, _ex.getMessage());
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
            readOnlyTables.add(t.getName());
            String ntn = schema(escapeIdentifier(tn), systemTable);
            exec("SET TABLE " + ntn + " READONLY TRUE ", false);
            loadedTables.add(tn + " READONLY");
        }

        private void recreate(Table _t, boolean _systemTable, Row _record, int _errorCode) throws SQLException, IOException {
            String type = "";
            switch (_errorCode) {
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
            logger.log(Level.WARNING, "Detected {0} constraint breach, table {1}, record {2}: making the table {3} read-only",
                type, _t.getName(), _record, _t.getName());

            dropTable(_t, _systemTable);
            createSyncrTable(_t, _systemTable, false);
            if (_errorCode != HSQL_FK_VIOLATION) {
                loadTableFKs(_t.getName(), false);
            }
            loadTableData(_t, _systemTable);
            makeTableReadOnly(_t, _systemTable);

        }

        private void createTable(Table t, boolean systemTable) throws SQLException, IOException {
            String tn = t.getName();
            if (tn.indexOf(' ') > 0) {
                SQLConverter.addWhiteSpacedTableNames(tn);
            }
            String ntn = SQLConverter.escapeIdentifier(tn); // clean
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
            loadTableData(t, systemTable, false);
        }

        @SuppressWarnings("PMD.UseTryWithResources")
        private void loadTableData(Table _t, boolean _systemTable, boolean _errorCheck) throws IOException, SQLException {
            TimeZone prevJackcessTimeZone = _t.getDatabase().getTimeZone();
            _t.getDatabase().setTimeZone(TimeZone.getTimeZone("UTC"));
            int step = _errorCheck ? 1 : DEFAULT_STEP;
            int i = 0;
            PreparedStatement ps = null;

            try {
                Iterator<Row> it = _t.iterator();

                while (it.hasNext()) {
                    Row row = it.next();
                    if (row == null) {
                        continue;
                    }
                    List<Object> values = new ArrayList<>();
                    if (ps == null) {
                        ps = sqlInsert(_t, row, _systemTable);
                    }
                    for (Map.Entry<String, Object> entry : row.entrySet()) {
                        values.add(value(entry.getValue(), _t, entry.getKey(), row));
                    }
                    tablesLoader.execInsert(ps, values);

                    if (_errorCheck || i > 0 && i % step == 0 || !it.hasNext()) {
                        try {
                            ps.executeBatch();
                        } catch (SQLException _ex) {
                            int ec = _ex.getErrorCode();
                            if (!_errorCheck && ec == HSQL_NOT_NULL) {
                                dropTable(_t, _systemTable);
                                createSyncrTable(_t, _systemTable, true);
                                loadTableData(_t, _systemTable, true);
                            } else {
                                if (ec == HSQL_NOT_NULL || ec == HSQL_FK_VIOLATION || ec == HSQL_UK_VIOLATION) {
                                    if (ec == HSQL_FK_VIOLATION) {
                                        logger.log(Level.WARNING, _ex.getMessage());
                                    }
                                    recreate(_t, _systemTable, row, _ex.getErrorCode());
                                } else {
                                    throw _ex;
                                }
                            }
                        }
                        if (_errorCheck) {
                            conn.rollback();
                        } else {
                            conn.commit();
                        }
                    }
                    i++;

                }
                if (i != _t.getRowCount() && step != 1) {
                    logger.log(Level.WARNING, "Error in the metadata of the table {0}: the table's row count in metadata is {1} "
                        + "but {2} records have been found and loaded by UCanAccess. "
                        + "All will work fine, but it's better to repair your database",
                        _t.getName(), _t.getRowCount(), i);
                }
            } finally {
                if (ps != null) {
                    ps.close();
                }
            }
            _t.getDatabase().setTimeZone(prevJackcessTimeZone);
        }

        private void loadTableFKs(String tn, boolean autoref) throws IOException, SQLException {
            if (readOnlyTables.contains(tn)) {
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
                        if (autoref && isAuto || !autoref && !isAuto) {
                            loadForeignKey(idx, tn);
                        }
                    }
                }
            }
        }

        private void createCalculatedFieldsTriggers() {
            calculatedFieldsTriggers.forEach(t -> Try.catching(() -> exec(t, false))
                .orElse(e -> logger.log(Level.WARNING, e.getMessage())));
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
                if (tn.startsWith("~")) {
                    logger.log(Level.DEBUG, "Skipping table '{0}'", tn);
                    continue;
                }

                try {
                    Table jt = dbIO.getTable(tn);
                    UcanaccessTable ut = new UcanaccessTable(jt, tn);

                    if (TableMetaData.Type.LINKED_ODBC == jt.getDatabase().getTableMetaData(tn).getType()) {
                        logger.log(Level.WARNING, "Skipping table '{0}' (linked to an ODBC table)", tn);
                        unresolvedTables.add(tn);
                        continue;
                    }

                    createTable(ut);
                    loadingOrder.add(ut.getName());

                } catch (Exception _ex) {
                    logger.log(Level.WARNING, "Failed to create table '{0}': {1}", tn, _ex.getMessage());
                    unresolvedTables.add(tn);
                    continue;
                }

            }
        }

        private void createIndexesUK() throws SQLException, IOException {
            for (String tn : dbIO.getTableNames()) {
                if (!unresolvedTables.contains(tn)) {
                    loadTableIndexesUK(tn);
                    conn.commit();
                }
            }
        }

        private void createIndexesNotUK() throws SQLException, IOException {
            for (String tn : dbIO.getTableNames()) {
                if (!unresolvedTables.contains(tn)) {
                    loadTableIndexesNotUK(tn);
                    conn.commit();
                }
            }
        }

        private void createFKs() throws SQLException, IOException {
            for (String tn : dbIO.getTableNames()) {
                if (!unresolvedTables.contains(tn)) {
                    loadTableFKs(tn, false);
                    conn.commit();
                }
            }

        }

        private void createAutoFKs() throws SQLException, IOException {
            for (String tn : dbIO.getTableNames()) {
                if (!unresolvedTables.contains(tn)) {
                    try {
                        loadTableFKs(tn, true);
                    } catch (SQLException _ex) {
                        UcanaccessTable t = new UcanaccessTable(dbIO.getTable(tn), tn);
                        makeTableReadOnly(t, false);
                    }
                    conn.commit();
                }
            }

        }

        private void loadTablesData() throws SQLException, IOException {
            for (String tn : loadingOrder) {
                if (!unresolvedTables.contains(tn)) {
                    UcanaccessTable t = new UcanaccessTable(dbIO.getTable(tn), tn);
                    loadTableData(t, false);
                    conn.commit();

                }
            }
        }

        private void createTriggers() throws IOException, SQLException {

            for (String tn : loadingOrder) {
                if (!unresolvedTables.contains(tn) && !readOnlyTables.contains(tn)) {
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
                    } catch (Exception _ignored) {
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
                throws SQLException {
            String tn = t.getName();
            String ntn = schema(escapeIdentifier(tn), systemTable);
            String comma = "";
            StringBuilder sbI = new StringBuilder(" INSERT INTO ").append(ntn).append(" (");
            StringBuilder sbE = new StringBuilder(" VALUES( ");
            Set<String> se = row.keySet();
            comma = "";
            for (String cn : se) {
                sbI.append(comma).append(escapeIdentifier(cn));
                sbE.append(comma).append(" ? ");
                comma = ",";
            }
            sbI.append(") ");
            sbE.append(')');
            sbI.append(sbE);

            return conn.prepareStatement(sbI.toString());
        }

        private Object value(Object value, Table table, String columnName, Row row) throws SQLException {
            if (value == null) {
                return null;
            }
            if (value instanceof Float) {
                if (value.equals(Float.NaN)) {
                    return value;
                }
                return new BigDecimal(value.toString());
            }
            if (value instanceof Date && !(value instanceof Timestamp)) {
                return LocalDateTime.ofInstant(((Date) value).toInstant(), ZoneId.of("UTC"));
            }
            if (value instanceof ComplexValueForeignKey) {
                try {
                    return ComplexBase.convert((ComplexValueForeignKey) value);
                } catch (IOException _ex) {
                    throw new UcanaccessSQLException(_ex);
                }
            }
            if (value instanceof byte[] && BlobKey.hasPrimaryKey(table)) {
                BlobKey bk = new BlobKey(table, columnName, row);
                return bk.getBytes();
            }

            if (value instanceof Byte) {
                return SQLConverter.asUnsigned((Byte) value);
            }
            return value;
        }

        private void execInsert(PreparedStatement st, List<Object> values) throws SQLException {
            int i = 1;
            for (Object value : values) {
                st.setObject(i++, value);
            }
            // st.execute();
            st.addBatch();
        }

    }

    private final class TriggersLoader {
        void loadTrigger(String tableName, String namePrefix, String when, Class<? extends TriggerBase> clazz) throws SQLException {
            String triggerName = escapeIdentifier(namePrefix + '_' + tableName);
            String q0 = DBReference.is2xx() ? "" : "QUEUE 0 ";
            exec("CREATE TRIGGER " + triggerName + ' ' + when + " ON " + tableName + " FOR EACH ROW " + q0
                + "CALL \"" + clazz.getName() + "\"", true);
        }

        void synchronisationTriggers(String tableName, boolean hasAutoNumberColumn, boolean hasAutoAppendOnly) throws SQLException {
            loadTrigger(tableName, "genericInsert", "AFTER INSERT", TriggerInsert.class);
            loadTrigger(tableName, "genericUpdate", "AFTER UPDATE", TriggerUpdate.class);
            loadTrigger(tableName, "genericDelete", "AFTER DELETE", TriggerDelete.class);
            if (hasAutoAppendOnly) {
                loadTrigger(tableName, "appendOnly", "BEFORE INSERT", TriggerAppendOnly.class);
                loadTrigger(tableName, "appendOnly_upd", "BEFORE UPDATE", TriggerAppendOnly.class);
            }
            if (hasAutoNumberColumn) {
                loadTrigger(tableName, "autonumber", "BEFORE INSERT", TriggerAutoNumber.class);
                loadTrigger(tableName, "autonumber_validate", "BEFORE UPDATE", TriggerAutoNumber.class);
            }
        }
    }

    private final class ViewsLoader {
        private static final int          OBJECT_ALREADY_EXISTS = -ErrorCode.X_42504;
        private static final int          OBJECT_NOT_FOUND      = -ErrorCode.X_42501;
        private static final int          UNEXPECTED_TOKEN      = -ErrorCode.X_42581;

        private final Map<String, String> notLoaded             = new HashMap<>();
        private final Map<String, String> notLoadedProcedure    = new HashMap<>();

        private boolean loadView(Query _q) throws SQLException {
            return loadView(_q, null);
        }

        private void registerQueryColumns(Query _q, int _seq) throws SQLException {
            QueryImpl qi = (QueryImpl) _q;
            for (QueryImpl.Row row : qi.getRows()) {

                if (QueryFormat.COLUMN_ATTRIBUTE.equals(row._attribute)) {
                    String name = row._name1;

                    if (name == null) {
                        int beginIndex = Math.max(row._expression.lastIndexOf('['), row._expression.lastIndexOf('.'));

                        if (beginIndex < 0 || beginIndex == row._expression.length() - 1
                                || row._expression.endsWith(")")) {
                            continue;
                        }
                        name = row._expression.substring(beginIndex + 1);
                        if (name.endsWith("]")) {
                            name = name.substring(0, name.length() - 1);
                        }
                        if (name.contentEquals("*")) {
                            String table = row._expression.substring(0, beginIndex);
                            List<String> result = metadata.getColumnNames(table);
                            if (result != null) {
                                for (String column : result) {
                                    metadata.newColumn(column, SQLConverter.preEscapingIdentifier(column), null, _seq);
                                }
                                // return;
                            }

                        }
                    }

                    metadata.newColumn(name, SQLConverter.preEscapingIdentifier(name), null, _seq);

                }
            }
        }

        private boolean loadView(Query q, String queryWKT) throws SQLException {
            String qnn = SQLConverter.preEscapingIdentifier(q.getName());
            if (qnn == null) {
                return false;
            }
            int seq = metadata.newTable(q.getName(), qnn, ObjectType.VIEW);
            registerQueryColumns(q, seq);
            qnn = SQLConverter.completeEscaping(qnn, false);
            qnn = SQLConverter.checkLang(qnn, conn, false);
            if (qnn.indexOf(' ') > 0) {
                SQLConverter.addWhiteSpacedTableNames(q.getName());
            }

            String querySQL = queryWKT == null ? q.toSQLString() : queryWKT;
            Pivot pivot = null;
            boolean isPivot = q.getType().equals(Query.Type.CROSS_TAB);
            if (isPivot) {
                pivot = new Pivot(conn);

                if (!pivot.parsePivot(querySQL) || (querySQL = pivot.toSQL(q.getName())) == null) {
                    notLoaded.put(q.getName(), "cannot load this query");

                    return false;
                }

            }
            querySQL = new DFunction(conn, querySQL).toSQL();
            StringBuilder sb = new StringBuilder("CREATE VIEW ").append(qnn).append(" AS ").append(querySQL);
            String v = null;
            try {
                v = SQLConverter.convertSQL(sb.toString(), true).getSql();

                if (v.trim().endsWith(";")) {
                    v = v.trim().substring(0, v.length() - 1);
                }
                exec(v, false);
                loadedQueries.add(q.getName());
                notLoaded.remove(q.getName());
                if (pivot != null) {
                    pivot.registerPivot(SQLConverter.preEscapingIdentifier(q.getName()));
                }
                return true;
            } catch (Exception _ex) {
                if (_ex instanceof SQLSyntaxErrorException) {
                    if (queryWKT == null && ((SQLSyntaxErrorException) _ex).getErrorCode() == OBJECT_ALREADY_EXISTS) {
                        return loadView(q, solveAmbiguous(querySQL));
                    } else {
                        SQLSyntaxErrorException sqle = (SQLSyntaxErrorException) _ex;
                        if (sqle.getErrorCode() == OBJECT_NOT_FOUND || sqle.getErrorCode() == UNEXPECTED_TOKEN) {
                            ParametricQuery pq = new ParametricQuery(conn, (QueryImpl) q);
                            pq.setIssueWithParameterName(sqle.getErrorCode() == UNEXPECTED_TOKEN);
                            pq.createSelect();
                            if (pq.loaded()) {
                                loadedQueries.add(q.getName());
                                notLoaded.remove(q.getName());
                                return true;
                            }

                        }
                    }
                }

                String cause = UcanaccessSQLException.explainCause(_ex);

                notLoaded.put(q.getName(), ": " + cause);

                if (!err) {
                    logger.log(Level.WARNING, "Error occured at the first loading attempt of {0}", q.getName());
                    logger.log(Level.WARNING, "Converted view was: {0}", v);
                    logger.log(Level.WARNING, "Error message was: {0}", _ex.getMessage());
                    err = true;
                }
                return false;
            }
        }

        @SuppressWarnings("java:S5852")
        private String solveAmbiguous(String sql) {
            try {
                sql = sql.replaceAll("\\s+", " ");
                Pattern pat = Pattern.compile("(.*)\\s+SELECT(\\s.*\\s)FROM(\\s)(.*)", Pattern.CASE_INSENSITIVE);
                Matcher mtc = pat.matcher(sql);
                if (mtc.find()) {
                    String select = mtc.group(2);
                    String pre = mtc.group(1) == null ? "" : mtc.group(1);
                    String[] split = select.split(",", -1);
                    StringBuilder sb = new StringBuilder(pre).append(" select ");
                    List<String> lkl = new LinkedList<>();

                    Pattern patAlias = Pattern.compile("\\s+AS\\s+", Pattern.CASE_INSENSITIVE);
                    for (String s : split) {
                        int i = s.lastIndexOf('.');
                        boolean alias = patAlias.matcher(s).find();
                        if (i < 0 || alias) {
                            lkl.add(s);
                        } else {
                            String k = s.substring(i + 1);
                            if (lkl.contains(k)) {
                                int idx = lkl.indexOf(k);
                                String old = lkl.get(lkl.indexOf(k));
                                lkl.remove(old);
                                lkl.add(idx, split[idx] + " AS [" + split[idx].trim() + ']');
                                lkl.add(s + " AS [" + s.trim() + ']');
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
            } catch (Exception _ex) {
                return sql;
            }
        }

        private void loadViews() throws SQLException {
            List<Query> lq = null;
            List<Query> procedures = new ArrayList<>();
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
            } catch (Exception _ex) {
                notLoaded.put("", "");
            }
            loadProcedures(procedures);

        }

        private void loadProcedures(List<Query> procedures) {
            for (Query q : procedures) {
                ParametricQuery pq = new ParametricQuery(conn, (QueryImpl) q);
                if (!q.getType().equals(Query.Type.DATA_DEFINITION)) {
                    pq.createProcedure();
                    if (pq.loaded()) {
                        loadedProcedures.add(pq.getSignature());

                    } else {
                        String msg = pq.getException() == null ? "" : pq.getException().getMessage();
                        notLoadedProcedure.put(q.getName(), msg);

                    }

                }
            }
        }

        private void queryPorting(List<Query> lq) throws SQLException {
            List<String> arn = new ArrayList<>();
            for (Query q : lq) {
                arn.add(q.getName().toLowerCase());
            }
            boolean heavy = false;
            while (!lq.isEmpty()) {
                List<Query> arq = new ArrayList<>();
                for (Query q : lq) {
                    String qtxt = null;
                    boolean qryGot = true;
                    try {
                        qtxt = q.toSQLString().toLowerCase();
                    } catch (Exception _ignored) {
                        qryGot = false;
                    }
                    boolean foundDep = false;
                    if (qryGot && !heavy) {
                        for (String name : arn) {
                            if (qtxt.contains(name)) {
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
                if (arq.isEmpty()) {
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

}
