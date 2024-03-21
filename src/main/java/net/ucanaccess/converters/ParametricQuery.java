package net.ucanaccess.converters;

import io.github.spannm.jackcess.impl.query.AppendQueryImpl;
import io.github.spannm.jackcess.impl.query.QueryImpl;
import io.github.spannm.jackcess.query.Query;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

@SuppressWarnings("java:S2692") // suppress sonarcloud warnings
public class ParametricQuery {
    private final Connection    hsqldb;
    private final QueryImpl     qi;
    private boolean             loaded;
    private PreparedStatement   ps;
    private String              parameters;
    private String              defaultParameterValues;
    private boolean             conversionOk;
    private boolean             issueWithParameterName;
    private Map<String, String> aposMap;
    private List<String>        parameterList;
    private Exception           exception;
    private boolean             isProcedure;
    private String              sqlContent;
    private String              signature;
    private StringBuilder       originalParameters = new StringBuilder();

    public ParametricQuery(Connection _hsqldb, QueryImpl _qi) {
        hsqldb = _hsqldb;
        if (_qi.getType() == Query.Type.APPEND) {
            qi = new AppendQueryTemp((AppendQueryImpl) _qi);
        } else {
            qi = _qi;
        }
    }

    public boolean isIssueWithParameterName() {
        return issueWithParameterName;
    }

    public void setIssueWithParameterName(boolean _issueWithParameterName) {
        issueWithParameterName = _issueWithParameterName;
    }

    private List<String> queryParameters() {
        List<String> l = qi.getParameters();
        if (aposMap != null) {
            return parameterList;
        }
        aposMap = new HashMap<>();
        int i = 0;
        parameterList = new ArrayList<>();
        parameterList.addAll(l);
        for (String par : parameterList) {
            if (par.contains("'") || par.contains("\"")) {
                int index = Math.max(Math.max(par.lastIndexOf(' '), par.lastIndexOf('\n')), par.lastIndexOf('\r'));

                String decl = par.substring(0, index).trim();
                if (decl.startsWith("[")) {
                    decl = decl.substring(1, decl.length() - 1);
                }
                String h = treatApos(decl);
                aposMap.put(h, decl);

                parameterList.set(i, treatApos(par));
            }
            i++;
        }
        return parameterList;
    }

    public void createProcedure() {
        isProcedure = true;
        String sql = null;
        try {

            List<String> l = queryParameters();
            sql = getSQL();

            if (l == null || l.isEmpty()) {
                parametersEmpiric();

            } else {
                sql = SQLConverter.removeParameters(sql);
                parametersDeclared();
                if (!conversionOk) {
                    parametersEmpiric(true);
                }
            }

            if (conversionOk) {
                exception = null;

            } else {
                return;
            }
            String inside = convertSQL(convertApos(sql)).trim();
            if (!inside.endsWith(";")) {
                inside = inside + ";";
            }
            String procedureName = SQLConverter.escapeIdentifier(qi.getName(), hsqldb);
            String procedure = "CREATE PROCEDURE " + procedureName + "(" + parameters + ") MODIFIES SQL DATA \n"
                    + " BEGIN ATOMIC " + inside + "\n END";

            if (exec(procedure)) {
                signature = qi.getName() + "(" + originalParameters + ")";
                loaded = true;
            }
        } catch (Exception _ex) {
            exception = _ex;
        }
    }

    public void createSelect() {
        String qnn = null;
        String sql = null;
        try {
            List<String> l = queryParameters();
            qnn = SQLConverter.escapeIdentifier(qi.getName(), hsqldb);
            sql = getSQL();

            if (l == null || l.isEmpty()) {
                parametersEmpiric();

            } else {
                sql = SQLConverter.removeParameters(sql);
                parametersDeclared();
                if (!conversionOk) {
                    parametersEmpiric();
                }
            }
            if (conversionOk) {
                exception = null;

            } else {
                return;
            }

            String inside = convertSQL(convertApos(sql)).trim();
            if (inside.endsWith(";")) {
                inside = inside.substring(0, inside.length() - 1);
            }
            String funName = qnn;
            String function = "CREATE FUNCTION " + funName + "(" + parameters + ") RETURNS " + "TABLE ("
                    + getTableDefinition() + ") READS SQL DATA \n" + " RETURN (TABLE(" + inside + "));";
            if (!exec(function)) {
                return;
            }
            String createView = "CREATE VIEW " + qnn + " AS SELECT * FROM TABLE(" + funName + "("
                    + defaultParameterValues + "))";

            if (!exec(createView)) {
                return;
            }
            loaded = true;
        } catch (Exception _ex) {
            exception = _ex;
        }
    }

    private String convertApos(String sql) {
        if (aposMap != null) {
            for (Map.Entry<String, String> me : aposMap.entrySet()) {
                sql = sql.replaceAll("(?i)" + Pattern.quote("[" + me.getValue() + "]"), "[" + me.getKey() + "]");
            }
        }

        return sql;
    }

    private String getTableDefinition() throws SQLException {
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        StringBuilder sb = new StringBuilder();
        String comma = "";
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            String type = completeTypeName(rsmd.getColumnTypeName(i), rsmd.getPrecision(i), rsmd.getScale(i), false);
            sb.append(comma).append(SQLConverter.escapeIdentifier(rsmd.getColumnLabel(i), hsqldb)).append(" ")
                    .append(type);
            comma = ",";
        }
        return sb.toString();
    }

    private String completeTypeName(String type, int i, int j, boolean useDefault) {
        if ("VARCHAR".equals(type)) {
            type += "(" + i + ")";
        }
        if ("NUMERIC".equals(type)) {
            if (useDefault) {
                type += "(100,10)";
            } else {
                type += "(" + i + "," + j + ")";
            }
        }
        return type;
    }

    private String treatApos(String _s) {
        return _s.replace("'", "").replace("\"", "").toUpperCase();
    }

    private boolean exec(String expression) {
        try (Statement st = hsqldb.createStatement()) {
            st.execute(expression);
            return true;
        } catch (SQLException _ex) {
            exception = _ex;
            return false;
        }
    }

    private String getSQL() {
        if (sqlContent == null) {
            sqlContent = transalteFormFields(qi.toSQLString());
        }
        return sqlContent;
    }

    private static String transalteFormFields(String sqlString) {
        return sqlString.replaceAll("\\[([^\\]]*)\\]\\!\\[([^\\]]*)\\]\\!\\[([^\\]]*)\\]", "[$1_$2_$3]")
                .replaceAll("\\[(\\w*)\\]\\!\\[(\\w*)\\]\\!\\[(\\w*)\\]", "[$1_$2_$3]")
                .replaceAll("((?i)FORMS)\\.(\\w*)\\.(\\w*)", "[$1_$2_$3]");
    }

    private void parametersEmpiric() {
        parametersEmpiric(false);
    }

    private void parametersEmpiric(boolean partialParDecl) {
        aposMap = new HashMap<>();
        Map<String, Integer> hm = new LinkedHashMap<>();
        String s = getSQL();
        if (partialParDecl) {
            s = SQLConverter.removeParameters(s);
        }
        List<String> params = SQLConverter.getParameters(s);
        Map<String, String> parem = new HashMap<>();
        doParametersEmpiric(hm, params, parem, s);
    }

    public Exception getException() {
        return exception;
    }

    private void parametersDeclared() {
        List<String> ls = queryParameters();
        StringBuilder args = new StringBuilder();
        String comma = "";
        List<String> ar = new ArrayList<>();
        for (String par : ls) {
            par = par.trim();
            int index = Math.max(Math.max(par.lastIndexOf(' '), par.lastIndexOf('\n')), par.lastIndexOf('\r'));

            String decl = par.substring(0, index).trim();
            String type = par.substring(index).trim();
            if (!decl.startsWith("[")) {
                decl = "[" + decl + "]";
            }
            ar.add(decl);
            String type0 = type.indexOf('(') > 0 ? type.substring(0, type.indexOf('(')) : type;
            String typeS = type.indexOf('(') > 0 ? type.substring(type.indexOf('(')) : "";
            Map<String, String> hm = TypesMap.getAccess2HsqlTypesMap();

            type = hm.get(type0.toUpperCase()) + typeS;
            if ("VARCHAR".equalsIgnoreCase(type)) {
                type += "(255)";
            }

            args.append(comma).append(decl).append(" ").append(type);
            comma = ",";

        }
        String sql = getSQL();
        sql = convertApos(SQLConverter.removeParameters(sql));

        for (String var : ar) {
            sql = sql.replaceAll("(?i)" + Pattern.quote(var), "?");
        }

        try {
            ps = hsqldb.prepareStatement(convertSQL(sql));
            if (!isProcedure) {
                ParameterMetaData pmd = ps.getParameterMetaData();
                StringBuilder defPar = new StringBuilder();
                comma = "";

                for (int i = 1; i <= pmd.getParameterCount(); i++) {
                    if (!isProcedure) {
                        ps.setNull(i, pmd.getParameterType(i));
                    }
                    defPar.append(comma).append("NULL");
                    comma = ",";
                }
                defaultParameterValues = defPar.toString();
            }
            originalParameters = args;
            parameters = SQLConverter.convertSQL(args.toString()).getSql();

            conversionOk = true;
        } catch (SQLException _ex) {
            exception = _ex;
        }

    }

    private List<Integer> parIndexes(String s) {
        List<Integer> ar = new ArrayList<>();
        char character = '?';
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == character) {
                ar.add(i);
            }
        }
        return ar;
    }

    private String convertSQL(String sql) {
        return SQLConverter.convertSQL(sql, true).getSql();
    }

    private String convertSQL(String sql, List<String> parameters2) {
        for (String s : parameters2) {
            if (s.indexOf('\'') > 0 || s.indexOf('"') > 0) {
                String src = Pattern.quote(s);
                String target = treatApos(s);
                sql = sql.replaceAll(src, target);
                aposMap.put(target.substring(1, target.length() - 1), s.substring(1, s.length() - 1));
            }
        }
        return convertSQL(sql);

    }

    // now something truly naif, yeah! It was about time!!!
    private void doParametersEmpiric(Map<String, Integer> _psmp, List<String> _parameters, Map<String, String> parem, String sql) {
        String psTxt = null;
        try {
            psTxt = convertSQL(sql, _parameters);

            List<String> l = SQLConverter.getParameters(psTxt);

            // apostrophe treatment
            for (String s : l) {
                String h = treatApos(s);
                for (String modf : _parameters) {
                    if (convertSQL(modf).equals(s)) {
                        h = treatApos(modf);
                        if (!aposMap.containsKey(h)) {
                            _parameters.set(_parameters.indexOf(modf), h);
                            psTxt = psTxt.replaceAll("(?i)" + Pattern.quote(s), convertSQL(h));
                            sql = sql.replaceAll("(?i)" + Pattern.quote(modf), h);
                            h = h.substring(1, h.length() - 1);
                            aposMap.put(h, modf.substring(1, modf.length() - 1));
                        }
                    }
                }
            }

            ps = hsqldb.prepareStatement(psTxt);

            ParameterMetaData pmd = ps.getParameterMetaData();
            _psmp = reorderIndexes(_psmp, parem);
            List<String> ar = new ArrayList<>(_psmp.keySet());
            List<Integer> pI = parIndexes(sql);
            StringBuilder parS = new StringBuilder();
            StringBuilder defPar = new StringBuilder();
            int j = 0;
            String comma = "";
            for (int i = 1; i <= pmd.getParameterCount(); i++) {
                if (!isProcedure) {
                    ps.setNull(i, pmd.getParameterType(i));
                }

                if (j > ar.size() - 1) {
                    continue;
                }
                String key = ar.get(j);
                int index = _psmp.get(key);
                if (index == pI.get(i - 1)) {
                    defPar.append(comma).append("NULL");
                    String type =
                            completeTypeName(pmd.getParameterTypeName(i), pmd.getPrecision(i), pmd.getScale(i), true);
                    parS.append(comma).append(SQLConverter.escapeIdentifier(key, hsqldb)).append(" ").append(type);
                    comma = ",";
                    j++;
                }

            }
            parameters = parS.toString();
            defaultParameterValues = defPar.toString();
            conversionOk = true;

        } catch (SQLException _ex) {

            for (String par : _parameters) {
                String par1 = SQLConverter.preEscapingIdentifier(par.substring(1, par.length() - 1));
                int index = sql.toUpperCase().indexOf(par.toUpperCase());
                if (index >= 0 && _ex.getMessage() != null && (_ex.getMessage().toUpperCase().endsWith(": " + par1)
                        || _ex.getMessage().toUpperCase().contains(": " + par1 + " IN STATEMENT "))) {
                    sql = sql.replaceAll("(?i)" + Pattern.quote(par), "?");
                    _psmp.put(par1, index);
                    parem.put(par1, par);
                    String parname = originalParameters.length() == 0 ? par : "," + par;
                    originalParameters.append(parname);
                    doParametersEmpiric(_psmp, _parameters, parem, sql);

                } else {
                    exception = _ex;
                }
            }
            if (exception == null) {
                exception = _ex;

            }

        }

    }

    private Map<String, Integer> reorderIndexes(Map<String, Integer> psmp, Map<String, String> parem) {
        Map<Integer, String> tm = new TreeMap<>();
        Integer[] nI = new Integer[psmp.size()];
        String[] sI = new String[psmp.size()];
        Map<String, Integer> dI = new HashMap<>();
        int j = 0;
        for (Map.Entry<String, Integer> me : psmp.entrySet()) {
            tm.put(me.getValue(), me.getKey());
            nI[j] = me.getValue();
            sI[j] = me.getKey();
            dI.put(sI[j], 0);
            j++;
        }

        boolean changedSignature = false;
        for (int i = 0; i < tm.size() - 1; i++) {

            for (j = i + 1; j < tm.size(); j++) {
                if (nI[j] < nI[i]) {
                    changedSignature = true;
                    dI.put(sI[i], dI.get(sI[i]) - parem.get(sI[j]).length() + 1);
                }
            }
        }

        Map<String, Integer> rlhm = new LinkedHashMap<>();
        for (Map.Entry<Integer, String> me : tm.entrySet()) {
            rlhm.put(me.getValue(), me.getKey() + dI.get(me.getValue()));
        }
        if (changedSignature) {
            StringBuilder sb = new StringBuilder();
            String comma = "";
            for (String key : rlhm.keySet()) {
                sb.append(comma).append(parem.get(key));
                comma = ",";
            }
            originalParameters = sb;
        }

        return rlhm;
    }

    public boolean loaded() {
        return loaded;
    }

    public String getSignature() {
        return signature;
    }

}
