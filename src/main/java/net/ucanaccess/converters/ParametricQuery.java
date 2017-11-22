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

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.healthmarketscience.jackcess.impl.query.AppendQueryImpl;
import com.healthmarketscience.jackcess.impl.query.QueryImpl;
import com.healthmarketscience.jackcess.query.Query;

public class ParametricQuery {
    private Connection          hsqldb;
    private QueryImpl           qi;
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
    private StringBuffer        originalParameters = new StringBuffer();

    public ParametricQuery(Connection _hsqldb, QueryImpl _qi) throws SQLException {
        this.hsqldb = _hsqldb;
        if (_qi.getType() == Query.Type.APPEND) {
            this.qi = new AppendQueryTemp((AppendQueryImpl) _qi);
        } else {
            this.qi = _qi;
        }
    }

    public boolean isIssueWithParameterName() {
        return issueWithParameterName;
    }

    public void setIssueWithParameterName(boolean _issueWithParameterName) {
        this.issueWithParameterName = _issueWithParameterName;
    }

    private List<String> queryParameters() {
        List<String> l = qi.getParameters();
        if (this.aposMap != null) {
            return parameterList;
        }
        this.aposMap = new HashMap<String, String>();
        int i = 0;
        this.parameterList = new ArrayList<String>();
        this.parameterList.addAll(l);
        for (String par : parameterList) {
            if (par.indexOf("'") >= 0 || par.indexOf("\"") >= 0) {
                int index = Math.max(Math.max(par.lastIndexOf(' '), par.lastIndexOf('\n')), par.lastIndexOf('\r'));

                String decl = par.substring(0, index).trim();
                if (decl.startsWith("[")) {
                    decl = decl.substring(1, decl.length() - 1);
                }
                String h = treatApos(decl);
                this.aposMap.put(h, decl);

                parameterList.set(i, treatApos(par));
            }
            i++;
        }
        return parameterList;
    }

    public void createProcedure() throws SQLException {
        this.isProcedure = true;
        String sql = null;
        try {

            List<String> l = queryParameters();
            sql = getSQL();

            if (l == null || l.size() == 0) {
                parametersEmpiric();

            } else {
                sql = SQLConverter.removeParameters(sql);
                parametersDeclared();
                if (!conversionOk) {
                    parametersEmpiric(true);
                }
            }

            if (conversionOk) {
                this.exception = null;

            } else {
                return;
            }
            String inside = convertSQL(convertApos(sql)).trim();
            if (!inside.endsWith(";")) {
                inside = inside + ";";
            }
            String procedureName = SQLConverter.escapeIdentifier(qi.getName(), hsqldb);
            String procedure = "CREATE PROCEDURE " + procedureName + "(" + this.parameters + ") MODIFIES SQL DATA \n"
                    + " BEGIN ATOMIC " + inside + "\n END";

            if (exec(procedure)) {
                this.signature = qi.getName() + "(" + this.originalParameters + ")";
                this.loaded = true;
            }
        } catch (Exception _ex) {
            this.exception = _ex;
        }
    }

    public void createSelect() throws SQLException {
        String qnn = null;
        String sql = null;
        try {
            List<String> l = queryParameters();
            qnn = SQLConverter.escapeIdentifier(qi.getName(), hsqldb);
            sql = getSQL();

            if (l == null || l.size() == 0) {
                parametersEmpiric();

            } else {
                sql = SQLConverter.removeParameters(sql);
                parametersDeclared();
                if (!conversionOk) {
                    parametersEmpiric();
                }
            }
            if (conversionOk) {
                this.exception = null;

            } else {
                return;
            }

            String inside = convertSQL(convertApos(sql)).trim();
            if (inside.endsWith(";")) {
                inside = inside.substring(0, inside.length() - 1);
            }
            String funName = qnn;
            String function = "CREATE FUNCTION " + funName + "(" + this.parameters + ") RETURNS " + "TABLE ("
                    + getTableDefinition() + ") READS SQL DATA \n" + " RETURN (TABLE(" + inside + "));";
            if (!exec(function)) {
                return;
            }
            String createView = "CREATE VIEW " + qnn + " AS SELECT * FROM TABLE(" + funName + "("
                    + this.defaultParameterValues + "))";

            if (!exec(createView)) {
                return;
            }
            this.loaded = true;
        } catch (Exception _ex) {
            this.exception = _ex;
        }
    }

    private String convertApos(String sql) {
        if (this.aposMap != null) {
            for (Map.Entry<String, String> me : this.aposMap.entrySet()) {
                sql = sql.replaceAll("(?i)" + Pattern.quote("[" + me.getValue() + "]"), "[" + me.getKey() + "]");
            }
        }

        return sql;
    }

    private String getTableDefinition() throws SQLException {
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        StringBuffer sb = new StringBuffer();
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
        // TODO riww
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

    private String treatApos(String s) {
        s = s.replaceAll("'", "").replaceAll("\"", "").toUpperCase();
        return s;
    }

    private boolean exec(String expression) throws SQLException {
        Statement st = null;
        try {
            st = hsqldb.createStatement();
            st.execute(expression);
            return true;
        } catch (SQLException _ex) {
            this.exception = _ex;
            return false;
        } finally {
            if (st != null) {
                st.close();
            }
        }
    }

    private String getSQL() {
        if (this.sqlContent == null) {
            this.sqlContent = transalteFormFields(this.qi.toSQLString());
        }
        return this.sqlContent;
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
        this.aposMap = new HashMap<String, String>();
        Map<String, Integer> hm = new LinkedHashMap<String, Integer>();
        String s = getSQL();
        if (partialParDecl) {
            s = SQLConverter.removeParameters(s);
        }
        List<String> params = SQLConverter.getParameters(s);
        Map<String, String> parem = new HashMap<String, String>();
        getParametersEmpiric(hm, params, parem, s, true);
    }

    public Exception getException() {
        return exception;
    }

    private void parametersDeclared() {
        List<String> ls = queryParameters();
        StringBuffer args = new StringBuffer();
        String comma = "";
        List<String> ar = new ArrayList<String>();
        for (String par : ls) {
            par = par.trim();
            int index = Math.max(Math.max(par.lastIndexOf(' '), par.lastIndexOf('\n')), par.lastIndexOf('\r'));

            String decl = par.substring(0, index).trim();
            String type = par.substring(index).trim();
            if (!decl.startsWith("[")) {
                decl = "[" + decl + "]";
            }
            ar.add(decl);
            String type0 = type.indexOf("(") > 0 ? type.substring(0, type.indexOf("(")) : type;
            String typeS = type.indexOf("(") > 0 ? type.substring(type.indexOf("(")) : "";
            Map<String, String> hm = TypesMap.getAccess2HsqlTypesMap();

            type = hm.get(type0.toUpperCase()) + typeS;
            if (type.equalsIgnoreCase("VARCHAR")) {
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
            this.ps = hsqldb.prepareStatement(convertSQL(sql));
            if (!this.isProcedure) {
                ParameterMetaData pmd = ps.getParameterMetaData();
                StringBuffer defPar = new StringBuffer();
                comma = "";

                for (int i = 1; i <= pmd.getParameterCount(); i++) {
                    if (!this.isProcedure) {
                        this.ps.setNull(i, pmd.getParameterType(i));
                    }
                    defPar.append(comma).append("NULL");
                    comma = ",";
                }
                this.defaultParameterValues = defPar.toString();
            }
            this.originalParameters = args;
            this.parameters = SQLConverter.convertSQL(args.toString()).getSql();

            this.conversionOk = true;
        } catch (SQLException _ex) {
            this.exception = _ex;
        }

    }

    private List<Integer> parIndexes(String s) {
        List<Integer> ar = new ArrayList<Integer>();
        char character = '?';
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == character) {
                ar.add(i);
            }
        }
        return ar;
    }

    private String convertSQL(String sql) {
        String h = SQLConverter.convertSQL(sql, true).getSql();
        return h;
    }

    private String convertSQL(String sql, List<String> parameters2) {
        for (String s : parameters2) {
            if (s.indexOf("'") > 0 || s.indexOf("\"") > 0) {
                String src = Pattern.quote(s);
                String target = treatApos(s);
                sql = sql.replaceAll(src, target);
                this.aposMap.put(target.substring(1, target.length() - 1), s.substring(1, s.length() - 1));
            }
        }
        return convertSQL(sql);

    }

    // now something truly naif, yeah! It was about time!!!
    private void getParametersEmpiric(Map<String, Integer> _psmp, List<String> _parameters,
            Map<String, String> parem, String sql, boolean init) {
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
                        if (!this.aposMap.containsKey(h)) {
                            _parameters.set(_parameters.indexOf(modf), h);
                            psTxt = psTxt.replaceAll("(?i)" + Pattern.quote(s), convertSQL(h));
                            sql = sql.replaceAll("(?i)" + Pattern.quote(modf), h);
                            h = h.substring(1, h.length() - 1);
                            this.aposMap.put(h, modf.substring(1, modf.length() - 1));
                        }
                    }
                }
            }

            this.ps = hsqldb.prepareStatement(psTxt);

            ParameterMetaData pmd = ps.getParameterMetaData();
            List<String> ar = new ArrayList<String>();
            _psmp = reorderIndexes(_psmp, parem);
            ar.addAll(_psmp.keySet());
            List<Integer> pI = parIndexes(sql);
            StringBuffer parS = new StringBuffer();
            StringBuffer defPar = new StringBuffer();
            int j = 0;
            String comma = "";
            for (int i = 1; i <= pmd.getParameterCount(); i++) {
                if (!this.isProcedure) {
                    this.ps.setNull(i, pmd.getParameterType(i));
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
            this.parameters = parS.toString();
            this.defaultParameterValues = defPar.toString();
            this.conversionOk = true;

        } catch (SQLException _ex) {

            for (String par : _parameters) {
                String par1 = SQLConverter.preEscapingIdentifier(par.substring(1, par.length() - 1));
                int index = sql.toUpperCase().indexOf(par.toUpperCase());
                if (index >= 0 && _ex.getMessage() != null && _ex.getMessage().toUpperCase().endsWith(": " + par1)) {
                    sql = sql.replaceAll("(?i)" + Pattern.quote(par), "?");
                    _psmp.put(par1, index);
                    parem.put(par1, par);
                    String parname = this.originalParameters.length() == 0 ? par : "," + par;
                    this.originalParameters.append(parname);
                    getParametersEmpiric(_psmp, _parameters, parem, sql, false);

                } else {

                    this.exception = _ex;
                }
            }
            if (this.exception == null) {
                this.exception = _ex;

            }

        }

    }

    private Map<String, Integer> reorderIndexes(Map<String, Integer> psmp, Map<String, String> parem) {
        Map<Integer, String> tm = new TreeMap<Integer, String>();
        Integer[] nI = new Integer[psmp.size()];
        String[] sI = new String[psmp.size()];
        Map<String, Integer> dI = new HashMap<String, Integer>();
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

        Map<String, Integer> rlhm = new LinkedHashMap<String, Integer>();
        for (Map.Entry<Integer, String> me : tm.entrySet()) {
            rlhm.put(me.getValue(), me.getKey() + dI.get(me.getValue()));
        }
        if (changedSignature) {
            StringBuffer sb = new StringBuffer();
            String comma = "";
            for (String key : rlhm.keySet()) {
                sb.append(comma).append(parem.get(key));
                comma = ",";
            }
            this.originalParameters = sb;
        }

        return rlhm;
    }

    public boolean loaded() {
        return this.loaded;
    }

    public String getSignature() {
        return this.signature;
    }

}
