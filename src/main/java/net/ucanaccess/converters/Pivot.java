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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ucanaccess.jdbc.NormalizedSQL;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Logger;

public class Pivot {
    private String                                 transform;
    private String                                 select;
    private String                                 from;
    private String                                 expression;
    private String                                 pivot;
    private List<String>                           pivotIn;
    private static final Pattern                   PIVOT            =
            Pattern.compile("(?i)TRANSFORM(.*\\W)(?i)SELECT(.*\\W)(?i)FROM(.*\\W)(?i)PIVOT(.*)");
    private static final Pattern                   PIVOT_EXPR       = Pattern.compile("(.*)(?i)IN\\s*\\((.*)\\)");
    private static final Pattern                   PIVOT_AGGR       =
            Pattern.compile("((?i)SUM|MAX|MIN|FIRST|LAST|AVG|COUNT|STDEV|VAR)\\s*\\((.*)\\)");
    private static final Pattern                   PIVOT_CN         = Pattern.compile("[\"'#](.*)[\"'#]");
    private static final String                    PIVOT_GROUP_BY   = "(?i)GROUP\\s*(?i)BY";
    private String                                 aggregateFun;
    private Connection                             conn;
    private boolean                                pivotInCondition = true;
    private String                                 originalQuery;
    private static final Map<String, String>       PIVOT_MAP        = new HashMap<String, String>();
    private static final Map<String, List<String>> PREPARE_MAP      = new HashMap<String, List<String>>();

    public Pivot(Connection _conn) {
        this.conn = _conn;
    }

    public Pivot(String _name, Connection _conn) {

        this.conn = _conn;
    }

    private void cachePrepare(String name) {
        if (this.pivotIn != null) {
            PREPARE_MAP.put(name, this.pivotIn);
        }
    }

    public static void clearPrepared() {
        PREPARE_MAP.clear();

    }

    private void getPrepareFromCache(String name) {
        if (PREPARE_MAP.containsKey(name)) {
            this.pivotIn = PREPARE_MAP.get(name);
        }
    }

    public void registerPivot(String name) {
        if (!this.pivotInCondition) {
            PIVOT_MAP.put(name, this.originalQuery);
        }
    }

    public static void checkAndRefreshPivot(String currSql, UcanaccessConnection conu) {

        for (String name : PIVOT_MAP.keySet()) {
            Pattern ptrn = Pattern.compile("(\\W)(?i)" + name + "(\\W)");
            Matcher mtc = ptrn.matcher(currSql);
            if (mtc.find()) {
                Statement st = null;
                try {
                    if (conu == null && UcanaccessConnection.hasContext()) {
                        conu = UcanaccessConnection.getCtxConnection();
                    }
                    if (conu == null) {
                        return;
                    }
                    Connection conh = conu.getHSQLDBConnection();
                    Pivot pivot = new Pivot(conh);

                    if (!pivot.parsePivot(PIVOT_MAP.get(name))) {
                        return;
                    }
                    String sqlh = pivot.toSQL(null);
                    if (sqlh == null) {
                        return;
                    }
                    st = conh.createStatement();
                    String escqn = SQLConverter.completeEscaping(name, false);

                    st.executeUpdate(SQLConverter.convertSQL("DROP VIEW " + escqn, true).getSql());
                    StringBuffer sb = new StringBuffer("CREATE VIEW ").append(escqn).append(" AS ").append(sqlh);
                    NormalizedSQL nsql = SQLConverter.convertSQL(sb.toString(), true);
                    Metadata mt = new Metadata(conh);
                    String eqn = SQLConverter.preEscapingIdentifier(name);
                    Integer idTable = mt.getTableId(eqn);
                    if (idTable != null) {
                        for (Map.Entry<String, String> entry : nsql.getAliases().entrySet()) {
                            if (mt.getColumnName(eqn, entry.getKey()) == null) {
                                mt.newColumn(entry.getValue(), entry.getKey(), null, idTable);
                            }
                        }
                    }
                    String v = nsql.getSql();
                    st.executeUpdate(v);
                } catch (Exception e) {
                    Logger.logWarning(e.getMessage());
                } finally {
                    if (st != null) {
                        try {
                            st.close();
                        } catch (SQLException e) {
                        }
                    }
                }
            }
        }
    }

    public boolean parsePivot(String _originalQuery) {
        this.originalQuery = _originalQuery;
        _originalQuery = _originalQuery.replaceAll("\n", " ").replaceAll("\r", " ")
                .replaceAll("(?i)(\\[PIVOT\\])", "XPIVOT").trim();
        if (_originalQuery.endsWith(";")) {
            _originalQuery = _originalQuery.substring(0, _originalQuery.length() - 1);
        }
        Matcher mtc = PIVOT.matcher(_originalQuery);
        if (mtc.groupCount() < 4) {
            return false;
        }
        if (mtc.matches()) {
            this.transform = mtc.group(1);
            Matcher aggr = PIVOT_AGGR.matcher(this.transform);
            if (aggr.find()) {
                if (aggr.groupCount() < 2) {
                    return false;
                }
                this.aggregateFun = aggr.group(1);
                this.expression = aggr.group(2);
            } else {
                return false;
            }
            this.select = mtc.group(2);
            this.from = mtc.group(3);
            String pe = mtc.group(4);
            Matcher mtcExpr = PIVOT_EXPR.matcher(pe);
            if (mtcExpr.find()) {
                if (mtcExpr.groupCount() < 2) {
                    return false;
                }
                this.pivot = mtcExpr.group(1);
                this.pivotIn = Arrays.asList(mtcExpr.group(2).split(","));
            } else {
                this.pivot = pe;
            }
            return true;
        } else {
            return false;
        }
    }

    private void appendCaseWhen(StringBuffer sb, String condition, String cn) {
        sb.append(this.aggregateFun).append("(CASE WHEN ").append(condition).append(" THEN ").append(this.expression)
                .append(" END) AS ").append(cn);
    }

    public String verifySQL() {
        StringBuffer sb = new StringBuffer();
        String[] fromS = this.from.split(PIVOT_GROUP_BY);
        sb.append("SELECT DISTINCT ").append(this.pivot).append(" AS PIVOT ");
        sb.append(" FROM ").append(fromS[0]).append(" GROUP BY ").append(this.pivot).append(",").append(fromS[1]);
        return SQLConverter.convertSQL(sb.toString()).getSql();
    }

    public boolean prepare() {
        try {
            if (this.pivotInCondition) {
                this.pivotIn = new ArrayList<String>();
            }
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(verifySQL());
            int i = 0;
            while (rs.next()) {
                String frm = format(rs.getObject("PIVOT"));
                if (frm != null) {
                    this.pivotIn.add(frm);
                }
                i++;
                if (i > 1000) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private String format(Object cln) {
        if (cln == null) {
            return null;
        }
        if (cln instanceof Date) {
            SimpleDateFormat sdf = new SimpleDateFormat("#MM/dd/yyyy HH:mm:ss#");
            String clns = sdf.format((Date) cln);
            if (clns.endsWith(" 00:00:00#")) {
                clns = clns.replaceAll(" 00:00:00", "");
            }
            return clns;
        }
        if (cln instanceof String) {
            return "'" + cln.toString().replaceAll("\'", "''") + "'";
        }
        return cln.toString();
    }

    private String replaceQuotation(String cn) {
        cn = cn.replaceAll("\n", " ").replaceAll("\r", " ");
        Matcher dcm = PIVOT_CN.matcher(cn);

        if (dcm.matches()) {
            cn = dcm.group(1);
        }

        cn = cn.replaceAll("\'", "").replaceAll("\"", "");

        return "[" + cn + "]";
    }

    public String toSQL(String name) {
        if (this.pivotIn == null) {
            if (name != null && PREPARE_MAP.containsKey(name)) {
                this.getPrepareFromCache(name);
            } else if (prepare()) {
                this.cachePrepare(name);
            } else {
                return null;
            }
            this.pivotInCondition = false;
        }
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        sb.append(this.select);
        for (String s : this.pivotIn) {
            sb.append(",");
            appendCaseWhen(sb, this.pivot + "=" + s, replaceQuotation(s));
        }
        sb.append(" FROM ").append(this.from);

        return sb.toString();
    }
}
