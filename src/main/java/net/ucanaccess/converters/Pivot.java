package net.ucanaccess.converters;

import static net.ucanaccess.util.SqlConstants.PIVOT;

import net.ucanaccess.jdbc.NormalizedSQL;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.log.Logger;
import net.ucanaccess.util.Try;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Pivot {
    private static final Pattern                   PIVOT_PATT       =
        Pattern.compile("(?i)TRANSFORM(.*\\W)(?i)SELECT(.*\\W)(?i)FROM(.*\\W)(?i)PIVOT(.*)");
    private static final Pattern                   PIVOT_EXPR_PATT  = Pattern.compile("(.*)(?i)IN\\s*\\((.*)\\)");
    private static final Pattern                   PIVOT_AGGR_PATT  =
        Pattern.compile("((?i)SUM|MAX|MIN|FIRST|LAST|AVG|COUNT|STDEV|VAR)\\s*\\((.*)\\)");
    private static final Pattern                   PIVOT_CN_PATT    = Pattern.compile("[\"'#](.*)[\"'#]");
    private static final String                    PIVOT_GROUP_BY   = "(?i)GROUP\\s*(?i)BY";
    private String                                 transform;
    private String                                 select;
    private String                                 from;
    private String                                 expression;
    private String                                 pivot;
    private List<String>                           pivotIn;
    private String                                 aggregateFun;
    private Connection                             conn;
    private boolean                                pivotInCondition = true;
    private String                                 originalQuery;
    private static final Map<String, String>       PIVOT_MAP        = new HashMap<>();
    private static final Map<String, List<String>> PREPARE_MAP      = new HashMap<>();

    public Pivot(Connection _conn) {
        conn = _conn;
    }

    private void cachePrepare(String name) {
        if (pivotIn != null) {
            PREPARE_MAP.put(name, pivotIn);
        }
    }

    public static void clearPrepared() {
        PREPARE_MAP.clear();

    }

    private void getPrepareFromCache(String name) {
        if (PREPARE_MAP.containsKey(name)) {
            pivotIn = PREPARE_MAP.get(name);
        }
    }

    public void registerPivot(String name) {
        if (!pivotInCondition) {
            PIVOT_MAP.put(name, originalQuery);
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
                    NormalizedSQL nsql = SQLConverter.convertSQL("CREATE VIEW " + escqn + " AS " + sqlh, true);
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
                } catch (Exception _ex) {
                    Logger.logWarning(_ex.getMessage());
                } finally {
                    if (st != null) {
                        try {
                            st.close();
                        } catch (SQLException ignored) {
                        }
                    }
                }
            }
        }
    }

    public boolean parsePivot(String _originalQuery) {
        originalQuery = _originalQuery;
        _originalQuery = _originalQuery.replace('\n', ' ').replace('\r', ' ')
                .replaceAll("(?i)(\\[PIVOT\\])", "XPIVOT").trim();
        if (_originalQuery.endsWith(";")) {
            _originalQuery = _originalQuery.substring(0, _originalQuery.length() - 1);
        }
        Matcher mtc = PIVOT_PATT.matcher(_originalQuery);
        if (mtc.groupCount() < 4) {
            return false;
        }
        if (mtc.matches()) {
            transform = mtc.group(1);
            Matcher aggr = PIVOT_AGGR_PATT.matcher(transform);
            if (aggr.find()) {
                if (aggr.groupCount() < 2) {
                    return false;
                }
                aggregateFun = aggr.group(1);
                expression = aggr.group(2);
            } else {
                return false;
            }
            select = mtc.group(2);
            from = mtc.group(3);
            String pe = mtc.group(4);
            Matcher mtcExpr = PIVOT_EXPR_PATT.matcher(pe);
            if (mtcExpr.find()) {
                if (mtcExpr.groupCount() < 2) {
                    return false;
                }
                pivot = mtcExpr.group(1);
                pivotIn = Arrays.asList(mtcExpr.group(2).split(","));
            } else {
                pivot = pe;
            }
            return true;
        } else {
            return false;
        }
    }

    private void appendCaseWhen(StringBuffer sb, String condition, String cn) {
        sb.append(aggregateFun).append("(CASE WHEN ").append(condition).append(" THEN ").append(expression)
                .append(" END) AS ").append(cn);
    }

    public String verifySQL() {
        StringBuilder sb = new StringBuilder();
        String[] fromS = from.split(PIVOT_GROUP_BY);
        sb.append("SELECT DISTINCT ").append(pivot).append(" AS PIVOT ");
        sb.append(" FROM ").append(fromS[0]).append(" GROUP BY ").append(pivot).append(",").append(fromS[1]);
        return SQLConverter.convertSQL(sb.toString()).getSql();
    }

    public boolean prepare() {
        if (pivotInCondition) {
            pivotIn = new ArrayList<>();
        }
        return Try.withResources(() -> conn.createStatement(), st -> {
            ResultSet rs = st.executeQuery(verifySQL());
            int i = 0;
            while (rs.next()) {
                String frm = format(rs.getObject(PIVOT));
                if (frm != null) {
                    pivotIn.add(frm);
                }
                i++;
                if (i > 1000) {
                    return false;
                }
            }
            return true;
        }).orElse(false);
    }

    private String format(Object cln) {
        if (cln == null) {
            return null;
        } else if (cln instanceof Date) {
            SimpleDateFormat sdf = new SimpleDateFormat("#MM/dd/yyyy HH:mm:ss#");
            String clns = sdf.format((Date) cln);
            if (clns.endsWith(" 00:00:00#")) {
                clns = clns.replaceAll(" 00:00:00", "");
            }
            return clns;
        } else if (cln instanceof String) {
            return "'" + cln.toString().replaceAll("'", "''") + "'";
        }
        return cln.toString();
    }

    private String replaceQuotation(String cn) {
        cn = cn.replaceAll("\n", " ").replaceAll("\r", " ");
        Matcher dcm = PIVOT_CN_PATT.matcher(cn);

        if (dcm.matches()) {
            cn = dcm.group(1);
        }

        cn = cn.replaceAll("'", "").replaceAll("\"", "");

        return "[" + cn + "]";
    }

    public String toSQL(String name) {
        if (pivotIn == null) {
            if (name != null && PREPARE_MAP.containsKey(name)) {
                getPrepareFromCache(name);
            } else if (prepare()) {
                cachePrepare(name);
            } else {
                return null;
            }
            pivotInCondition = false;
        }
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        sb.append(select);
        for (String s : pivotIn) {
            sb.append(",");
            appendCaseWhen(sb, pivot + "=" + s, replaceQuotation(s));
        }
        sb.append(" FROM ").append(from);

        return sb.toString();
    }
}
