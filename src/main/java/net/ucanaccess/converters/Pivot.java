package net.ucanaccess.converters;

import static net.ucanaccess.type.SqlConstants.PIVOT;

import net.ucanaccess.jdbc.NormalizedSQL;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Try;

import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Pivot {
    @SuppressWarnings("java:S5852")
    private static final Pattern                   PAT_PIVOT          = Pattern.compile("TRANSFORM(.*\\W)SELECT(.*\\W)FROM(.*\\W)PIVOT(.*)", Pattern.CASE_INSENSITIVE);
    private static final Pattern                   PAT_PIVOT_EXPR     = Pattern.compile("(.*)IN\\s*\\((.*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern                   PAT_PIVOT_AGGR     = Pattern.compile("(SUM|MAX|MIN|FIRST|LAST|AVG|COUNT|STDEV|VAR)\\s*\\((.*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern                   PAT_PIVOT_CN       = Pattern.compile("[\"'#](.*)[\"'#]");
    private static final String                    PAT_PIVOT_GROUP_BY = "(?i)GROUP\\s*(?i)BY";

    private static final Map<String, String>       PIVOT_MAP          = new HashMap<>();
    private static final Map<String, List<String>> PREPARE_MAP        = new HashMap<>();

    private String                                 transform;
    private String                                 select;
    private String                                 from;
    private String                                 expression;
    private String                                 pivotStr;
    private List<String>                           pivotIn;
    private String                                 aggregateFun;
    private final Connection                       conn;
    private boolean                                pivotInCondition   = true;
    private String                                 originalQuery;

    public Pivot(Connection _conn) {
        conn = _conn;
    }

    private void cachePrepare(String _name) {
        Optional.ofNullable(pivotIn).ifPresent(p -> PREPARE_MAP.put(_name, p));
    }

    public static void clearPrepared() {
        PREPARE_MAP.clear();

    }

    private void prepareGetFromCache(String _name) {
        pivotIn = PREPARE_MAP.getOrDefault(_name, pivotIn);
    }

    public void registerPivot(String _name) {
        if (!pivotInCondition) {
            PIVOT_MAP.put(_name, originalQuery);
        }
    }

    public static void checkAndRefreshPivot(String _currSql, UcanaccessConnection _conn) {

        for (String name : PIVOT_MAP.keySet()) {
            Pattern pat = Pattern.compile("(\\W)(?i)" + name + "(\\W)");
            Matcher mtc = pat.matcher(_currSql);
            if (mtc.find()) {
                try {
                    if (_conn == null && UcanaccessConnection.hasContext()) {
                        _conn = UcanaccessConnection.getCtxConnection();
                    }
                    if (_conn == null) {
                        return;
                    }
                    Connection connHsql = _conn.getHSQLDBConnection();
                    Pivot pivot = new Pivot(connHsql);

                    if (!pivot.parsePivot(PIVOT_MAP.get(name))) {
                        return;
                    }
                    String sqlh = pivot.toSQL(null);
                    if (sqlh == null) {
                        return;
                    }
                    try (Statement st = connHsql.createStatement()) {
                        String escqn = SQLConverter.completeEscaping(name, false);

                        st.executeUpdate(SQLConverter.convertSQL("DROP VIEW " + escqn, true).getSql());
                        NormalizedSQL nsql = SQLConverter.convertSQL("CREATE VIEW " + escqn + " AS " + sqlh, true);
                        Metadata mt = new Metadata(connHsql);
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
                    }
                } catch (Exception _ex) {
                    System.getLogger(Pivot.class.getName()).log(Level.WARNING, _ex.getMessage());
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
        Matcher mtc = PAT_PIVOT.matcher(_originalQuery);
        if (mtc.groupCount() < 4) {
            return false;
        }
        if (mtc.matches()) {
            transform = mtc.group(1);
            Matcher aggr = PAT_PIVOT_AGGR.matcher(transform);
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
            Matcher matcher = PAT_PIVOT_EXPR.matcher(pe);
            if (matcher.find()) {
                if (matcher.groupCount() < 2) {
                    return false;
                }
                pivotStr = matcher.group(1);
                pivotIn = Arrays.asList(matcher.group(2).split(","));
            } else {
                pivotStr = pe;
            }
            return true;
        } else {
            return false;
        }
    }

    private void appendCaseWhen(StringBuilder _sb, String _condition, String _cn) {
        _sb.append(aggregateFun).append("(CASE WHEN ").append(_condition).append(" THEN ").append(expression)
                .append(" END) AS ").append(_cn);
    }

    public String verifySQL() {
        StringBuilder sb = new StringBuilder();
        String[] fromS = from.split(PAT_PIVOT_GROUP_BY);
        sb.append("SELECT DISTINCT ").append(pivotStr).append(" AS PIVOT ")
          .append(" FROM ").append(fromS[0]).append(" GROUP BY ").append(pivotStr).append(",").append(fromS[1]);
        return SQLConverter.convertSQL(sb.toString()).getSql();
    }

    public boolean prepare() {
        if (pivotInCondition) {
            pivotIn = new ArrayList<>();
        }
        return Try.withResources(conn::createStatement, st -> {
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
                clns = clns.replace(" 00:00:00", "");
            }
            return clns;
        } else if (cln instanceof String) {
            return "'" + cln.toString().replace("'", "''") + "'";
        }
        return cln.toString();
    }

    private String replaceQuotation(String cn) {
        cn = cn.replaceAll("[\n\\r]", " ");
        Matcher dcm = PAT_PIVOT_CN.matcher(cn);

        if (dcm.matches()) {
            cn = dcm.group(1);
        }

        cn = cn.replace("'", "").replace("\"", "");

        return "[" + cn + "]";
    }

    public String toSQL(String name) {
        if (pivotIn == null) {
            if (name != null && PREPARE_MAP.containsKey(name)) {
                prepareGetFromCache(name);
            } else if (prepare()) {
                cachePrepare(name);
            } else {
                return null;
            }
            pivotInCondition = false;
        }
        StringBuilder sb = new StringBuilder()
          .append("SELECT ")
          .append(select);
        for (String s : pivotIn) {
            sb.append(",");
            appendCaseWhen(sb, pivotStr + "=" + s, replaceQuotation(s));
        }
        sb.append(" FROM ").append(from);

        return sb.toString();
    }
}
