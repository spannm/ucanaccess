package net.ucanaccess.converters;

import static net.ucanaccess.type.SqlConstants.COLUMN_NAME;

import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Try;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DFunction {
    private static final Pattern      FROM_PATTERN             = Pattern.compile("\\w*(?i)FROM\\w*");
    private static final String       SELECT_FROM              = "(?i)SELECT(.*\\W)(?i)FROM(.*)";
    private static final String       DFUNCTIONS_WHERE         =
            "(?i)_[\\s\n\r]*\\([\\s\n\r]*['\"](.*)['\"]\\,[\\s\n\r]*['\"](.*)['\"]\\,[\\s\n\r]*['\"](.*)['\"][\\s\n\r]*\\)";
    private static final String       DFUNCTIONS_WHERE_DYNAMIC =
            "(?i)_[\\s\n\r]*\\([\\s\n\r]*['\"](.*)['\"]\\,[\\s\n\r]*['\"](.*)['\"]\\,(.*)\\)";
    private static final String       DFUNCTIONS_NO_WHERE      =
            "(?i)_[\\s\n\r]*\\([\\s\n\r]*['\"](.*)['\"]\\,[\\s\n\r]*['\"](.*)['\"][\\s\n\r]*\\)";
    private static final String       IDENTIFIER               = "(\\W)((?i)_)(\\W)";
    private static final List<String> DFUNCTIONLIST            =
            List.of("COUNT", "MAX", "MIN", "SUM", "AVG", "LAST", "FIRST", "LOOKUP");

    private Connection                conn;
    private String                    sql;

    public DFunction(Connection _conn, String _sql) {
        conn = _conn;
        sql = _sql;
    }

    private String convertDFunctions() {
        String sql0 = sql;
        try {
            boolean hasFrom = FROM_PATTERN.matcher(sql).find();
            String init = hasFrom ? " (SELECT " : "";
            String end = hasFrom ? " ) " : "";
            for (String s : DFUNCTIONLIST) {

                String fun = "D" + s;
                s = "lookup".equalsIgnoreCase(s) ? " " : s;
                sql0 = sql0.replaceAll(DFUNCTIONS_WHERE.replaceFirst("_", fun),
                    init + s + "($1) FROM $2 WHERE $3     " + end);
                sql0 = sql0.replaceAll(DFUNCTIONS_NO_WHERE.replaceFirst("_", fun), init + s + "($1) FROM $2    " + end);
                Pattern dfd = Pattern.compile(DFUNCTIONS_WHERE_DYNAMIC.replaceFirst("_", fun));
                for (Matcher mtc = dfd.matcher(sql0); mtc.find(); mtc = dfd.matcher(sql0)) {
                    StringBuilder sb = new StringBuilder();
                    String g3 = mtc.group(3);
                    String tableN = mtc.group(2).trim();
                    String alias = tableN.startsWith("[") && tableN.endsWith("]") ? "[" + unpad(tableN) + "_DALIAS]"
                        : tableN + "_DALIAS";
                    String tn = tableN.startsWith("[") && tableN.endsWith("]") ? unpad(tableN) : tableN;
                    sb.append(init).append(s).append("(").append(mtc.group(1)).append(") FROM ").append(tableN)
                        .append(" AS ").append(alias).append(" WHERE ");
                    boolean accessConcat = g3.indexOf('&') > 0;
                    boolean sqlConcat = g3.indexOf("||") > 0;
                    if (accessConcat || sqlConcat) {
                        String concat = accessConcat ? "&" : Pattern.quote("||");
                        String[] pts = g3.split(concat, -1);
                        for (String tkn : pts) {
                            if (isQuoted(tkn)) {
                                tkn = tkn.trim();
                                sb.append(unpad(tkn));
                            } else {
                                tkn += " ";
                                for (String cln : getColumnNames(tn.toUpperCase())) {
                                    String oppn = IDENTIFIER.replaceFirst("_", cln);
                                    Pattern op = Pattern.compile(oppn);
                                    Matcher mtcop = op.matcher(tkn);
                                    if (!mtcop.find()) {
                                        continue;
                                    }
                                    String pref = mtcop.group(1);
                                    if (".".equals(pref) || "[".equals(pref) && mtcop.start(1) > 0
                                        && tkn.charAt(mtcop.start(1) - 1) == '.') {
                                        continue;
                                    }
                                    tkn = tkn.replaceAll(oppn,
                                        "[".equals(pref) ? resolveAmbiguosTableName(cln) + ".$1$2$3"
                                            : "$1" + resolveAmbiguosTableName(cln) + ".$2$3");
                                }
                                sb.append(tkn);
                            }
                        }
                    }
                    sb.append(end);
                    sql0 = sql0.replaceFirst(DFUNCTIONS_WHERE_DYNAMIC.replaceFirst("_", fun), sb.toString());
                }
            }
        } catch (SQLException _ignored) {
        }
        return sql0;
    }

    private String resolveAmbiguosTableName(String _identifier) {
        return Try.withResources(conn::createStatement, st -> {
            String f4t = SQLConverter.convertSQL(
                sql.replaceAll("[\r\n]", " ").replaceFirst(SELECT_FROM, "SELECT " + _identifier + " FROM $2 ")).getSql();
            ResultSetMetaData rsmd = st.executeQuery(f4t).getMetaData();
            String tableN = rsmd.getTableName(1);
            return tableN == null || tableN.isBlank() ? _identifier : tableN;
        }).orElse(_identifier);
    }

    private List<String> getColumnNames(String tableName) throws SQLException {
        List<String> ar = new ArrayList<>();
        if (conn == null) {
            UcanaccessConnection conu = UcanaccessConnection.getCtxConnection();
            if (conu == null) {
                return ar;
            }
            conn = conu.getHSQLDBConnection();
        }
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getColumns(null, null, tableName, null);
        while (rs.next()) {
            ar.add(rs.getString(COLUMN_NAME));
        }
        return ar;
    }

    private static boolean isQuoted(String g3) {
        g3 = g3.trim();
        return g3.startsWith("'") && g3.endsWith("'") && g3.substring(1, g3.length() - 1).indexOf('\'') < 0
                || g3.startsWith("\"") && g3.endsWith("\"") && g3.substring(1, g3.length() - 1).indexOf('"') < 0;
    }

    private static String unpad(String tkn) {
        return tkn.substring(1, tkn.length() - 1);
    }

    public String toSQL() {
        return convertDFunctions();
    }

    public static String convertDFunctions(String sql, Connection conu) {
        return new DFunction(conu, sql).toSQL();
    }
}
