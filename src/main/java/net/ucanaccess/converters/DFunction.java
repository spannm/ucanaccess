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
    private static final Pattern      PAT_SELECT_FROM          = Pattern.compile("SELECT(.*\\W)FROM(.*)", Pattern.CASE_INSENSITIVE);
    private static final String       DFUNCTIONS_WHERE         = "\\s*\\(\\s*['\"](.*)['\"]\\,\\s*['\"](.*)['\"]\\,\\s*['\"](.*)['\"]\\s*\\)";
    private static final String       DFUNCTIONS_WHERE_DYNAMIC = "\\s*\\(\\s*['\"](.*)['\"]\\,\\s*['\"](.*)['\"]\\,(.*)\\)";
    private static final String       DFUNCTIONS_NO_WHERE      = "\\s*\\(\\s*['\"](.*)['\"]\\,\\s*['\"](.*)['\"]\\s*\\)";
    private static final List<String> DFUNCTION_LIST           = List.of("COUNT", "MAX", "MIN", "SUM", "AVG", "LAST", "FIRST", "LOOKUP");

    private Connection                conn;
    private final String              sql;

    public DFunction(Connection _conn, String _sql) {
        conn = _conn;
        sql = _sql;
    }

    private String convertDFunctions() {
        String sqlOut = sql;
        boolean hasFrom = sql.toUpperCase().contains(" FROM ");

        String init = hasFrom ? " (SELECT " : "";
        String end = hasFrom ? " ) " : "";
        for (String f : DFUNCTION_LIST) {

            String dfun = "D" + f;
            if ("lookup".equalsIgnoreCase(f)) {
                f = " ";
            }
            sqlOut = Pattern.compile(dfun + DFUNCTIONS_WHERE, Pattern.CASE_INSENSITIVE).matcher(sqlOut).replaceAll(init + f + "($1) FROM $2 WHERE $3" + end);
            sqlOut = Pattern.compile(dfun + DFUNCTIONS_NO_WHERE, Pattern.CASE_INSENSITIVE).matcher(sqlOut).replaceAll(init + f + "($1) FROM $2" + end);

            Pattern patDfd = Pattern.compile(dfun + DFUNCTIONS_WHERE_DYNAMIC, Pattern.CASE_INSENSITIVE);
            for (Matcher mtc = patDfd.matcher(sqlOut); mtc.find(); mtc = patDfd.matcher(sqlOut)) {
                StringBuilder sb = new StringBuilder();
                String g3 = mtc.group(3);
                String tableN = mtc.group(2).trim();
                String alias = tableN.startsWith("[") && tableN.endsWith("]") ? "[" + unpad(tableN) + "_DALIAS]" : tableN + "_DALIAS";
                String tn = tableN.startsWith("[") && tableN.endsWith("]") ? unpad(tableN) : tableN;
                sb.append(init).append(f).append("(").append(mtc.group(1)).append(") FROM ").append(tableN)
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
                            try {
                                for (String cln : getColumnNames(tn.toUpperCase())) {
                                    Pattern patOppn = Pattern.compile("(\\W)(" + cln + ")(\\W)", Pattern.CASE_INSENSITIVE);
                                    Matcher mtcop = patOppn.matcher(tkn);
                                    if (!mtcop.find()) {
                                        continue;
                                    }
                                    String pref = mtcop.group(1);
                                    if (".".equals(pref) || "[".equals(pref) && mtcop.start(1) > 0
                                        && tkn.charAt(mtcop.start(1) - 1) == '.') {
                                        continue;
                                    }
                                    tkn = tkn.replaceAll("(\\W)((?i)" + cln + ")(\\W)",
                                        "[".equals(pref) ? resolveAmbiguosTableName(cln) + ".$1$2$3"
                                            : "$1" + resolveAmbiguosTableName(cln) + ".$2$3");
                                }
                            } catch (SQLException _ignored) {
                            }
                            sb.append(tkn);
                        }
                    }
                }
                sb.append(end);
                sqlOut = sqlOut.replaceFirst(DFUNCTIONS_WHERE_DYNAMIC.replaceFirst("_", dfun), sb.toString());
            }
        }

        return sqlOut;
    }

    private String resolveAmbiguosTableName(String _identifier) {
        return Try.withResources(conn::createStatement, st -> {
            String sqlOut = sql.replaceAll("[\\r\\n]+", " ");
            sqlOut = PAT_SELECT_FROM.matcher(sqlOut).replaceFirst("SELECT " + _identifier + " FROM $2");
            String f4t = SQLConverter.convertSQL(sqlOut).getSql();
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
