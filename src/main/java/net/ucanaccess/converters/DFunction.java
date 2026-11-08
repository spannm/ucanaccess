package net.ucanaccess.converters;

import static net.ucanaccess.type.SqlConstants.COLUMN_NAME;

import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Try;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DFunction {
    private static final Pattern             PAT_SELECT_FROM = Pattern.compile("SELECT(.*\\W)FROM(.*)", Pattern.CASE_INSENSITIVE);
    /** Name of a domain function followed by its opening parenthesis; longer names come first, like DStDevP. */
    private static final Pattern             PAT_DFUNCTION   = Pattern.compile(
        "\\bD(COUNT|MAX|MIN|SUM|AVG|LAST|FIRST|LOOKUP|STDEVP|STDEV|VARP|VAR)\\s*\\(", Pattern.CASE_INSENSITIVE);
    /** Aggregates of HSQLDB for the domain functions whose name differs from the Access aggregate. */
    private static final Map<String, String> AGGREGATES      = Map.of("LOOKUP", " ", "STDEV", "STDDEV_SAMP",
        "STDEVP", "STDDEV_POP", "VAR", "VAR_SAMP", "VARP", "VAR_POP");

    private Connection                       conn;
    private final String                     sql;

    public DFunction(Connection conn, String sql) {
        this.conn = conn;
        this.sql = sql;
    }

    /**
     * Replaces each domain function by a subquery. The expression and the domain must be text literals; the criterion
     * is a text literal or text joined with &amp;, whose other parts refer to the row of the outer query. A SELECT
     * without a FROM clause gets FROM DUAL, as HSQLDB needs one.
     *
     * @return converted SQL
     */
    private String convertDFunctions() {
        String out = sql;
        Matcher m = PAT_DFUNCTION.matcher(out);
        int pos = 0;
        while (m.find(pos)) {
            boolean[] quoted = SQLConverter.quotedPositions(out);
            int close = quoted[m.start()] ? -1 : closingParenthesis(out, quoted, m.end() - 1);
            String query = close < 0 ? null
                : domainQuery(m.group(1).toUpperCase(Locale.ROOT), splitArguments(out.substring(m.end(), close)));
            if (query == null) {
                pos = m.end();
                continue;
            }
            String replacement = " (SELECT " + query + ") ";
            out = out.substring(0, m.start()) + replacement + out.substring(close + 1);
            pos = m.start() + replacement.length();
            m = PAT_DFUNCTION.matcher(out);
        }
        // HSQLDB needs a FROM clause around the subqueries
        String upper = sql.toUpperCase(Locale.ROOT);
        if (!out.equals(sql) && upper.trim().startsWith("SELECT") && !upper.contains(" FROM ")) {
            out = out.trim() + " FROM DUAL";
        }
        return out;
    }

    /**
     * Builds the subquery of a domain function.
     *
     * @param name name of the function without the leading D, in upper case
     * @param args arguments: expression, domain and optionally the criterion
     * @return subquery without parentheses, {@code null} if the arguments are not supported
     */
    private String domainQuery(String name, List<String> args) {
        String expr = args.isEmpty() ? null : literal(args.get(0));
        String table = args.size() < 2 ? null : literal(args.get(1));
        if (expr == null || table == null || args.size() > 3) {
            return null;
        }
        StringBuilder sb = new StringBuilder(AGGREGATES.getOrDefault(name, name)).append('(').append(expr).append(") FROM ")
            .append(table);
        if (args.size() == 3) {
            String criterion = literal(args.get(2));
            if (criterion == null) {
                String tn = table.startsWith("[") && table.endsWith("]") ? unpad(table) : table;
                criterion = dynamicCriterion(args.get(2), tn);
                if (criterion == null) {
                    return null;
                }
                sb.append(" AS ").append(table.startsWith("[") && table.endsWith("]") ? "[" + tn + "_DALIAS]" : table + "_DALIAS");
            }
            sb.append(" WHERE ").append(criterion);
        }
        return sb.toString();
    }

    /**
     * Translates a criterion built with &amp; from text and expressions of the outer row. An expression between quotes
     * or # signs, as in {@code "v='" & v & "'"}, is a value, so the quotes are left out: {@code v= t.v}.
     *
     * @param criterion criterion as written in the domain function
     * @param tableName table of the domain, whose column names in the expressions are qualified with the outer table
     * @return criterion, {@code null} if it has no text part
     */
    private String dynamicCriterion(String criterion, String tableName) {
        List<String> parts = splitConcatenation(criterion);
        List<String> texts = new ArrayList<>();
        for (String part : parts) {
            texts.add(literal(part));
        }
        if (texts.stream().allMatch(Objects::isNull)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        boolean stripQuote = false;
        for (int i = 0; i < parts.size(); i++) {
            String text = texts.get(i);
            if (text != null) {
                sb.append(stripQuote ? text.substring(1) : text);
                stripQuote = false;
                continue;
            }
            String next = i + 1 < texts.size() ? texts.get(i + 1) : null;
            char last = sb.length() == 0 ? 0 : sb.charAt(sb.length() - 1);
            if ((last == '\'' || last == '#') && next != null && !next.isEmpty() && next.charAt(0) == last) {
                sb.setLength(sb.length() - 1);
                stripQuote = true;
            }
            sb.append(' ').append(qualify(parts.get(i), tableName)).append(' ');
        }
        return sb.toString();
    }

    /**
     * Qualifies the column names of the domain table in an expression with the table of the outer query, so they refer
     * to the outer row and not to the aliased domain table.
     *
     * @param expression expression of the outer row
     * @param tableName  table of the domain
     * @return qualified expression
     */
    private String qualify(String expression, String tableName) {
        String tkn = " " + expression + " ";
        try {
            for (String cln : getColumnNames(tableName.toUpperCase(Locale.ROOT))) {
                Pattern patOppn = Pattern.compile("(\\W)(" + cln + ")(\\W)", Pattern.CASE_INSENSITIVE);
                Matcher mtcop = patOppn.matcher(tkn);
                if (!mtcop.find()) {
                    continue;
                }
                String pref = mtcop.group(1);
                if (".".equals(pref) || ("[".equals(pref) && mtcop.start(1) > 0 && tkn.charAt(mtcop.start(1) - 1) == '.')) {
                    continue;
                }
                tkn = tkn.replaceAll("(\\W)((?i)" + cln + ")(\\W)",
                    "[".equals(pref) ? resolveAmbiguosTableName(cln) + ".$1$2$3" : "$1" + resolveAmbiguosTableName(cln) + ".$2$3");
            }
        } catch (SQLException ignored) {
            // metadata not available, keep the expression unchanged
        }
        return tkn.trim();
    }

    /**
     * Returns the content of a text literal in single quotes, with doubled quotes turned into single ones.
     *
     * @param arg argument
     * @return content, {@code null} if the argument is not exactly one text literal
     */
    private static String literal(String arg) {
        String s = arg.trim();
        if (s.length() < 2 || s.charAt(0) != '\'' || s.charAt(s.length() - 1) != '\'') {
            return null;
        }
        String content = s.substring(1, s.length() - 1);
        return content.replace("''", "").indexOf('\'') < 0 ? content.replace("''", "'") : null;
    }

    /**
     * Finds the parenthesis that closes the one at a position, skipping quoted text and nested parentheses.
     *
     * @param s      SQL string
     * @param quoted quoted positions, see {@link SQLConverter#quotedPositions(String)}
     * @param open   position of the opening parenthesis
     * @return position of the closing parenthesis, -1 if there is none
     */
    private static int closingParenthesis(String s, boolean[] quoted, int open) {
        int depth = 0;
        for (int i = open; i < s.length(); i++) {
            if (!quoted[i] && s.charAt(i) == '(') {
                depth++;
            } else if (!quoted[i] && s.charAt(i) == ')' && --depth == 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Splits the arguments of a function call at the commas outside quotes and nested parentheses.
     *
     * @param args text between the parentheses of the call
     * @return arguments, not trimmed
     */
    private static List<String> splitArguments(String args) {
        return split(args, ",");
    }

    /**
     * Splits a criterion at the concatenation operators &amp; or, if there is none, || outside quotes and parentheses.
     *
     * @param criterion criterion
     * @return parts, trimmed
     */
    private static List<String> splitConcatenation(String criterion) {
        List<String> parts = split(criterion, "&");
        if (parts.size() == 1) {
            parts = split(criterion, "||");
        }
        parts.replaceAll(String::trim);
        return parts;
    }

    /**
     * Splits a text at a separator outside quotes and parentheses.
     *
     * @param s         text
     * @param separator separator
     * @return parts
     */
    private static List<String> split(String s, String separator) {
        boolean[] quoted = SQLConverter.quotedPositions(s);
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!quoted[i] && c == '(') {
                depth++;
            } else if (!quoted[i] && c == ')') {
                depth--;
            } else if (!quoted[i] && depth == 0 && s.startsWith(separator, i)) {
                parts.add(s.substring(start, i));
                start = i + separator.length();
                i = start - 1;
            }
        }
        parts.add(s.substring(start));
        return parts;
    }

    private String resolveAmbiguosTableName(String identifier) {
        return Try.withResources(conn::createStatement, st -> {
            String sqlOut = sql.replaceAll("[\\r\\n]+", " ");
            sqlOut = PAT_SELECT_FROM.matcher(sqlOut).replaceFirst("SELECT " + identifier + " FROM $2");
            String f4t = SQLConverter.convertSQL(sqlOut).getSql();
            ResultSetMetaData rsmd = st.executeQuery(f4t).getMetaData();
            String tableN = rsmd.getTableName(1);
            return tableN == null || tableN.isBlank() ? identifier : tableN;
        }).orElse(identifier);
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
