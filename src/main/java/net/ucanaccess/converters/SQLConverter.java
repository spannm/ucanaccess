package net.ucanaccess.converters;

import com.healthmarketscience.jackcess.TableBuilder;
import net.ucanaccess.jdbc.NormalizedSQL;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SQLConverter {
    private static final Pattern QUOTE_S_PATTERN           = Pattern.compile("(')+");
    private static final Pattern DOUBLE_QUOTE_S_PATTERN    = Pattern.compile("(\")+");

    private static final Pattern SELECT_FROM_PATTERN_START = Pattern.compile("[\\s\n\r]*(?i)SELECT[\\s\n\r]+");
    private static final Pattern SELECT_FROM_PATTERN_END   = Pattern.compile("[\\s\n\r]*(?i)FROM[\\s\n\r\\[]+");
    private static final Pattern UNESCAPED_ALIAS           = Pattern.compile("[\\s\n\r]*(?i)AS[\\s\n\r]*");

    private static final Pattern QUOTE_M_PATTERN           = Pattern.compile("'(([^'])*)'");
    private static final Pattern DOUBLE_QUOTE_M_PATTERN    = Pattern.compile("\"(([^\"])*)\"");
    private static final Pattern FIND_LIKE_PATTERN         = Pattern
            .compile("[\\s\n\r\\(]*([\\w\\.]*)([\\s\n\r\\)]*)((?i)NOT[\\s\n\r]*)*(?i)LIKE[\\s\n\r]*'([^']*(?:'')*)'");
    private static final Pattern ACCESS_LIKE_CHARINTERVAL_PATTERN =
            Pattern.compile("\\[(?:\\!*[a-zA-Z0-9]\\-[a-zA-Z0-9])+\\]");
    private static final Pattern ACCESS_LIKE_ESCAPE_PATTERN       = Pattern.compile("\\[[\\*|_|#]\\]");
    private static final Pattern CHECK_DDL                        =
            Pattern.compile("^([\n\r\\s]*(?i)(create|alter|drop|enable|disable))[\n\r\\s]+.*");
    private static final Pattern KIND_OF_SUBQUERY                 =
            Pattern.compile("(\\[)(((?i) FROM )*((?i)SELECT )*([^\\]])*)(\\]\\.[\\s\n\r])");

    private static final Pattern NO_DATA_PATTERN      = Pattern.compile(" (?i)WITH[\\s\n\r]+(?i)NO[\\s\n\r]+(?i)DATA");
    private static final Pattern NO_ALFANUMERIC       = Pattern.compile("\\W");
    private static final String  IDENTITY             = "(\\W+)((?i)@@identity)(\\W*)";
    private static final Pattern SELECT_IDENTITY      = Pattern.compile("(?i)select[\\s\n\r]+(?i)@@identity.*");
    private static final Pattern HAS_FROM             = Pattern.compile("[\\s\n\r]+(?i)from[\\s\n\r]+");
    private static final Pattern FORMULA_DEPENDENCIES = Pattern.compile("\\[([^\\]]*)\\]");
    private static final String  EXCLAMATION_POINT    = "(\\!)([\n\r\\s]*)([^\\=])";

    private static final String   YES                        = "(\\W)((?i)YES)(\\W)";
    private static final String   NO                         = "(\\W)((?i)NO)(\\W)";
    private static final String   WITH_OWNERACCESS_OPTION    =
            "(\\W)(?i)WITH[\\s\n\r]+(?i)OWNERACCESS[\\s\n\r]+(?i)OPTION(\\W)";
    private static final Pattern  DIGIT_STARTING_IDENTIFIERS =
            Pattern.compile("(\\W)(([0-9])+(([_a-zA-Z])+([0-9])*)+)(\\W)");
    private static final String   UNDERSCORE_IDENTIFIERS     = "(\\W)((_)+([_a-zA-Z0-9])+)(\\W)";
    private static final String   XESCAPED                   = "(\\W)((?i)X)((?i)_)(\\W)";
    private static final String[] DEFAULT_CATCH              = new String[] {
        "([\\s\n\r]*(?i)DEFAULT[\\s\n\r]+)('(?:[^']*(?:'')*)*')([\\s\n\r\\)\\,])",
        "([\\s\n\r]*(?i)DEFAULT[\\s\n\r]+)(\"(?:[^\"]*(?:\"\")*)*\")([\\s\n\r\\)\\,])",
        "([\\s\n\r]*(?i)DEFAULT[\\s\n\r]+)([0-9\\.\\-\\+]+)([\\s\n\r\\)\\,])",
        "([\\s\n\r]*(?i)DEFAULT[\\s\n\r]+)([_0-9a-zA-Z]*\\([^\\)]*\\))([\\s\n\r\\)\\,])"};
    private static final Pattern  DEFAULT_CATCH_0            = Pattern.compile("([\\s\n\r]*(?i)DEFAULT[\\s\n\r]+)");
    public static final String    NOT_NULL                   = "[\\s\n\r](?i)NOT[\\s\n\r](?i)NULL";

    private static final Pattern QUOTED_ALIAS       =
            Pattern.compile("([\\s\n\r]+(?i)AS[\\s\n\r]*)(\\[[^\\]]*\\])(\\W)");
    private static final String  TYPES_TRANSLATE    = "(?i)_(\\W)";
    public static final String   DATE_ACCESS_FORMAT =
            "(0[1-9]|[1-9]|1[012])/(0[1-9]|[1-9]|[12][0-9]|3[01])/(\\d\\d\\d\\d)";

    public static final String  DATE_FORMAT          =
            "(\\d\\d\\d\\d)-(0[1-9]|[1-9]|1[012])-(0[1-9]|[1-9]|[12][0-9]|3[01])";
    public static final String  HHMMSS_ACCESS_FORMAT =
            "([0-9]|0[0-9]|1[0-9]|2[0-4]):([0-9]|[0-5][0-9]):([0-9]|[0-5][0-9])";
    public static final String  HHMMSS_FORMAT        =
            "([0-9]|0[0-9]|1[0-9]|2[0-4]):([0-9]|[0-5][0-9]):([0-5][0-9]|[0-9])";
    private static final String NAME_PATTERN         = "(([_a-zA-Z0-9])+|\\[([^\\]])*\\]|`([^`])*`)";
    private static final int    NAME_PATTERN_STEP    = 4;
    private static final String UNION                = "(;)([\\s\n\r]*)((?i)UNION)([\\s\n\r]*)";
    private static final String DISTINCT_ROW         = "[\\s\n\r]+(?i)DISTINCTROW[\\s\n\r]+";
    private static final String DEFAULT_VARCHAR      = "(\\W)(?i)VARCHAR([\\s\n\r,\\)])";

    private static final String                  DEFAULT_VARCHAR_0            = "(\\W)(?i)VARCHAR([^\\(])";
    private static final String                  BACKTRIK                     = "(`)([^`]*)(`)";
    private static final String                  DELETE_ALL                   =
            "((?i)DELETE[\\s\n\r]+)(\\*)([\\s\n\r]+(?i)FROM[\\s\n\r]+)";
    private static final String                  PARAMETERS                   = "(?i)PARAMETERS([^;]*);";
    private static final Pattern                 ESPRESSION_DIGIT             = Pattern.compile("([\\d]+)(?![\\.\\d])");
    private static final String                  BIG_BANG                     = "1899-12-30";
    private static final List<String>            KEYWORDLIST                  = List.of("ALL", "AND", "ANY",
        "ALTER", "AS", "AT", "AVG", "BETWEEN", "BOTH", "BY", "CALL", "CASE", "CAST", "CHECK", "COALESCE",
        "CORRESPONDING", "CONVERT", "COUNT", "CREATE", "CROSS", "DEFAULT", "DISTINCT", "DROP", "ELSE", "EVERY",
        "EXISTS", "EXCEPT", "FOR", "FOREIGN", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "IN", "INNER",
        "INTERSECT", "INTO", "IS", "JOIN", "LEFT", "LEADING", "LIKE", "MAX", "MIN", "NATURAL", "NOT", "NULLIF",
        "ON", "ORDER", "OR", "OUTER", "PRIMARY", "REFERENCES", "RIGHT", "SELECT", "SET", "SOME", "STDDEV_POP",
        "STDDEV_SAMP", "SUM", "TABLE", "THEN", "TO", "TRAILING", "TRIGGER", "UNION", "UNIQUE", "USING", "VALUES",
        "VAR_POP", "VAR_SAMP", "WHEN", "WHERE", "WITH", "END", "DO", "CONSTRAINT", "USER", "ROW");
    private static final String              KEYWORD_ALIAS                  = createKeywordAliasRegex();
    private static final List<String>        PROCEDURE_KEYWORD_LIST         = List.of("NEW", "ROW");
    private static final List<String>        WHITE_SPACED_TABLE_NAMES       = new ArrayList<>();
    private static final Set<String>         ESCAPED_IDENTIFIERS            = new HashSet<>();
    private static final Set<String>         ALREADY_ESCAPED_IDENTIFIERS    = new HashSet<>();
    private static final Map<String, String> IDENTIFIERS_CONTAINING_KEYWORD = new HashMap<>();
    private static final Set<String>         APOSTROPHISED_NAMES            = new HashSet<>();
    private static final Set<String>         WORKAROUND_FUNCTIONS           = new HashSet<>();

    private static boolean supportsAccessLike  = true;
    private static boolean dualUsedAsTableName = false;

    private SQLConverter() {
    }

    public static boolean hasIdentity(String sql) {
        return sql.indexOf("@@") > 0 && sql.toUpperCase(java.util.Locale.US).indexOf("@@IDENTITY") > 0;
    }

    private static String createKeywordAliasRegex() {
        List<String> keywordList = new ArrayList<>(KEYWORDLIST);
        keywordList.remove("SELECT");
        StringBuilder keywords = new StringBuilder();
        String sep = "";
        for (String s : keywordList) {
            keywords.append(sep).append(s);
            sep = "|";
        }
        return "([\\s\\n\\r]+AS[\\s\\n\\r]+)(" + keywords + ")(\\W)";
    }

    private static void aliases(String sql, NormalizedSQL nsql) {
        Matcher mtc = SELECT_FROM_PATTERN_START.matcher(sql);
        if (mtc.find()) {

            int init = mtc.end();
            sql = sql.substring(init);
            mtc = SELECT_FROM_PATTERN_END.matcher(sql);
            if (mtc.find()) {
                int end = mtc.start();
                sql = sql.substring(0, end);

                for (mtc = UNESCAPED_ALIAS.matcher(sql); mtc.find(); mtc = UNESCAPED_ALIAS.matcher(sql)) {
                    int e = mtc.end();

                    sql = sql.substring(e) + " ";
                    char[] sqlc = sql.toCharArray();
                    if (sqlc[0] == '[') {
                        continue;
                    }
                    StringBuilder sb = new StringBuilder();
                    for (char c : sqlc) {
                        if (c == ' ' || c == '\n' || c == '\r' || c == ',') {
                            String key = SQLConverter.preEscapingIdentifier(sb.toString());
                            nsql.put(key, sb.toString());
                            break;
                        } else {
                            sb.append(c);
                        }
                    }
                }

            }
        }
    }

    public static String preprocess(String sql, Object key) {

        Matcher mtc = SELECT_IDENTITY.matcher(sql);
        Matcher mtc1 = HAS_FROM.matcher(sql);
        String end = mtc.matches() && !mtc1.find() ? " FROM DUAL" : "";
        if (key instanceof String) {
            key = "'" + key + "'";
        }
        return sql.replaceAll(IDENTITY, "$1" + key + "$3") + end;
    }

    public static boolean isListedAsKeyword(String s) {
        return KEYWORDLIST.contains(s.toUpperCase());
    }

    private static int[] getQuoteGroup(String _s) {
        if (!_s.contains("''")) {
            Matcher mtc = QUOTE_M_PATTERN.matcher(_s);
            return mtc.find() ? new int[] {mtc.start(), mtc.end()} : null;
        } else {
            int[] ret = new int[] {-1, -1};
            Matcher mc = QUOTE_S_PATTERN.matcher(_s);
            while (mc.find()) {
                int start = mc.start();
                int end = mc.end();
                if ((end - start) % 2 == 0) {
                    if (ret[0] == -1) {
                        return new int[] {mc.start(), mc.end()};
                    }
                } else {
                    if (ret[0] == -1) {
                        ret[0] = mc.start();
                    } else {
                        ret[1] = mc.end();
                        return ret;
                    }
                }
            }
            return null;
        }
    }

    private static int[] getDoubleQuoteGroup(String _s) {
        if (!_s.contains("\"\"")) {
            Matcher mtc = DOUBLE_QUOTE_M_PATTERN.matcher(_s);
            return mtc.find() ? new int[] {mtc.start(), mtc.end()} : null;
        } else {
            int[] ret = new int[] {-1, -1};
            Matcher mc = DOUBLE_QUOTE_S_PATTERN.matcher(_s);
            while (mc.find()) {
                int start = mc.start();
                int end = mc.end();
                if ((end - start) % 2 == 0) {
                    if (ret[0] == -1) {
                        return new int[] {mc.start(), mc.end()};
                    }
                    continue;
                } else {
                    if (ret[0] == -1) {
                        ret[0] = mc.start();
                    } else {
                        ret[1] = mc.end();
                        return ret;
                    }
                }
            }
            return null;
        }
    }

    public enum DDLType {

        CREATE_TABLE_AS_SELECT(
                Pattern.compile("[\\s\n\r]*(?i)create[\\s\n\r]+(?i)table[\\s\n\r]+" + NAME_PATTERN
                        + "[\\s\n\r]*(?)AS[\\s\n\r]*\\(\\s*((?)SELECT)")),
        CREATE_TABLE(Pattern.compile("[\\s\n\r]*(?i)create[\\s\n\r]+(?i)table[\\s\n\r]+" + NAME_PATTERN)),
        DROP_TABLE_CASCADE(
                Pattern.compile(
                        "[\\s\n\r]*(?i)drop[\\s\n\r]+(?i)table[\\s\n\r]+" + NAME_PATTERN + "[\\s\n\r]+(?i)cascade")),

        DROP_TABLE(Pattern.compile("[\\s\n\r]*(?i)drop[\\s\n\r]+(?i)table[\\s\n\r]+" + NAME_PATTERN)),
        ALTER_RENAME(
                Pattern.compile("[\\s\n\r]*(?i)alter[\\s\n\r]+(?i)table[\\s\n\r]+" + NAME_PATTERN
                        + "[\\s\n\r]+(?i)rename[\\s\n\r]+(?i)to[\\s\n\r]+" + NAME_PATTERN)),
        CREATE_PRIMARY_KEY(
                Pattern.compile("[\\s\n\r]*(?i)alter[\\s\n\r]+(?i)table[\\s\n\r]+" + NAME_PATTERN
                        + "[\\s\n\r]+(?i)add[\\s\n\r]+(?:(?i)constraint[\\s\n\r]+" + NAME_PATTERN
                        + "[\\s\n\r]+)?(?i)primary[\\s\n\r]+(?i)key(.*)")),
        CREATE_FOREIGN_KEY(
                Pattern.compile("[\\s\n\r]*(?i)alter[\\s\n\r]+(?i)table[\\s\n\r]+" + NAME_PATTERN
                        + "[\\s\n\r]+(?i)add[\\s\n\r]+(?:(?i)constraint[\\s\n\r]+" + NAME_PATTERN
                        + "[\\s\n\r]+)?(?i)foreign[\\s\n\r]+(?i)key[\\s\n\r]+"
                        + "(?:\\(.*\\))[\\s\n\r]*(?i)references[\\s\n\r]+" + NAME_PATTERN + "(.*)")),
        DROP_FOREIGN_KEY(
                Pattern.compile("[\\s\n\r]*(?i)alter[\\s\n\r]+(?i)table[\\s\n\r]+" + NAME_PATTERN
                        + "[\\s\n\r]+(?i)drop[\\s\n\r]+(?i)constraint[\\s\n\r]+" + NAME_PATTERN)),

        ADD_COLUMN(
                Pattern.compile("[\\s\n\r]*(?i)alter[\\s\n\r]+(?i)table[\\s\n\r]+" + NAME_PATTERN
                        + "[\\s\n\r]+(?i)add[\\s\n\r]+(?:(?i)column[\\s\n\r]+)?" + NAME_PATTERN + "(.*)")),
        CREATE_INDEX(
                Pattern.compile("(?i)CREATE[\\s\n\r]+(?:(?i)unique)?[\\s\n\r]*(?i)index[\\s\n\r]+" + NAME_PATTERN
                        + "[\\s\n\r]+(?i)ON[\\s\n\r]+" + NAME_PATTERN + "[\\s\n\r]+")),

        DISABLE_AUTOINCREMENT(
                Pattern.compile(
                        "[\\s\n\r]*(?i)disable[\\s\n\r]+(?i)autoincrement[\\s\n\r]+(?i)on[\\s\n\r]*" + NAME_PATTERN)),

        ENABLE_AUTOINCREMENT(
                Pattern.compile(
                        "[\\s\n\r]*(?i)enable[\\s\n\r]+(?i)autoincrement[\\s\n\r]+(?i)on[\\s\n\r]*" + NAME_PATTERN));
        private Pattern pattern;
        private String  ddl;

        DDLType(Pattern _pattern) {
            pattern = _pattern;
        }

        public boolean in(DDLType... types) {
            for (DDLType type : types) {
                if (equals(type)) {
                    return true;
                }
            }
            return false;
        }

        public static DDLType getDDLType(String s) {
            DDLType[] dts = DDLType.values();
            for (DDLType cand : dts) {
                if (cand.pattern.matcher(s).find()) {
                    if (cand.equals(DDLType.DROP_TABLE_CASCADE)) {
                        return null;
                    }
                    cand.ddl = elab(s);
                    return cand;
                }
            }
            return null;
        }

        private static String elab(String s) {
            if (!s.contains("[") || !s.contains("]")) {
                return s;
            }
            return s.replaceAll("\\[([^\\]]*)\\]", " $0 ");
        }

        public String getDBObjectName() {
            Matcher m = pattern.matcher(ddl);
            if (m.find()) {
                return m.group(1);
            }
            return null;
        }

        public String getSecondDBObjectName() {
            Matcher m = pattern.matcher(ddl);
            if (m.find()) {
                return m.group(1 + NAME_PATTERN_STEP);
            }
            return null;
        }

        public String getThirdDBObjectName() {
            Matcher m = pattern.matcher(ddl);
            if (m.find()) {
                return m.group(1 + 2 * NAME_PATTERN_STEP);
            }
            return null;
        }

        public String getColumnDefinition() {
            Matcher m = pattern.matcher(ddl);
            if (m.find()) {
                return m.group(2 * NAME_PATTERN_STEP + 1);
            }
            return null;
        }

        public String getSelect(String s) {
            Matcher m = pattern.matcher(s);
            if (m.find()) {
                return s.substring(m.start(m.groupCount()), s.lastIndexOf(')'));
            }
            return null;
        }
    }

    static void addWAFunctionName(String name) {
        WORKAROUND_FUNCTIONS.add(name);
    }

    public static DDLType getDDLType(String s) {
        return DDLType.getDDLType(s);
    }

    private static String replaceWorkAroundFunctions(String sql) {

        for (String waFun : WORKAROUND_FUNCTIONS) {
            sql = sql.replaceAll("(\\W)(?i)" + waFun + "\\s*\\(", "$1" + waFun + "WA(");
        }
        sql = sql.replaceAll("(\\W)(?i)STDEV\\s*\\(", "$1STDDEV_SAMP(");
        sql = sql.replaceAll("(\\W)(?i)STDEVP\\s*\\(", "$1STDDEV_POP(");
        sql = sql.replaceAll("(\\W)(?i)VAR\\s*\\(", "$1VAR_SAMP(");
        sql = sql.replaceAll("(\\W)(?i)VARP\\s*\\(", "$1VAR_POP(");
        return sql.replaceAll("(\\W)(?i)currentUser\\s*\\(", "$1user(");
    }

    public static String restoreWorkAroundFunctions(String sql) {
        for (String waFun : WORKAROUND_FUNCTIONS) {
            sql = sql.replaceAll("(\\W)(?i)" + waFun + "WA\\s*\\(", "$1" + waFun + "(");
        }
        sql = sql.replaceAll("(\\W)(?i)STDDEV_SAMP\\s*\\(", "$1STDEV(");
        sql = sql.replaceAll("(\\W)(?i)STDDEV_POP\\s*\\(", "$1STDEVP(");
        sql = sql.replaceAll("(\\W)(?i)VAR_SAMP\\s*\\(", "$1VAR(");
        sql = sql.replaceAll("(\\W)(?i)VAR_POP\\s*\\(", "$1VARP(");
        return sql.replaceAll("(\\W)(?i)user\\s*\\(", "$1currentUser(");
    }

    private static String replaceBacktrik(String sql) {
        return sql.replaceAll(BACKTRIK, "[$2]");
    }

    private static String replaceAposNames(String sql) {
        for (String an : APOSTROPHISED_NAMES) {
            sql = sql.replaceAll("(?i)" + Pattern.quote("[" + an + "]"),
                    "[" + SQLConverter.basicEscapingIdentifier(an) + "]");
        }
        return sql;
    }

    public static NormalizedSQL convertSQL(String sql, boolean creatingQuery) {
        return convertSQL(sql, null, creatingQuery);
    }

    public static NormalizedSQL convertSQL(String sql, UcanaccessConnection conn, boolean creatingQuery) {
        NormalizedSQL nsql = new NormalizedSQL();
        sql = sql + " ";
        aliases(sql, nsql);
        sql = replaceBacktrik(sql);
        sql = replaceAposNames(sql);
        sql = convertUnion(sql);
        sql = convertAccessDate(sql);
        sql = convertQuotedAliases(sql, nsql);
        sql = escape(sql);
        sql = convertLike(sql);
        sql = replaceWhiteSpacedTables(sql);
        // sql = replaceExclamationPoints(sql);
        if (!creatingQuery) {
            Pivot.checkAndRefreshPivot(sql, conn);
            sql = DFunction.convertDFunctions(sql, conn);
        }
        sql = sql.trim();

        nsql.setSql(sql);

        return nsql;
    }

    private static String replaceExclamationPoints(String sql) {
        return sql.replaceAll(EXCLAMATION_POINT, ".$2$3");
    }

    private static String convertOwnerAccess(String sql) {
        return sql.replaceAll(WITH_OWNERACCESS_OPTION, "");
    }

    private static String convertDeleteAll(String sql) {
        return sql.replaceAll(DELETE_ALL, "$1$3");
    }

    private static String convertUnion(String sql) {
        return sql.replaceAll(UNION, "$2$3$4");
    }

    private static String convertYesNo(String sql) {
        sql = sql.replaceAll(YES, "$1true$3");
        Matcher mtc = NO_DATA_PATTERN.matcher(sql);
        if (mtc.find()) {
            sql = sql.substring(0, mtc.start()).replaceAll(NO, "$1false$3") + sql.substring(mtc.start());
        } else {
            sql = sql.replaceAll(NO, "$1false$3");
        }
        return sql;
    }

    private static String convertQuotedAliases(String sql, NormalizedSQL nsql) {
        for (Matcher mtc = KIND_OF_SUBQUERY.matcher(sql); mtc.find(); mtc = KIND_OF_SUBQUERY.matcher(sql)) {
            String g2 = mtc.group(2).trim();
            if (g2.endsWith(";")) {
                g2 = g2.substring(0, g2.length() - 1);
            }
            sql = sql.substring(0, mtc.start()) + "(" + g2 + ")" + sql.substring(mtc.end());
        }
        Set<String> hs = new HashSet<>();
        String sqle = sql;
        String sqlN = "";
        for (Matcher mtc = QUOTED_ALIAS.matcher(sqle); mtc.find(); mtc = QUOTED_ALIAS.matcher(sqle)) {
            String g2 = mtc.group(2);
            if (g2.indexOf('\'') >= 0 || g2.indexOf('"') >= 0) {
                hs.add(g2);
            }
            String value = g2.substring(1, g2.length() - 1);
            nsql.put(SQLConverter.preEscapingIdentifier(value), value);
            sqlN += sqle.substring(0, mtc.start()) + mtc.group(1) + g2.replaceAll("['\"]", "") + mtc.group(3);
            sqle = sqle.substring(mtc.end());
        }
        sql = sqlN + sqle;
        for (String escaped : hs) {
            sql = sql.replaceAll("\\[" + escaped.substring(1, escaped.length() - 1) + "\\]",
                    escaped.replaceAll("['\"]", ""));
        }
        return sql;
    }

    private static String replaceDistinctRow(String sql) {
        return sql.replaceAll(DISTINCT_ROW, " DISTINCT ");
    }

    static void addWhiteSpacedTableNames(String _name) {
        if (_name == null) {
            return;
        }
        String name = basicEscapingIdentifier(_name);
        if (name == null || WHITE_SPACED_TABLE_NAMES.contains(name)) {
            return;
        }
        for (String alrIn : WHITE_SPACED_TABLE_NAMES) {
            if (name.contains(alrIn)) {
                WHITE_SPACED_TABLE_NAMES.add(WHITE_SPACED_TABLE_NAMES.indexOf(alrIn), name);
                return;
            }
        }
        WHITE_SPACED_TABLE_NAMES.add(name);
    }

    public static NormalizedSQL convertSQL(String sql) {
        return convertSQL(sql, null, false);
    }

    public static NormalizedSQL convertSQL(String sql, UcanaccessConnection conn) {
        return convertSQL(sql, conn, false);
    }

    public static String convertAccessDate(String sql) {
        sql = sql.replaceAll("#" + DATE_ACCESS_FORMAT + "#", "Timestamp('$3-$1-$2 00:00:00')")
                // FORMAT MM/dd/yyyy
                .replaceAll("#" + DATE_ACCESS_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")#",
                        "Timestamp0('$3-$1-$2 $4')")

                .replaceAll("#" + DATE_ACCESS_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#",
                        "Timestamp0('$3-$1-$2 $4')")

                .replaceAll("#" + DATE_ACCESS_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#",
                        "(Timestamp0('$3-$1-$2 $4')+ 12 Hour) ")
                // FORMAT yyyy-MM-dd
                .replaceAll("#" + DATE_FORMAT + "#", "Timestamp0('$1-$2-$3 00:00:00')")

                .replaceAll("#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")#", "Timestamp0('$1-$2-$3 $4')")

                .replaceAll("#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#",
                        "Timestamp0('$1-$2-$3 $4')")

                .replaceAll("#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#",
                        "(Timestamp0('$1-$2-$3 $4')+ 12 Hour)")

                .replaceAll("#(" + HHMMSS_ACCESS_FORMAT + ")#", "Timestamp'" + BIG_BANG + " $1'")
                .replaceAll("#(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#", "Timestamp'" + BIG_BANG + " $1'")
                .replaceAll("#(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#", "(Timestamp'" + BIG_BANG + " $1'+ 12 Hour)");

        return sql;
    }

    private static String replaceWhiteSpacedTables(String sql) {
        String[] sqls = sql.split("'", -1);
        StringBuilder sb = new StringBuilder();
        String cm = "";
        for (int i = 0; i < sqls.length; ++i) {
            sb.append(cm).append(i % 2 == 0 ? replaceWhiteSpacedTables(sqls[i], "\"") : sqls[i]);
            cm = "'";
        }
        return sb.toString();
    }

    private static String replaceWhiteSpacedTables(String sql, String character) {
        String[] sqls = sql.split(character, -1);
        StringBuilder sb = new StringBuilder();
        String cm = "";
        for (int i = 0; i < sqls.length; ++i) {
            sb.append(cm).append(i % 2 == 0 ? replaceWhiteSpacedTableNames0(sqls[i]) : sqls[i]);
            cm = character;
        }
        return sb.toString();
    }

    private static String replaceWhiteSpacedTableNames0(String sql) {
        if (WHITE_SPACED_TABLE_NAMES.isEmpty()) {
            return sql;
        }
        StringBuilder sb = new StringBuilder("(");
        String or = "";
        for (String bst : WHITE_SPACED_TABLE_NAMES) {
            sb.append(or).append("(?i)").append(Pattern.quote(bst));
            or = "|";
        }
        // workaround o.o. and l.o.
        for (String bst : WHITE_SPACED_TABLE_NAMES) {
            String dw = bst.replaceAll(" ", "  ");
            sql = sql.replaceAll(Pattern.quote(dw), bst);
        }
        sb.append(")");
        sql = sql.replaceAll("([^A-Za-z0-9\"])" + sb + "([^A-Za-z0-9\"])", " $1\"$2\"$3");
        return sql;
    }

    private static String convertIdentifiers(String _sql) {
        int init = _sql.indexOf('[');
        if (init != -1) {
            int end = _sql.indexOf(']');
            if (end < init) {
                return convertResidualSQL(_sql);
            }
            String content = _sql.substring(init + 1, end);
            if (content.indexOf(' ') > 0) {
                String tryContent = " " + content + " ";
                String tryConversion = convertXescaped(tryContent);
                if (!tryConversion.equalsIgnoreCase(tryContent)) {
                    IDENTIFIERS_CONTAINING_KEYWORD.put(tryConversion.trim(), content.toUpperCase());
                }
            }
            boolean isKeyword = KEYWORDLIST.contains(content.toUpperCase());

            content = basicEscapingIdentifier(content).toUpperCase();
            String subs = " ";
            if (content != null && !isKeyword && (content.indexOf(' ') > 0 || NO_ALFANUMERIC.matcher(content).find())) {
                subs = "\"";
            }
            _sql = convertResidualSQL(_sql.substring(0, init)) + subs + content + subs
                    + convertIdentifiers(_sql.substring(end + 1));
        } else {
            _sql = convertResidualSQL(_sql);
        }
        return _sql;
    }

    private static String convertResidualSQL(String sql) {
        sql = convertSQLTokens(sql);
        if ("!".equals(sql)) {
            return ".";
        }
        return replaceExclamationPoints(
                replaceDigitStartingIdentifiers(sql.replaceAll(UNDERSCORE_IDENTIFIERS, "$1Z$2$5")));
    }

    private static String convertSQLTokens(String sql) {
        return convertDeleteAll(replaceWorkAroundFunctions(
                convertOwnerAccess(replaceDistinctRow(convertYesNo(sql.replaceAll("&", "||"))))));
    }

    private static String replaceDigitStartingIdentifiers(String sql) {

        Matcher mtc = DIGIT_STARTING_IDENTIFIERS.matcher(sql);
        if (mtc.find()) {
            if (Character.isLetter(mtc.group(0).charAt(0))) {
                return sql;
            }
            String prefix =
                    mtc.group(0).matches("\\.([0-9])+[Ee]([0-9])+\\s") || mtc.group(0).matches("\\.([0-9])+[Ee][-+]")
                            ? "" : "Z_";
            String build = mtc.group(1) + prefix + mtc.group(2);
            sql = sql.substring(0, mtc.start()) + build
                    + replaceDigitStartingIdentifiers(mtc.group(7) + sql.substring(mtc.end()));
        }

        return sql;
    }

    private static String convertXescaped(String sqlc) {
        for (String xidt : ESCAPED_IDENTIFIERS) {
            sqlc = sqlc.replaceAll(XESCAPED.replaceAll("_", xidt), "$1$3$4");
        }
        return sqlc;
    }

    private static String convertPartIdentifiers(String sql) {
        String sqlc = convertIdentifiers(sql);
        sqlc = convertXescaped(sqlc);
        for (Map.Entry<String, String> entry : IDENTIFIERS_CONTAINING_KEYWORD.entrySet()) {
            sqlc = sqlc.replaceAll("(?i)\"" + entry.getKey() + "\"", "\"" + entry.getValue() + "\"");
        }
        sqlc = Pattern.compile(KEYWORD_ALIAS, Pattern.CASE_INSENSITIVE).matcher(sqlc).replaceAll("$1\"$2\"$3");
        return sqlc;
    }

    private static String escape(String sql) {
        int li = Math.max(sql.lastIndexOf('"'), sql.lastIndexOf('\''));
        boolean enddq = sql.endsWith("\"") || sql.endsWith("'");
        String suff = enddq ? "" : sql.substring(li + 1);
        suff = convertPartIdentifiers(suff);
        String tsql = enddq ? sql : sql.substring(0, li + 1);
        int[] fd = getDoubleQuoteGroup(tsql);
        int[] fs = getQuoteGroup(tsql);
        if (fd != null || fs != null) {
            boolean inid = fs == null || fd != null && fd[0] < fs[0];
            String group;
            String str;
            int[] mcr = inid ? fd : fs;
            group = tsql.substring(mcr[0] + 1, mcr[1] - 1);
            if (inid) {
                group = group.replaceAll("'", "''").replaceAll("\"\"", "\"");
            }
            str = tsql.substring(0, mcr[0]);
            str = convertPartIdentifiers(str);
            tsql = str + "'" + group + "'" + escape(tsql.substring(mcr[1]));
        } else {
            tsql = convertPartIdentifiers(tsql);
        }
        return tsql + suff;
    }

    public static void cleanEscaped() {
        ESCAPED_IDENTIFIERS.removeAll(ALREADY_ESCAPED_IDENTIFIERS);
    }

    public static String procedureEscapingIdentifier(String name) {
        if (PROCEDURE_KEYWORD_LIST.contains(name.toUpperCase())) {
            name = "\"" + name.toUpperCase() + "\"";

        }
        return name;
    }

    public static String preEscapingIdentifier(String _name) {
        if (_name.length() == 0) {
            return _name;
        }
        if (_name.startsWith("~")) {
            return null;
        }
        String nl = _name.toUpperCase(Locale.US);
        if (TableBuilder.isReservedWord(nl)) {
            ESCAPED_IDENTIFIERS.add(nl);
        }
        if (_name.contains("'") || _name.indexOf('"') > 0) {
            APOSTROPHISED_NAMES.add(_name);
        }

        if (nl.startsWith("X") && TableBuilder.isReservedWord(nl.substring(1))) {
            ALREADY_ESCAPED_IDENTIFIERS.add(nl.substring(1));
        }

        String escaped = _name;
        escaped = _name.replace("'", "").replace("\"", "").replaceAll(Pattern.quote("\\"), "_");

        if (escaped.length() > 0 && Character.isDigit(escaped.trim().charAt(0))) {
            escaped = "Z_" + escaped.trim();
        }
        if (escaped.length() > 0 && escaped.charAt(0) == '_') {
            escaped = "Z" + escaped;
        }
        if (dualUsedAsTableName && "DUAL".equalsIgnoreCase(escaped)) {
            escaped = "DUAL_13031971";
        }

        return escaped.toUpperCase(Locale.US);
    }

    private static String escapeKeywordIdentifier(String _escaped, boolean _quote) {
        if (_escaped != null && KEYWORDLIST.contains(_escaped.toUpperCase())) {
            return _quote ? "\"" + _escaped + "\"" : "[" + _escaped + "]";
        }
        return _escaped;
    }

    public static String basicEscapingIdentifier(String name) {
        return escapeKeywordIdentifier(preEscapingIdentifier(name), true);

    }

    public static String escapeIdentifier(String name, Connection conn) throws SQLException {
        return checkLang(escapeIdentifier(name), conn, true);
    }

    public static String completeEscaping(String escaped, boolean quote) {
        return hsqlEscape(escapeKeywordIdentifier(escaped, quote), quote);
    }

    public static String completeEscaping(String escaped) {
        return completeEscaping(escaped, true);
    }

    private static String hsqlEscape(String escaped, boolean quote) {
        if (escaped != null && (escaped.indexOf(' ') > 0 || escaped.contains("$"))) {
            escaped = quote ? "\"" + escaped + "\"" : "[" + escaped + "]";
        }
        return escaped;
    }

    public static String checkLang(String _name, Connection _conn) throws SQLException {
        return checkLang(_name, _conn, true);
    }

    public static String checkLang(String _name, Connection _conn, boolean _quote) throws SQLException {
        String name = _name;
        if (!_quote) {
            name = _name.replace(Pattern.quote("["), "\"").replace(Pattern.quote("]"), "\"");
        }
        try (Statement st = _conn.createStatement()) {
            st.execute("SELECT 1 AS " + name + " FROM dual");
            return _name;
        } catch (SQLException _ex) {
            return _quote ? "\"" + _name + "\"" : "[" + _name + "]";
        }
    }

    public static String escapeIdentifier(String name) {
        String escaped = basicEscapingIdentifier(name);

        if (escaped == null) {
            return null;
        }
        return hsqlEscape(escaped, true);
    }

    public static boolean couldNeedDefault(String typeDeclaration) {
        Matcher mtc = DEFAULT_CATCH_0.matcher(typeDeclaration);
        return !mtc.find() && Pattern.compile(NOT_NULL).matcher(typeDeclaration).find();

    }

    public static String convertAddColumn(String tableName, String columnName, String typeDeclaration) {
        typeDeclaration = convertTypeDeclaration(typeDeclaration);
        typeDeclaration = typeDeclaration.replaceAll(NOT_NULL, "");
        Matcher mtc = DEFAULT_CATCH_0.matcher(typeDeclaration);
        if (mtc.find()) {
            typeDeclaration = typeDeclaration.substring(0, mtc.start());
        }

        return "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + typeDeclaration;
    }

    private static String convertTypeDeclaration(String typeDecl) {
        typeDecl = " " + typeDecl + " "; // padding for a generic RE use
        for (Map.Entry<String, String> entry : TypesMap.getAccess2HsqlTypesMap().entrySet()) {
            typeDecl = typeDecl.replaceAll("([\\s\n\r]+)((?i)" + entry.getKey() + ")([\\s\n\r\\(]+)",
                    "$1" + entry.getValue() + "$3");
        }
        return typeDecl.replaceAll(DEFAULT_VARCHAR_0, "$1VARCHAR(255)$2");
    }

    private static String convertCreateTable(String sql, Map<String, String> _types2Convert) throws SQLException {
        // padding for detecting the right exception
        sql += " ";
        if (!sql.contains("(")) {
            return sql;
        }
        String pre = sql.substring(0, sql.indexOf('('));
        sql = sql.substring(sql.indexOf('('));
        for (Map.Entry<String, String> entry : _types2Convert.entrySet()) {
            sql = sql
                    .replaceAll("([,\\(][\\s\n\r]*)" + TYPES_TRANSLATE.replaceAll("_", entry.getKey()),
                            "$1___" + entry.getKey() + "___$2")
                    .replaceAll("(\\W)" + TYPES_TRANSLATE.replaceAll("_", entry.getKey()),
                            "$1" + entry.getValue() + "$2")
                    .replaceAll("(\\W)" + TYPES_TRANSLATE.replaceAll("_", "___" + entry.getKey() + "___"),
                            "$1" + entry.getKey() + "$2");
        }
        sql = sql.replaceAll(DEFAULT_VARCHAR, "$1VARCHAR(255)$2");
        return clearDefaultsCreateStatement(pre + sql);
    }

    public static String getDDLDefault(String ddlf) {
        for (String pattern : DEFAULT_CATCH) {
            Pattern pt = Pattern.compile(pattern);
            Matcher mtch = pt.matcher(ddlf + " ");
            if (mtch.find()) {
                return mtch.group(2);
            }

        }
        return null;
    }

    private static String clearDefaultsCreateStatement(String sql) throws SQLException {
        if (!sql.toUpperCase().contains("DEFAULT")) {
            return sql;
        }
        int startDecl = sql.indexOf('(');
        int endDecl = sql.lastIndexOf(')');
        if (startDecl >= endDecl) {
            throw new UcanaccessSQLException(ExceptionMessages.INVALID_CREATE_STATEMENT);
        }
        for (String pattern : DEFAULT_CATCH) {
            sql = sql.replaceAll(pattern, "$3");
        }

        return sql;
    }

    public static String convertCreateTable(String sql) throws SQLException {
        return convertCreateTable(sql, TypesMap.getAccess2HsqlTypesMap());
    }

    public static boolean checkDDL(String sql) {
        if (sql == null) {
            return false;
        }
        return CHECK_DDL.matcher(sql.replaceAll("[\n\r]", " ")).matches();
    }

    private static String convertLike(String sql) {
        Matcher matcher = FIND_LIKE_PATTERN.matcher(sql);
        if (matcher.find()) {
            return sql.substring(0, matcher.start(1))
                    + convertLike(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4))
                    + convertLike(sql.substring(matcher.end(0)));
        } else {
            return sql;
        }
    }

    private static String convert2RegexMatches(String likeContent) {
        Matcher mtc = ACCESS_LIKE_ESCAPE_PATTERN.matcher(likeContent);
        if (mtc.find()) {
            return convert2RegexMatches(likeContent.substring(0, mtc.start(0))) + mtc.group(0).charAt(1)
                    + convert2RegexMatches(likeContent.substring(mtc.end(0)));
        }
        return likeContent.replaceAll("#", "\\\\d").replaceAll("\\*", ".*").replaceAll("_", ".")
                .replaceAll("(\\[)\\!(\\w\\-\\w\\])", "$1^$2");
    }

    private static String convert2LikeCondition(String likeContent) {
        Matcher mtc = ACCESS_LIKE_ESCAPE_PATTERN.matcher(likeContent);
        if (mtc.find()) {
            return convert2LikeCondition(likeContent.substring(0, mtc.start(0))) + mtc.group(0).charAt(1)
                    + convert2LikeCondition(likeContent.substring(mtc.end(0)));
        }
        return likeContent.replaceAll("\\*", "%").replaceAll("\\?", "_");
    }

    private static String convertLike(String conditionField, String closePar, String not, String likeContent) {
        int i = likeContent.replaceAll("\\[#\\]", "").indexOf('#');
        not = not == null ? "" : " NOT ";
        if (i >= 0 || ACCESS_LIKE_CHARINTERVAL_PATTERN.matcher(likeContent).find()) {
            return not + "REGEXP_MATCHES(" + conditionField + ",'" + convert2RegexMatches(likeContent) + "')" + closePar
                    + " ";
        }
        return " " + conditionField + closePar + not + " like '" + convert2LikeCondition(likeContent) + "'";
    }

    public static boolean isSupportsAccessLike() {
        return supportsAccessLike;
    }

    public static void setSupportsAccessLike(boolean _supportsAccessLike) {
        SQLConverter.supportsAccessLike = _supportsAccessLike;
    }

    public static boolean isXescaped(String identifier) {
        return ESCAPED_IDENTIFIERS.contains(identifier);
    }

    public static String convertFormula(String sql) {
        // white space to allow replaceWorkAroundFunction pattern to work fine
        sql = convertFormula0(convertSQL(" " + sql).getSql());
        return sql;
    }

    private static String convertDigit(String sql) {
        Matcher mtc = ESPRESSION_DIGIT.matcher(sql);
        char[] cq = sql.toCharArray();
        if (mtc.find()) {
            int idx = mtc.start();
            int idxe = mtc.end();
            boolean replace = true;
            for (int j = idx; j >= 0; j--) {
                if (Character.isDigit(cq[j])) {
                    continue;
                } else {
                    if (Character.isLetter(cq[j])) {
                        replace = false;
                    }
                    break;
                }
            }
            if (replace) {
                return sql.substring(0, idx) + mtc.group(1) + "E0" + convertDigit(sql.substring(idxe));
            } else {
                return sql.substring(0, idxe) + convertDigit(sql.substring(idxe));
            }
        } else {
            return sql;
        }

    }

    private static String convertFormula0(String sql) {
        int li = Math.max(sql.lastIndexOf('\"'), sql.lastIndexOf('\''));
        boolean enddq = sql.endsWith("\"") || sql.endsWith("'");
        String suff = enddq ? "" : sql.substring(li + 1);
        suff = convertDigit(suff);
        String tsql = enddq ? sql : sql.substring(0, li + 1);
        int[] fd = getDoubleQuoteGroup(tsql);
        int[] fs = getQuoteGroup(tsql);
        if (fd != null || fs != null) {
            boolean inid = fs == null || fd != null && fd[0] < fs[0];
            String group;
            String str;
            int[] mcr = inid ? fd : fs;
            group = tsql.substring(mcr[0], mcr[1]);
            str = tsql.substring(0, mcr[0]);
            str = convertDigit(str);
            tsql = str + group + convertFormula0(tsql.substring(mcr[1]));
        } else {
            tsql = convertDigit(tsql);
        }
        return tsql + suff;
    }

    public static String convertPowOperator(String sql) {
        int i = sql.indexOf('^');
        if (i < 0) {
            return sql;
        }
        while ((i = sql.indexOf('^')) >= 0) {
            int foi = firstOperandIndex(sql, i);
            int loi = i + secondOperandIndex(sql, i) + 1;
            sql = sql.substring(0, foi) + " (power(" + sql.substring(foi, i) + "," + sql.substring(i + 1, loi + 1)
                    + "))" + sql.substring(loi + 1);
        }

        return sql;
    }

    private static int secondOperandIndex(String sql, int i) {
        sql = sql.substring(i + 1);
        char[] ca = sql.toCharArray();
        boolean foundType = false;
        boolean field = false;
        boolean group = false;
        boolean digit = false;
        int countPar = 0;
        int j;

        for (j = 0; j < ca.length; j++) {
            char c = ca[j];
            if (c == ' ') {
                continue;
            }

            if (foundType) {
                if (field && c == ']') {
                    return j;
                } else if (digit && !Character.isDigit(c) && c != '.') {
                    return j - 1;
                } else if (group) {
                    if (c == '(') {
                        countPar++;
                    }
                    if (c == ')') {
                        countPar--;
                        if (countPar == 0) {
                            return j;
                        }
                    }
                }
            } else {
                if (c == '[') {
                    foundType = true;
                    field = true;
                } else if (c == '(') {
                    foundType = true;
                    group = true;
                    countPar++;
                } else if (Character.isDigit(c)) {
                    foundType = true;
                    digit = true;
                } else if (c == '+' || c == '-') {
                    if (j + 1 < ca.length && ca[j + 1] != '(' && ca[j + 1] != '[') {
                        foundType = true;
                        digit = true;
                    }
                }

            }

        }
        return j - 1;
    }

    private static int firstOperandIndex(String sql, int i) {
        sql = sql.substring(0, i);

        char[] ca = sql.toCharArray();
        boolean foundType = false;
        boolean field = false;
        boolean group = false;
        boolean digit = false;
        int countPar = 0;
        int j;
        for (j = ca.length - 1; j >= 0; j--) {
            char c = ca[j];

            if (c == ' ') {
                continue;
            }
            if (foundType) {
                if (field && c == '[') {
                    return j;
                } else if (digit && !Character.isDigit(c) && c != '.') {
                    if ((c == '+' || c == '-') && j > 0 && (ca[j - 1] == '+' || ca[j - 1] == '-')) {
                        return j;
                    }

                    return j + 1;
                } else if (group) {
                    if (c == ')') {
                        countPar++;
                    }

                    if (c == '(') {
                        countPar--;
                        if (countPar == 0) {
                            return j;
                        }
                    }
                }
            } else {
                if (c == ']') {
                    foundType = true;
                    field = true;
                } else if (c == ')') {
                    foundType = true;
                    group = true;
                    countPar++;
                } else if (Character.isDigit(c)) {
                    foundType = true;
                    digit = true;
                }

            }

        }
        return j + 1;
    }

    public static int asUnsigned(byte _a) {
        return _a & 0xFF;
    }

    public static Set<String> getFormulaDependencies(String formula) {
        Matcher mtc = FORMULA_DEPENDENCIES.matcher(formula);
        Set<String> fd = new HashSet<>();
        while (mtc.find()) {
            fd.add(escapeIdentifier(mtc.group(1)));
        }
        return fd;
    }

    static boolean isDualUsedAsTableName() {
        return dualUsedAsTableName;
    }

    static void setDualUsedAsTableName(boolean _dualUsedAsTableName) {
        SQLConverter.dualUsedAsTableName = _dualUsedAsTableName;
    }

    public static String removeParameters(String qtxt) {
        return qtxt.replaceAll(PARAMETERS, "");
    }

    public static String getPreparedStatement(String qtxt, List<String> l) {
        String s = qtxt;
        for (String p : l) {
            s = s.replaceAll("(\\W)((?i)" + Pattern.quote(p) + ")(\\W)", "$1?$3");
        }
        return s.replaceAll(Pattern.quote("[?]"), "?");
    }

    public static List<String> getParameters(String s) {
        List<String> ar = new ArrayList<>();

        for (Matcher mtch = FORMULA_DEPENDENCIES.matcher(s); mtch.find(); mtch = FORMULA_DEPENDENCIES.matcher(s)) {
            ar.add(mtch.group());
            s = s.substring(mtch.end());
        }
        return ar;
    }
}
