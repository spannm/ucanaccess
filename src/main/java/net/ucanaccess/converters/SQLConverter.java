package net.ucanaccess.converters;

import io.github.spannm.jackcess.TableBuilder;
import net.ucanaccess.exception.InvalidCreateStatementException;
import net.ucanaccess.jdbc.NormalizedSQL;
import net.ucanaccess.jdbc.UcanaccessConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings({"PMD.FieldDeclarationsShouldBeAtStartOfClass", "PMD.UnnecessaryFullyQualifiedName",
                   "java:S1192", "java:S6353"})
public final class SQLConverter {

    @SuppressWarnings({"java:S5842", "java:S5852", "java:S5998"})
    public static final class Patterns {
        private static final Pattern       SELECT_FROM_START          = Pattern.compile("\\s*SELECT\\s+", Pattern.CASE_INSENSITIVE);
        private static final Pattern       SELECT_FROM_END            = Pattern.compile("\\s*FROM[\\s\\[]+", Pattern.CASE_INSENSITIVE);
        private static final Pattern       UNESCAPED_ALIAS            = Pattern.compile("\\s*AS\\s*", Pattern.CASE_INSENSITIVE);
        private static final Pattern       QUOTE_S                    = Pattern.compile("(')+");
        private static final Pattern       DOUBLE_QUOTE_S             = Pattern.compile("(\")+");
        private static final Pattern       QUOTE_M                    = Pattern.compile("'(([^'])*)'");
        private static final Pattern       DOUBLE_QUOTE_M             = Pattern.compile("\"(([^\"])*)\"");
        private static final Pattern       FIND_LIKE                  = Pattern.compile(
            "[\\s\\(]*([\\w\\.]*)([\\s\\)]*)(NOT\\s*)*LIKE\\s*'([^']*(?:'')*)'", Pattern.CASE_INSENSITIVE);
        private static final Pattern       ACCESS_LIKE_CHARINTERVAL   = Pattern.compile("\\[(?:\\!*[a-zA-Z0-9]\\-[a-zA-Z0-9])+\\]");
        private static final Pattern       ACCESS_LIKE_ESCAPE         = Pattern.compile("\\[[\\*|_|#]\\]");
        private static final Pattern       CHECK_DDL                  = Pattern.compile("^(\\s*(CREATE|ALTER|DROP|ENABLE|DISABLE))\\s+.*", Pattern.CASE_INSENSITIVE);
        private static final Pattern       KIND_OF_SUBQUERY           = Pattern.compile("(\\[)(( FROM )*(SELECT )*([^\\]])*)(\\]\\.\\s)", Pattern.CASE_INSENSITIVE);
        private static final Pattern       NO_DATA                    = Pattern.compile(" WITH\\s+NO\\s+DATA", Pattern.CASE_INSENSITIVE);
        private static final Pattern       NO_ALPHANUMERIC            = Pattern.compile("\\W");
        private static final Pattern       IDENTITY                   = Pattern.compile("(\\W+)(@@identity)(\\W*)", Pattern.CASE_INSENSITIVE);
        private static final Pattern       SELECT_IDENTITY            = Pattern.compile("SELECT\\s+@@identity.*", Pattern.CASE_INSENSITIVE);
        private static final Pattern       HAS_FROM                   = Pattern.compile("\\s+FROM\\s+", Pattern.CASE_INSENSITIVE);
        private static final Pattern       FORMULA_DEPS               = Pattern.compile("\\[([^\\]]*)\\]");
        private static final Pattern       EXCLAM_POINT               = Pattern.compile("(\\!)(\\s*)([^\\=])");
        private static final Pattern       YES                        = Pattern.compile("(\\W)YES(\\W)", Pattern.CASE_INSENSITIVE);
        private static final Pattern       NO                         = Pattern.compile("(\\W)NO(\\W)", Pattern.CASE_INSENSITIVE);
        private static final Pattern       WITH_OWNERACCESS_OPTION    = Pattern.compile("(\\W)WITH\\s+OWNERACCESS\\s+OPTION(\\W)", Pattern.CASE_INSENSITIVE);
        private static final Pattern       DIGIT_STARTING_IDENTIFIERS = Pattern.compile("(\\W)(([0-9])+(([_A-Z])+([0-9])*)+)(\\W)", Pattern.CASE_INSENSITIVE);
        private static final Pattern       UNDERSCORE_IDENTIFIERS     = Pattern.compile("(\\W)((_)+([_A-Z0-9])+)(\\W)", Pattern.CASE_INSENSITIVE);
        private static final List<Pattern> DEFAULT_CATCH              = List.of(
            Pattern.compile("(\\s*DEFAULT\\s+)('(?:[^']*(?:'')*)*')([\\s\\)\\,])", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(\\s*DEFAULT\\s+)(\"(?:[^\"]*(?:\"\")*)*\")([\\s\\)\\,])", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(\\s*DEFAULT\\s+)([0-9\\.\\-\\+]+)([\\s\\)\\,])", Pattern.CASE_INSENSITIVE),
            Pattern.compile("(\\s*DEFAULT\\s+)([_0-9a-zA-Z]*\\([^\\)]*\\))([\\s\\)\\,])", Pattern.CASE_INSENSITIVE));
        private static final Pattern       DEFAULT_CATCH_0            = Pattern.compile("(\\s*DEFAULT\\s+)", Pattern.CASE_INSENSITIVE);
        public static final Pattern        NOT_NULL                   = Pattern.compile("\\sNOT\\sNULL", Pattern.CASE_INSENSITIVE);
        private static final Pattern       QUOTED_ALIAS               = Pattern.compile(
            "(\\s+AS\\s*)(\\[[^\\]]*\\])(\\W)", Pattern.CASE_INSENSITIVE);
        private static final Pattern       DISTINCT_ROW               = Pattern.compile("\\s+DISTINCTROW\\s+", Pattern.CASE_INSENSITIVE);
        private static final Pattern       DEFAULT_VARCHAR            = Pattern.compile("(\\W)VARCHAR([\\s,\\)])", Pattern.CASE_INSENSITIVE);
        private static final Pattern       DEFAULT_VARCHAR_0          = Pattern.compile("(\\W)VARCHAR([^\\(])", Pattern.CASE_INSENSITIVE);
        private static final Pattern       ESPRESSION_DIGIT           = Pattern.compile("([\\d]+)(?![\\.\\d])");

        private Patterns() {
        }
    }

    private static final String              NAME_PAT                       = "(([_a-zA-Z0-9])+|\\[([^\\]])*\\]|`([^`])*`)";
    private static final int                 NAME_PAT_STEP                  = 4;
    private static final String              UNION                          = "(;)(\\s*)((?i)UNION)(\\s*)";
    private static final String              BACKTICK                       = "(`)([^`]*)(`)";
    private static final String              DELETE_ALL                     = "((?i)DELETE\\s+)(\\*)(\\s+(?i)FROM\\s+)";
    private static final String              PARAMETERS                     = "(?i)PARAMETERS([^;]*);";
    private static final String              BIG_BANG                       = "1899-12-30";
    private static final List<String>        KEYWORDS_LIST                  = List.of("ALL", "AND", "ANY",
        "ALTER", "AS", "AT", "AVG", "BETWEEN", "BOTH", "BY", "CALL", "CASE", "CAST", "CHECK", "COALESCE",
        "CORRESPONDING", "CONVERT", "COUNT", "CREATE", "CROSS", "DEFAULT", "DISTINCT", "DROP", "ELSE", "EVERY",
        "EXISTS", "EXCEPT", "FOR", "FOREIGN", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "IN", "INNER",
        "INTERSECT", "INTO", "IS", "JOIN", "LEFT", "LEADING", "LIKE", "MAX", "MIN", "NATURAL", "NOT", "NULLIF",
        "ON", "ORDER", "OR", "OUTER", "PRIMARY", "REFERENCES", "RIGHT", "SELECT", "SET", "SOME", "STDDEV_POP",
        "STDDEV_SAMP", "SUM", "TABLE", "THEN", "TO", "TRAILING", "TRIGGER", "UNION", "UNIQUE", "USING", "VALUES",
        "VAR_POP", "VAR_SAMP", "WHEN", "WHERE", "WITH", "END", "DO", "CONSTRAINT", "USER", "ROW");
    private static final Pattern             PAT_KEYWORD_ALIAS              = Pattern.compile(
        "(\\s+AS\\s+)("
        + KEYWORDS_LIST.stream()
            .filter(s -> !"SELECT".equals(s))
            .collect(Collectors.joining("|"))
        + ")(\\W)", Pattern.CASE_INSENSITIVE);

    public static final String               DATE_ACCESS_FORMAT             = "(0[1-9]|[1-9]|1[012])/(0[1-9]|[1-9]|[12][0-9]|3[01])/(\\d\\d\\d\\d)";
    public static final String               DATE_FORMAT                    = "(\\d\\d\\d\\d)-(0[1-9]|[1-9]|1[012])-(0[1-9]|[1-9]|[12][0-9]|3[01])";
    public static final String               HHMMSS_ACCESS_FORMAT           = "([0-9]|0[0-9]|1[0-9]|2[0-4]):([0-9]|[0-5][0-9]):([0-9]|[0-5][0-9])";
    public static final String               HHMMSS_FORMAT                  = "([0-9]|0[0-9]|1[0-9]|2[0-4]):([0-9]|[0-5][0-9]):([0-5][0-9]|[0-9])";

    private static final List<String>        PROCEDURE_KEYWORDS             = List.of("NEW", "ROW");
    private static final List<String>        WHITE_SPACED_TABLE_NAMES       = new ArrayList<>();
    private static final Set<String>         ESCAPED_IDENTIFIERS            = new HashSet<>();
    private static final Set<String>         ALREADY_ESCAPED_IDENTIFIERS    = new HashSet<>();
    private static final Map<String, String> IDENTIFIERS_CONTAINING_KEYWORD = new HashMap<>();
    private static final Set<String>         APOSTROPHISED_NAMES            = new HashSet<>();
    private static final Set<String>         WORKAROUND_FUNCTIONS           = new HashSet<>();

    private static boolean                   supportsAccessLike             = true;
    private static boolean                   dualUsedAsTableName            = false;

    private SQLConverter() {
    }

    public static boolean hasIdentity(String sql) {
        return sql.indexOf("@@") > 0 && sql.toUpperCase(Locale.US).indexOf("@@IDENTITY") > 0;
    }

    private static void aliases(String sql, NormalizedSQL nsql) {
        Matcher m = Patterns.SELECT_FROM_START.matcher(sql);
        if (m.find()) {

            int init = m.end();
            sql = sql.substring(init);
            m = Patterns.SELECT_FROM_END.matcher(sql);
            if (m.find()) {
                int end = m.start();
                sql = sql.substring(0, end);

                for (m = Patterns.UNESCAPED_ALIAS.matcher(sql); m.find(); m = Patterns.UNESCAPED_ALIAS.matcher(sql)) {
                    int e = m.end();

                    sql = sql.substring(e) + " ";
                    char[] sqlc = sql.toCharArray();
                    if (sqlc[0] == '[') {
                        continue;
                    }
                    StringBuilder sb = new StringBuilder();
                    for (char c : sqlc) {
                        if (c == ' ' || c == '\n' || c == '\r' || c == ',') {
                            String key = preEscapingIdentifier(sb.toString());
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
        Matcher m1 = Patterns.SELECT_IDENTITY.matcher(sql);
        Matcher m2 = Patterns.HAS_FROM.matcher(sql);
        String end = m1.matches() && !m2.find() ? " FROM DUAL" : "";
        if (key instanceof String) {
            key = "'" + key + "'";
        }
        return Patterns.IDENTITY.matcher(sql).replaceAll("$1" + key + "$3") + end;
    }

    public static boolean isListedAsKeyword(String s) {
        return s != null && KEYWORDS_LIST.contains(s.toUpperCase());
    }

    private static int[] getQuoteGroup(String _s) {
        if (!_s.contains("''")) {
            Matcher m = Patterns.QUOTE_M.matcher(_s);
            if (m.find()) {
                return new int[] {m.start(), m.end()};
            }
        } else {
            int[] ret = new int[] {-1, -1};
            Matcher m = Patterns.QUOTE_S.matcher(_s);
            while (m.find()) {
                int start = m.start();
                int end = m.end();
                if ((end - start) % 2 == 0) {
                    if (ret[0] == -1) {
                        return new int[] {m.start(), m.end()};
                    }
                } else {
                    if (ret[0] == -1) {
                        ret[0] = m.start();
                    } else {
                        ret[1] = m.end();
                        return ret;
                    }
                }
            }
        }
        return new int[0];
    }

    private static int[] getDoubleQuoteGroup(String _s) {
        if (!_s.contains("\"\"")) {
            Matcher m = Patterns.DOUBLE_QUOTE_M.matcher(_s);
            if (m.find()) {
                return new int[] {m.start(), m.end()};
            }
        } else {
            int[] ret = new int[] {-1, -1};
            Matcher mc = Patterns.DOUBLE_QUOTE_S.matcher(_s);
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
        }
        return new int[0];
    }

    public enum DDLType {
        CREATE_TABLE_AS_SELECT("\\s*create\\s+table\\s+" + NAME_PAT + "\\s*(?)AS\\s*\\(\\s*((?)SELECT)"),
        CREATE_TABLE("\\s*create\\s+table\\s+" + NAME_PAT),
        DROP_TABLE_CASCADE("\\s*drop\\s+table\\s+" + NAME_PAT + "\\s+cascade"),
        DROP_TABLE("\\s*drop\\s+table\\s+" + NAME_PAT),
        ALTER_RENAME("\\s*alter\\s+table\\s+" + NAME_PAT + "\\s+rename\\s+to\\s+" + NAME_PAT),
        CREATE_PRIMARY_KEY("\\s*alter\\s+table\\s+" + NAME_PAT + "\\s+add\\s+(?:constraint\\s+" + NAME_PAT + "\\s+)?primary\\s+key(.*)"),
        CREATE_FOREIGN_KEY("\\s*alter\\s+table\\s+" + NAME_PAT + "\\s+add\\s+(?:constraint\\s+" + NAME_PAT
            + "\\s+)?foreign\\s+key\\s+" + "(?:\\(.*\\))\\s*references\\s+" + NAME_PAT + "(.*)"),
        DROP_FOREIGN_KEY("\\s*alter\\s+table\\s+" + NAME_PAT + "\\s+drop\\s+constraint\\s+" + NAME_PAT),
        ADD_COLUMN("\\s*alter\\s+table\\s+" + NAME_PAT + "\\s+add\\s+(?:column\\s+)?" + NAME_PAT + "(.*)"),
        CREATE_INDEX("CREATE\\s+(?:unique)?\\s*index\\s+" + NAME_PAT + "\\s+ON\\s+" + NAME_PAT + "\\s+"),
        DISABLE_AUTOINCREMENT("\\s*disable\\s+autoincrement\\s+on\\s*" + NAME_PAT),
        ENABLE_AUTOINCREMENT("\\s*enable\\s+autoincrement\\s+on\\s*" + NAME_PAT);

        private final Pattern pattern;
        private String        ddl;

        DDLType(String _regex) {
            pattern = Pattern.compile(_regex, Pattern.CASE_INSENSITIVE);
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
                return m.group(1 + NAME_PAT_STEP);
            }
            return null;
        }

        public String getThirdDBObjectName() {
            Matcher m = pattern.matcher(ddl);
            if (m.find()) {
                return m.group(1 + 2 * NAME_PAT_STEP);
            }
            return null;
        }

        public String getColumnDefinition() {
            Matcher m = pattern.matcher(ddl);
            if (m.find()) {
                return m.group(2 * NAME_PAT_STEP + 1);
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

    private static String replaceBacktick(String sql) {
        return sql.replaceAll(BACKTICK, "[$2]");
    }

    private static String replaceAposNames(String sql) {
        for (String an : APOSTROPHISED_NAMES) {
            sql = sql.replaceAll("(?i)" + Pattern.quote('[' + an + ']'), '[' + basicEscapingIdentifier(an) + ']');
        }
        return sql;
    }

    public static NormalizedSQL convertSQL(String sql, boolean creatingQuery) {
        return convertSQL(sql, null, creatingQuery);
    }

    public static NormalizedSQL convertSQL(String _sql, UcanaccessConnection _conn, boolean _creatingQuery) {
        NormalizedSQL nsql = new NormalizedSQL();
        String sql = _sql + " ";
        aliases(sql, nsql);
        sql = replaceBacktick(sql);
        sql = replaceAposNames(sql);
        sql = convertUnion(sql);
        sql = convertAccessDate(sql);
        sql = convertQuotedAliases(sql, nsql);
        sql = escape(sql);
        sql = convertLike(sql);
        sql = replaceWhiteSpacedTables(sql);
        // sql = replaceExclamationPoints(sql);
        if (!_creatingQuery) {
            Pivot.checkAndRefreshPivot(sql, _conn);
            sql = DFunction.convertDFunctions(sql, _conn);
        }
        sql = sql.trim();

        nsql.setSql(sql);

        return nsql;
    }

    private static String replaceExclamationPoints(String sql) {
        return Patterns.EXCLAM_POINT.matcher(sql).replaceAll(".$2$3");
    }

    private static String convertOwnerAccess(String sql) {
        return Patterns.WITH_OWNERACCESS_OPTION.matcher(sql).replaceAll("");
    }

    private static String convertDeleteAll(String sql) {
        return sql.replaceAll(DELETE_ALL, "$1$3");
    }

    private static String convertUnion(String sql) {
        return sql.replaceAll(UNION, "$2$3$4");
    }

    private static String convertYesNo(String sql) {
        sql = Patterns.YES.matcher(sql).replaceAll("$1true$2");
        Matcher m = Patterns.NO_DATA.matcher(sql);
        if (m.find()) {
            sql = Patterns.NO.matcher(sql.substring(0, m.start())).replaceAll("$1false$2") + sql.substring(m.start());
        } else {
            sql = Patterns.NO.matcher(sql).replaceAll("$1false$2");
        }
        return sql;
    }

    private static String convertQuotedAliases(String sql, NormalizedSQL nsql) {
        for (Matcher m = Patterns.KIND_OF_SUBQUERY.matcher(sql); m.find(); m = Patterns.KIND_OF_SUBQUERY.matcher(sql)) {
            String g2 = m.group(2).trim();
            if (g2.endsWith(";")) {
                g2 = g2.substring(0, g2.length() - 1);
            }
            sql = sql.substring(0, m.start()) + "(" + g2 + ")" + sql.substring(m.end());
        }
        Set<String> hs = new HashSet<>();
        String sqle = sql;
        String sqlN = "";
        for (Matcher m = Patterns.QUOTED_ALIAS.matcher(sqle); m.find(); m = Patterns.QUOTED_ALIAS.matcher(sqle)) {
            String g2 = m.group(2);
            if (g2.indexOf('\'') >= 0 || g2.indexOf('"') >= 0) {
                hs.add(g2);
            }
            String value = g2.substring(1, g2.length() - 1);
            nsql.put(preEscapingIdentifier(value), value);
            sqlN += sqle.substring(0, m.start()) + m.group(1) + g2.replaceAll("['\"]", "") + m.group(3);
            sqle = sqle.substring(m.end());
        }
        sql = sqlN + sqle;
        for (String escaped : hs) {
            sql = sql.replaceAll("\\[" + escaped.substring(1, escaped.length() - 1) + "\\]",
                    escaped.replaceAll("['\"]", ""));
        }
        return sql;
    }

    private static String replaceDistinctRow(String sql) {
        return Patterns.DISTINCT_ROW.matcher(sql).replaceAll(" DISTINCT ");
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
                .replaceAll("#" + DATE_ACCESS_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")#", "Timestamp0('$3-$1-$2 $4')")

                .replaceAll("#" + DATE_ACCESS_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#", "Timestamp0('$3-$1-$2 $4')")

                .replaceAll("#" + DATE_ACCESS_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#", "(Timestamp0('$3-$1-$2 $4')+ 12 Hour) ")
                // FORMAT yyyy-MM-dd
                .replaceAll("#" + DATE_FORMAT + "#", "Timestamp0('$1-$2-$3 00:00:00')")

                .replaceAll("#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")#", "Timestamp0('$1-$2-$3 $4')")

                .replaceAll("#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#", "Timestamp0('$1-$2-$3 $4')")

                .replaceAll("#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#", "(Timestamp0('$1-$2-$3 $4')+ 12 Hour)")

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
            String dw = bst.replace(" ", "  ");
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
                return convertResidualSql(_sql);
            }
            String content = _sql.substring(init + 1, end);
            if (content.indexOf(' ') > 0) {
                String tryContent = " " + content + " ";
                String tryConversion = convertXescaped(tryContent);
                if (!tryConversion.equalsIgnoreCase(tryContent)) {
                    IDENTIFIERS_CONTAINING_KEYWORD.put(tryConversion.trim(), content.toUpperCase());
                }
            }
            boolean isKeyword = isListedAsKeyword(content);

            content = basicEscapingIdentifier(content).toUpperCase();
            String subs = " ";
            if (content != null && !isKeyword && (content.indexOf(' ') > 0 || Patterns.NO_ALPHANUMERIC.matcher(content).find())) {
                subs = "\"";
            }
            _sql = convertResidualSql(_sql.substring(0, init)) + subs + content + subs
                    + convertIdentifiers(_sql.substring(end + 1));
        } else {
            _sql = convertResidualSql(_sql);
        }
        return _sql;
    }

    private static String convertResidualSql(String sql) {
        sql = convertSQLTokens(sql);
        if ("!".equals(sql)) {
            return ".";
        }
        return replaceExclamationPoints(
                replaceDigitStartingIdentifiers(Patterns.UNDERSCORE_IDENTIFIERS.matcher(sql).replaceAll("$1Z$2$5")));
    }

    private static String convertSQLTokens(String sql) {
        return convertDeleteAll(replaceWorkAroundFunctions(
                convertOwnerAccess(replaceDistinctRow(convertYesNo(sql.replace("&", "||"))))));
    }

    private static String replaceDigitStartingIdentifiers(String sql) {

        Matcher m = Patterns.DIGIT_STARTING_IDENTIFIERS.matcher(sql);
        if (m.find()) {
            String grp0 = m.group(0);
            if (Character.isLetter(grp0.charAt(0))) {
                return sql;
            }
            String prefix = grp0.matches("\\.([0-9])+[Ee]([0-9])+\\s") || grp0.matches("\\.([0-9])+[Ee][-+]") ? "" : "Z_";
            String build = m.group(1) + prefix + m.group(2);
            sql = sql.substring(0, m.start()) + build
                    + replaceDigitStartingIdentifiers(m.group(7) + sql.substring(m.end()));
        }

        return sql;
    }

    private static String convertXescaped(String sqlc) {
        for (String xidt : ESCAPED_IDENTIFIERS) {
            sqlc = sqlc.replaceAll("(\\W)((?i)X)((?i)_)(\\W)".replace("_", xidt), "$1$3$4");
        }
        return sqlc;
    }

    private static String convertPartIdentifiers(String sql) {
        String sqlc = convertIdentifiers(sql);
        sqlc = convertXescaped(sqlc);
        for (Map.Entry<String, String> entry : IDENTIFIERS_CONTAINING_KEYWORD.entrySet()) {
            sqlc = sqlc.replaceAll("(?i)" + '"' + entry.getKey() + '"', '"' + entry.getValue() + '"');
        }
        sqlc = PAT_KEYWORD_ALIAS.matcher(sqlc).replaceAll("$1\"$2\"$3");
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
        if (fd.length > 0 || fs.length > 0) {
            boolean inid = fs.length == 0 || fd.length > 0 && fd[0] < fs[0];
            String group;
            String str;
            int[] mcr = inid ? fd : fs;
            group = tsql.substring(mcr[0] + 1, mcr[1] - 1);
            if (inid) {
                group = group.replace("'", "''").replace("\"\"", "\"");
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
        if (PROCEDURE_KEYWORDS.contains(name.toUpperCase())) {
            name = '"' + name.toUpperCase() + '"';

        }
        return name;
    }

    public static String preEscapingIdentifier(String _name) {
        if (_name.isEmpty()) {
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

        if (!escaped.isEmpty() && Character.isDigit(escaped.trim().charAt(0))) {
            escaped = "Z_" + escaped.trim();
        }
        if (!escaped.isEmpty() && escaped.charAt(0) == '_') {
            escaped = "Z" + escaped;
        }
        if (dualUsedAsTableName && "DUAL".equalsIgnoreCase(escaped)) {
            escaped = "DUAL_13031971";
        }

        return escaped.toUpperCase(Locale.US);
    }

    private static String escapeKeywordIdentifier(String _escaped, boolean _quote) {
        if (isListedAsKeyword(_escaped)) {
            return _quote ? '"' + _escaped + '"' : '[' + _escaped + ']';
        }
        return _escaped;
    }

    public static String basicEscapingIdentifier(String name) {
        return escapeKeywordIdentifier(preEscapingIdentifier(name), true);

    }

    public static String escapeIdentifier(String name, Connection conn) {
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
            escaped = quote ? '"' + escaped + '"' : '[' + escaped + ']';
        }
        return escaped;
    }

    public static String checkLang(String _name, Connection _conn) {
        return checkLang(_name, _conn, true);
    }

    public static String checkLang(String _name, Connection _conn, boolean _quote) {
        String name = _name;
        if (!_quote) {
            name = _name.replace(Pattern.quote("["), "\"").replace(Pattern.quote("]"), "\"");
        }
        try (Statement st = _conn.createStatement()) {
            st.execute(String.format("SELECT 1 AS %s FROM dual", name));
            return _name;
        } catch (SQLException _ex) {
            return _quote ? '"' + _name + '"' : '[' + _name + ']';
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
        Matcher m = Patterns.DEFAULT_CATCH_0.matcher(typeDeclaration);
        return !m.find() && Patterns.NOT_NULL.matcher(typeDeclaration).find();

    }

    public static String convertAddColumn(String tableName, String columnName, String _typeDeclaration) {
        String typeDeclaration = convertTypeDeclaration(_typeDeclaration);
        typeDeclaration = Patterns.NOT_NULL.matcher(typeDeclaration).replaceAll("");
        Matcher m = Patterns.DEFAULT_CATCH_0.matcher(typeDeclaration);
        if (m.find()) {
            typeDeclaration = typeDeclaration.substring(0, m.start());
        }

        return "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + typeDeclaration;
    }

    private static String convertTypeDeclaration(String typeDecl) {
        typeDecl = " " + typeDecl + " "; // padding for a generic RE use
        for (Map.Entry<String, String> entry : TypesMap.getAccess2HsqlTypesMap().entrySet()) {
            typeDecl = typeDecl.replaceAll("(\\s+)((?i)" + entry.getKey() + ")([\\s\\(]+)",
                    "$1" + entry.getValue() + "$3");
        }
        return Patterns.DEFAULT_VARCHAR_0.matcher(typeDecl).replaceAll("$1VARCHAR(255)$2");
    }

    private static String convertCreateTable(String sql, Map<String, String> _types2Convert) throws SQLException {
        // padding for detecting the right exception
        sql += " ";
        if (!sql.contains("(")) {
            return sql;
        }
        String pre = sql.substring(0, sql.indexOf('('));
        sql = sql.substring(sql.indexOf('('));

        String exprTypesTranslate = "(?i)_(\\W)";
        for (Map.Entry<String, String> entry : _types2Convert.entrySet()) {
            sql = sql.replaceAll("([,\\(]\\s*)" + exprTypesTranslate.replace("_", entry.getKey()), "$1___" + entry.getKey() + "___$2")
                     .replaceAll("(\\W)" + exprTypesTranslate.replace("_", entry.getKey()), "$1" + entry.getValue() + "$2")
                     .replaceAll("(\\W)" + exprTypesTranslate.replace("_", "___" + entry.getKey() + "___"), "$1" + entry.getKey() + "$2");
        }
        sql = Patterns.DEFAULT_VARCHAR.matcher(sql).replaceAll("$1VARCHAR(255)$2");
        return clearDefaultsCreateStatement(pre + sql);
    }

    public static String getDDLDefault(String ddlf) {
        for (Pattern pat : Patterns.DEFAULT_CATCH) {
            Matcher m = pat.matcher(ddlf + " ");
            if (m.find()) {
                return m.group(2);
            }

        }
        return null;
    }

    private static String clearDefaultsCreateStatement(String _sql) throws SQLException {
        if (!_sql.toUpperCase().contains("DEFAULT")) {
            return _sql;
        }
        int startDecl = _sql.indexOf('(');
        int endDecl = _sql.lastIndexOf(')');
        if (startDecl >= endDecl) {
            throw new InvalidCreateStatementException(_sql);
        }
        for (Pattern pat : Patterns.DEFAULT_CATCH) {
            _sql = pat.matcher(_sql).replaceAll("$3");
        }

        return _sql;
    }

    public static String convertCreateTable(String sql) throws SQLException {
        return convertCreateTable(sql, TypesMap.getAccess2HsqlTypesMap());
    }

    public static boolean checkDDL(String sql) {
        return sql != null && Patterns.CHECK_DDL.matcher(sql.replaceAll("\\s+", " ")).matches();
    }

    private static String convertLike(String sql) {
        Matcher matcher = Patterns.FIND_LIKE.matcher(sql);
        if (matcher.find()) {
            return sql.substring(0, matcher.start(1))
                    + convertLike(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4))
                    + convertLike(sql.substring(matcher.end(0)));
        } else {
            return sql;
        }
    }

    private static String convertToRegexMatches(String likeContent) {
        Matcher mtc = Patterns.ACCESS_LIKE_ESCAPE.matcher(likeContent);
        if (mtc.find()) {
            return convertToRegexMatches(likeContent.substring(0, mtc.start(0)))
                + mtc.group(0).charAt(1)
                + convertToRegexMatches(likeContent.substring(mtc.end(0)));
        }
        return likeContent.replaceAll("#", "\\\\d").replaceAll("\\*", ".*").replace('_', '.')
                .replaceAll("(\\[)\\!(\\w\\-\\w\\])", "$1^$2");
    }

    private static String convertToLikeCondition(String likeContent) {
        Matcher mtc = Patterns.ACCESS_LIKE_ESCAPE.matcher(likeContent);
        if (mtc.find()) {
            return convertToLikeCondition(likeContent.substring(0, mtc.start(0)))
                + mtc.group(0).charAt(1)
                + convertToLikeCondition(likeContent.substring(mtc.end(0)));
        }
        return likeContent.replaceAll("\\*", "%").replaceAll("\\?", "_");
    }

    private static String convertLike(String conditionField, String closePar, String not, String likeContent) {
        int i = likeContent.replaceAll("\\[#\\]", "").indexOf('#');
        not = not == null ? "" : " NOT ";
        if (i >= 0 || Patterns.ACCESS_LIKE_CHARINTERVAL.matcher(likeContent).find()) {
            return not + "REGEXP_MATCHES(" + conditionField + ",'" + convertToRegexMatches(likeContent) + "')" + closePar
                    + " ";
        }
        return " " + conditionField + closePar + not + " like '" + convertToLikeCondition(likeContent) + "'";
    }

    public static boolean isSupportsAccessLike() {
        return supportsAccessLike;
    }

    public static void setSupportsAccessLike(boolean _supportsAccessLike) {
        supportsAccessLike = _supportsAccessLike;
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
        Matcher mtc = Patterns.ESPRESSION_DIGIT.matcher(sql);
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
        if (fd.length > 0 || fs.length > 0) {
            boolean inid = fs.length == 0 || fd.length > 0 && fd[0] < fs[0];
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
        Matcher mtc = Patterns.FORMULA_DEPS.matcher(formula);
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
        dualUsedAsTableName = _dualUsedAsTableName;
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

        for (Matcher m = Patterns.FORMULA_DEPS.matcher(s); m.find(); m = Patterns.FORMULA_DEPS.matcher(s)) {
            ar.add(m.group());
            s = s.substring(m.end());
        }
        return ar;
    }
}
