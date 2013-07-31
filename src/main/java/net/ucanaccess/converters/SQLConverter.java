/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Marco Amadei at amadei.mar@gmail.com.

 */
package net.ucanaccess.converters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ucanaccess.jdbc.UcanaccessConnection;
import com.healthmarketscience.jackcess.Database;

public class SQLConverter {
	private static final Pattern QUOTE_G_PATTERN = Pattern
			.compile("\'(((([^'])*('')*))*)\'");
	private static final Pattern DOUBLE_QUOTE_G_PATTERN = Pattern
			.compile("\"(((([^\"])*(\"\")*))*)\"");
	private static final Pattern FIND_LIKE_PATTERN = Pattern
			.compile("[\\s\n\r\\(]*([\\w\\.]*)([\\s\n\r\\)]*)(?i)LIKE[\\s\n\r]*\'([^']*(?:'')*)\'");
	private static final Pattern ACCESS_LIKE_CHARINTERVAL_PATTERN = Pattern
			.compile("\\[(?:\\!*[a-zA-Z]\\-[a-zA-Z])+\\]");
	
	private static final Pattern ACCESS_LIKE_ESCAPE_PATTERN = Pattern
			.compile("\\[[\\*|_|#]\\]");
	private static final Pattern CHECK_DDL=Pattern
			.compile("^([\n\r\\s]*(?i)(create|alter|drop))[\n\r\\s]+.*");
	private static final Pattern KIND_OF_SUBQUERY = Pattern.compile("(\\[)(((?i) FROM )*((?i)SELECT )*([^\\]])*)(\\]\\.[\\s\n\r])");
	
	private static final Pattern SWITCH_PATTERN=Pattern
			.compile("(\\W(?i)SWITCH[\\s\n\r]*)(\\([^\\)]*\\))"); 
	
	private static final Pattern NO_DATA_PATTERN = Pattern.compile(" (?i)WITH[\\s\n\r]+(?i)NO[\\s\n\r]+(?i)DATA");
	private static final Pattern NO_ALFANUMERIC = Pattern.compile("\\W");
	private static final String YES = "(\\W)((?i)YES)(\\W)";
	private static final String NO = "(\\W)((?i)NO)(\\W)";
	private static final String WITH_OWNERACCESS_OPTION = "(\\W)(?i)WITH[\\s\n\r]+(?i)OWNERACCESS[\\s\n\r]+(?i)OPTION(\\W)";
	private static final String WA_CURRENT_USER = "(\\W)(?i)currentUser\\s*\\(";
	private static final String DIGIT_STARTING_IDENTIFIERS = "(\\W)(([0-9])+([_a-zA-Z])+)(\\W)";
	private static final String UNDERSCORE_IDENTIFIERS = "(\\W)((_)+([_a-zA-Z])+)(\\W)";
	private static final String XESCAPED = "(\\W)((?i)_)(\\W)";
	private static final String XESCAPED_FUNCTIONS = "(\\W)(?i)X(_)\\s*\\(";
	private static final String KEYWORD_ALIAS = "([\\s\n\r]+(?i)AS[\\s\n\r]*)((?i)_)(\\W)";
	
	private static final Pattern QUOTED_ALIAS = Pattern
			.compile("([\\s\n\r]+(?i)AS[\\s\n\r]*)(\\[[^\\]]*\\])(\\W)");
												 
	private static final String TYPES_TRANSLATE = "(\\W)(?i)_(\\W)";
	private static final String DATE_ACCESS_FORMAT = "(0[1-9]|[1-9]|1[012])/(0[1-9]|[1-9]|[12][0-9]|3[01])/(\\d\\d\\d\\d)";
	private static final String DATE_FORMAT = "(\\d\\d\\d\\d)-(0[1-9]|[1-9]|1[012])-(0[1-9]|[1-9]|[12][0-9]|3[01])";
	private static final String HHMMSS_ACCESS_FORMAT = "(0[0-9]|1[0-9]|2[0-4]):([0-5][0-9]):([0-5][0-9])";
	private static final String UNION = "(;)([\\s\n\r]*)((?i)UNION)([\\s\n\r]*)";
	private static final String DISTINCT_ROW = "[\\s\n\r]+(?i)DISTINCTROW[\\s\n\r]+";
	private static final String DEFAULT_VARCHAR="(\\W)(?i)VARCHAR[\\s\\w]*(\\)|,)";
	
	public static final String BIG_BANG = "1899-12-30";
	private static final List<String> NO_SQL_RESERVED_WORDS = Arrays.asList(
			"APPLICATION", "ASSISTANT", "COLUMN", "COMPACTDATABASE",
			"CONTAINER", "CREATEDATABASE", "CREATEFIELD", "CREATEGROUP",
			"CREATEINDEX", "CREATEOBJECT", "CREATEPROPERTY", "CREATERELATION",
			"CREATETABLEDEF", "CREATEUSER", "CREATEWORKSPACE", "DESCRIPTION",
			"DISALLOW", "DOCUMENT", "ECHO", "ERROR", "EXIT", "FIELD", "FIELDS",
			"FILLCACHE", "FORM", "FORMS", "GENERAL", "GETOBJECT", "GETOPTION",
			"GOTOPAGE", "IDLE", "IMP", "INDEXES", "INSERTTEXT", "LASTMODIFIED",
			"LEVEL", "LOGICAL", "LOGICAL1", "MACRO", "MODULE", "MOVE", "NAME",
			"NEWPASSWORD", "OFF", "OPENRECORDSET", "OPTION", "OWNERACCESS",
			"PARAMETER", "PARAMETERS", "PARTIAL", "PROPERTY", "QUERIES",
			"QUIT", "RECALC", "RECORDSET", "REFRESH", "REFRESHLINK",
			"REGISTERDATABASE", "REPAINT", "REPAIRDATABASE", "REPORT",
			"REPORTS", "REQUERY", "SCREEN", "SECTION", "SETFOCUS", "SETOPTION",
			"TABLEDEF", "TABLEDEFS", "TABLEID", "USER", "VALUE", "WORKSPACE",
			"YEAR",
			"COUNTER","CURRENCY", "DATETIME","MEMO", "OLE",	"SINGLE","TEXT","YESNO","GUID");
	
	
	
	private static final List<String> KEYWORDLIST = Arrays.asList("ALL", "AND",
			"ANY", "AS", "AT", "AVG", "BETWEEN", "BOTH", "BY", "CALL", "CASE",
			"CAST", "COALESCE", "CORRESPONDING", "CONVERT", "COUNT", "CREATE",
			"CROSS", "DEFAULT", "DISTINCT", "DROP", "ELSE", "EVERY", "EXISTS",
			"EXCEPT", "FOR", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "IN",
			"INNER ", "INTERSECT", "INTO", "IS", "JOIN", "LEFT", "LEADING",
			"LIKE", "MAX ", "MIN", "NATURAL", "NOT", "NULLIF", "ON", "ORDER",
			"OR", "OUTER", "PRIMARY", "REFERENCES", "RIGHT", "SELECT", "SET",
			"SOME", "STDDEV_POP", "STDDEV_SAMP", "SUM", "TABLE", "THEN", "TO",
			"TRAILING", "TRIGGER", "UNION", "UNIQUE", "USING", "VALUES",
			"VAR_POP", "VAR_SAMP", "WHEN", "WHERE", "WITH");
	private static ArrayList<String> whiteSpacedTableNames = new ArrayList<String>();
	private static final HashSet<String> xescapedIdentifiers = new HashSet<String>();
	private static final HashMap<String, String> identifiersContainingKeyword = new HashMap<String, String>();
	private static final HashSet<String> waFunctions = new HashSet<String>();
	private static boolean supportsAccessLike = true;

	public static enum DDLType {
		CREATE_TABLE_AS_SELECT(
				Pattern.compile("[\\s\n\r]*(?i)create[\\s\n\r]*(?i)table[\\s\n\r]*(([_a-zA-Z0-9])*)[\\s\n\r]*(?)AS[\\s\n\r]*\\(\\s*(?)SELECT")), CREATE_TABLE(
				Pattern.compile("[\\s\n\r]*(?i)create[\\s\n\r]*(?i)table[\\s\n\r]*(([_a-zA-Z0-9])*)")), DROP_TABLE(
				Pattern.compile("[\\s\n\r]*(?i)drop[\\s\n\r]*(?i)table[\\s\n\r]*(([_a-zA-Z0-9])*)"));
		private Pattern pattern;

		private DDLType(Pattern pattern) {
			this.pattern = pattern;
		}

		public boolean in(DDLType... types) {
			for (DDLType type : types) {
				if (this.equals(type)) {
					return true;
				}
			}
			return false;
		}

		public static DDLType getDDLType(String s) {
			DDLType[] dts = DDLType.values();
			for (DDLType cand : dts) {
				if (cand.pattern.matcher(s).find()) {
					return cand;
				}
			}
			return null;
		}

		public String getDBObjectName(String s) {
			Matcher m = pattern.matcher(s);
			if (m.find()) {
				return m.group(1);
			}
			return null;
		}
	}

	static void addWAFunctionName(String name) {
		waFunctions.add(name);
	}

	public static DDLType getDDLType(String s) {
		return DDLType.getDDLType(s);
	}

	private static String replaceWorkAroundFunctions(String sql) {
		for (String waFun : waFunctions) {
			sql = sql.replaceAll("(\\W)(?i)" + waFun + "\\s*\\(", "$1" + waFun
					+ "WA(");
		}
		sql = sql.replaceAll("(\\W)(?i)STDEV\\s*\\(", "$1STDDEV_SAMP(");
		sql = sql.replaceAll("(\\W)(?i)STDEVP\\s*\\(", "$1STDDEV_POP(");
		sql = sql.replaceAll("(\\W)(?i)VAR\\s*\\(", "$1VAR_SAMP(");
		sql = sql.replaceAll("(\\W)(?i)VARP\\s*\\(", "$1VAR_POP(");
		return sql.replaceAll(WA_CURRENT_USER, "$1user(");
	}

	public static String convertSQL(String sql, boolean creatingQuery) {
		return convertSQL(sql, null, creatingQuery);
	}

	public static String convertSQL(String sql, UcanaccessConnection conn,
			boolean creatingQuery) {
		sql = sql + " ";
		sql = convertUnion(sql);
		sql = convertOwnerAccess(sql);
		sql=  convertSwitch(sql);
		sql = convertAccessDate(sql);
		sql = convertQuotedAliases(sql);
		sql = replaceWorkAroundFunctions(sql);
		sql = escape(sql);
		sql = convertLike(sql);
		sql = replaceWhiteSpacedTables(sql);
		sql = replaceDistinctRow(sql);
		if (!creatingQuery) {
			Pivot.checkAndRefreshPivot(sql, conn);
			sql = DFunction.convertDFunctions(sql, conn);
		}
		
		sql = sql.trim();
		return sql;
	}

	private static String convertOwnerAccess(String sql) {
		return sql.replaceAll(WITH_OWNERACCESS_OPTION, "");
	}

	private static String convertSwitch(String sql) {
		for (Matcher mtc = SWITCH_PATTERN.matcher(sql); mtc.find();mtc =  SWITCH_PATTERN
				.matcher(sql)) {
			
			String g2=mtc.group(2);
			String baseSwitch= g2.substring(1,g2.length()-1);
			String[] elts=baseSwitch.split(",", -1);
			StringBuffer sb=new StringBuffer("(CASE ");
			for(int i=0;i<elts.length;++i){
				if(i==0||i%2==0){
					if(i==elts.length-1){
						sb.append(" ELSE").append(elts[i]);
					}else
					sb.append("  WHEN ").append( elts[i]).append(" THEN ");
				
				}else{
					sb.append( elts[i]);
				}
			}
			sb.append(" END )");
			sql=sql.substring(0,mtc.start())+sb.toString()+sql.substring(mtc.end());
						
			
		}
		return sql;
	}

	private static String convertUnion(String sql) {
		return sql.replaceAll(UNION, "$2$3$4");
	}

	private static String convertYesNo(String sql) {
		// TODO Auto-generated method stub
		  sql= sql.replaceAll(YES, "$1true$3");
		  Matcher mtc=NO_DATA_PATTERN.matcher(sql);
		  if(mtc.find()){
			  sql=sql.substring(0, mtc.start()).replaceAll(NO, "$1false$3")
					  +sql.substring(mtc.start());
		  }else{
			  sql= sql.replaceAll(NO, "$1false$3");
		  }
		  
		  return sql;
	}

	
	private static String convertQuotedAliases(String sql) {
		for (Matcher mtc = KIND_OF_SUBQUERY.matcher(sql); mtc.find();mtc =  SWITCH_PATTERN
				.matcher(sql)) {
			String g2=mtc.group(2).trim();
			if(g2.endsWith(";"))g2=g2.substring(0, g2.length()-1);
			sql=sql.substring(0,mtc.start())+"("+g2+")"+sql.substring(mtc.end());
		}
		HashSet<String> hs = new HashSet<String>();
		String sqle=sql;
		String sqlN="";
		for (Matcher mtc = QUOTED_ALIAS.matcher(sqle); mtc.find();mtc = QUOTED_ALIAS
				.matcher(sqle)) {
			
			String g2 = mtc.group(2);
			if (g2.indexOf('\'') >= 0 || g2.indexOf('"') >= 0) {
				hs.add(g2);
			}
			sqlN += sqle.substring(0, mtc.start()) + mtc.group(1)
					+ g2.replaceAll("[\'\"]", "") + mtc.group(3);
			sqle=sqle.substring(mtc.end());
			
		}
		sql=sqlN+sqle;
		for (String escaped : hs) {
			sql = sql.replaceAll(
					"\\[" + escaped.substring(1, escaped.length() - 1) + "\\]",
					escaped.replaceAll("[\'\"]", ""));
		}
		return sql;
	}

	private static String replaceDistinctRow(String sql) {
		return sql.replaceAll(DISTINCT_ROW, " DISTINCT ");
	}

	static void addWhiteSpacedTableNames(String name) {
		name = basicEscapingIdentifier(name);
		if (whiteSpacedTableNames.contains(name))
			return;
		for (String alrIn : whiteSpacedTableNames) {
			if (name.contains(alrIn)) {
				whiteSpacedTableNames.add(whiteSpacedTableNames.indexOf(alrIn),
						name);
				return;
			}
		}
		whiteSpacedTableNames.add(name);
	}

	public static String convertSQL(String sql) {
		return convertSQL(sql, null, false);
	}

	public static String convertSQL(String sql, UcanaccessConnection conn) {
		return convertSQL(sql, conn, false);
	}

	public static String convertAccessDate(String sql) {
		sql = sql
				.replaceAll("#" + DATE_ACCESS_FORMAT + "#",
						"Timestamp'$3-$1-$2 00:00:00'")
				// FORMAT MM/dd/yyyy
				.replaceAll(
						"#" + DATE_ACCESS_FORMAT + "\\s*("
								+ HHMMSS_ACCESS_FORMAT + ")#",
						"Timestamp'$3-$1-$2 $4'")
				.replaceAll(
						"#" + DATE_ACCESS_FORMAT + "\\s*("
								+ HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#",
						"Timestamp'$3-$1-$2 $4'")
				.replaceAll(
						"#" + DATE_ACCESS_FORMAT + "\\s*("
								+ HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#",
						"Timestamp'$3-$1-$2 $4'+ 12 Hour ")
				// FORMAT yyyy-MM-dd
				.replaceAll("#" + DATE_FORMAT + "#",
						"Timestamp'$1-$2-$3 00:00:00'")
				.replaceAll(
						"#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT
								+ ")#", "Timestamp'$1-$2-$3 $4'")
				.replaceAll(
						"#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT
								+ ")\\s*(?i)AM#", "Timestamp'$1-$2-$3 $4'")
				.replaceAll(
						"#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT
								+ ")\\s*(?i)PM#",
						"Timestamp'$1-$2-$3 $4'+ 12 Hour ")
				.replaceAll("#(" + HHMMSS_ACCESS_FORMAT + ")#",
						"Timestamp'" + BIG_BANG + " $1'")
				.replaceAll("#(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#",
						"Timestamp'" + BIG_BANG + " $1'")
				.replaceAll("#(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#",
						"Timestamp'" + BIG_BANG + " $1'+ 12 Hour");
		;
		return sql;
	}

	private static String replaceWhiteSpacedTables(String sql) {
		String[] sqls = sql.split("'", -1);
		StringBuffer sb = new StringBuffer();
		String cm = "";
		for (int i = 0; i < sqls.length; ++i) {
			sb.append(cm).append(
					i % 2 == 0 ? replaceWhiteSpacedTableNames0(sqls[i])
							: sqls[i]);
			cm = "'";
		}
		return sb.toString();
	}

	private static String replaceWhiteSpacedTableNames0(String sql) {
		if (whiteSpacedTableNames.size() == 0) {
			return sql;
		}
		StringBuffer sb = new StringBuffer("(");
		String or = "";
		for (String bst : whiteSpacedTableNames) {
			sb.append(or).append("(?i)" + bst);
			or = "|";
		}
		// workaround o.o. and l.o.
		for (String bst : whiteSpacedTableNames) {
			String dw = bst.replaceAll(" ", "  ");
			sql = sql.replaceAll(dw, bst);
		}
		sb.append(")");
		sql = sql.replaceAll("([^A-Za-z0-9\"])" + sb.toString()
				+ "([^A-Za-z0-9\"])", " $1\"$2\"$3");
		return sql;
	}

	public static String escapeKeyword(String identifier) {
		for (String bst : KEYWORDLIST) {
			identifier = identifier.replaceAll("(\\W)(?i)" + bst + "(\\W)",
					" $1\"" + bst + "\"$2");
		}
		return identifier;
	}

	private static String convertIdentifiers(String sql) {
		int init;
		if ((init = sql.indexOf("[")) != -1) {
			int end = sql.indexOf("]");
			if (end < init)
				return sql.replaceAll("&", "||");
			String content = sql.substring(init + 1, end);
			if (content.indexOf(" ") > 0) {
				String tryContent = " " + content + " ";
				String tryConversion = convertXescaped(tryContent);
				if (!tryConversion.equalsIgnoreCase(tryContent)) {
					identifiersContainingKeyword.put(tryConversion.trim(),
							content.toUpperCase());
				}
			}
			content = basicEscapingIdentifier(content).toUpperCase();
			String subs = content.indexOf(" ") > 0
					|| NO_ALFANUMERIC.matcher(content).find() ? "\"" : " ";
			sql = sql.substring(0, init).replaceAll("&", "||") + subs + content
					+ subs + convertIdentifiers(sql.substring(end + 1));
		} else {
			sql = sql.replaceAll("&", "||");
		}
		return sql;
	}

	private static String convertXescaped(String sqlc) {
		for (String xidt : xescapedIdentifiers) {
			sqlc = sqlc.replaceAll(XESCAPED.replaceAll("_", xidt), "$1X$2$3");
			sqlc = sqlc.replaceAll(XESCAPED_FUNCTIONS.replaceAll("_", xidt),
					"$1$2(");
		}
		return sqlc;
	}

	private static String convertPartIdentifiers(String sql) {
		String sqlc = convertIdentifiers(sql).replaceAll(
				DIGIT_STARTING_IDENTIFIERS, "$1Z_" + "$2$5").replaceAll(
				UNDERSCORE_IDENTIFIERS, "$1Z" + "$2$5");
		sqlc = convertXescaped(sqlc);
		for (Map.Entry<String, String> entry : identifiersContainingKeyword
				.entrySet()) {
			sqlc = sqlc.replaceAll("(?i)\"" + entry.getKey() + "\"", "\""
					+ entry.getValue() + "\"");
		}
		for (String xidt : KEYWORDLIST) {
			if (!xidt.equals("SELECT"))
				sqlc = sqlc.replaceAll(KEYWORD_ALIAS.replaceAll("_", xidt),
						"$1\"$2\"$3");
		}
		sqlc = convertYesNo(sqlc);
		return sqlc;
	}

	private static String escape(String sql) {
		Pattern pd = DOUBLE_QUOTE_G_PATTERN;
		Pattern ps = QUOTE_G_PATTERN;
		int li = Math.max(sql.lastIndexOf("\""), sql.lastIndexOf("'"));
		boolean enddq = sql.endsWith("\"") || sql.endsWith("'");
		String suff = enddq ? "" : sql.substring(li + 1);
		suff = convertPartIdentifiers(suff);
		String tsql = enddq ? sql : sql.substring(0, li + 1);
		Matcher md = pd.matcher(tsql);
		Matcher ms = ps.matcher(tsql);
		boolean fd = md.find();
		boolean fs = ms.find();
		if (fd || fs) {
			boolean inid = !fs || (fd && md.start(0) < ms.start(0));
			String group, str;
			Matcher mcr = inid ? md : ms;
			group = mcr.group(1);
			if (inid)
				group = group.replaceAll("'", "''").replaceAll("\"\"", "\"");
			str = tsql.substring(0, mcr.start(0));
			str = convertPartIdentifiers(str);
			tsql = str + "'" + group + "'" + escape(tsql.substring(mcr.end(0)));
		} else {
			tsql = convertPartIdentifiers(tsql);
		}
		return tsql + suff;
	}

	public static String basicEscapingIdentifier(String name) {
		if (name.startsWith("~"))
			return null;
		String nl = name.toUpperCase();
		if (Database.isReservedWord(nl) && NO_SQL_RESERVED_WORDS.contains(nl)
				) {
			xescapedIdentifiers.add(nl);
		}
		
		
		
		String escaped = Database
				.escapeIdentifier(
						name// .replaceAll(" ", "_")
						.replaceAll("[/\\\\$%^:-]", "_").replaceAll("~", "M_")
								.replaceAll("\\.", "_")).replaceAll("\'", "").replaceAll("#", "_")
				.replaceAll("\"", "").replaceAll("\\+", "").replaceAll("\\(", "_").replaceAll("\\)", "_");
		
		
		
		if (KEYWORDLIST.contains(escaped.toUpperCase())) {
			escaped = "\"" + escaped + "\"";
		}
		if (Character.isDigit(escaped.trim().charAt(0))) {
			escaped = "Z_" + escaped.trim();
		}
		if (escaped.charAt(0) == '_') {
			escaped = "Z" + escaped;
		}
		return escaped.toUpperCase();
	}

	public static String escapeIdentifier(String name) {
		String escaped = basicEscapingIdentifier(name);
		if (escaped == null)
			return null;
		if (escaped.indexOf(" ") > 0) {
			escaped = "\"" + escaped + "\"";
		}
		return escaped;
	}

	private static String convertCreateTable(String sql,
			Map<String, String> types2Convert) {
		// padding for detecting the right exception
		sql += " ";
		for (Map.Entry<String, String> entry : types2Convert.entrySet()) {
			sql = sql.replaceAll(
					TYPES_TRANSLATE.replaceAll("_", entry.getKey()), "$1"
							+ entry.getValue() + "$2");
			
		}
		sql = sql.replaceAll(DEFAULT_VARCHAR,
				"$1VARCHAR(255)$2");
		return sql;
	}

	public static String convertCreateTable(String sql) {
		return convertCreateTable(sql, TypesMap.getAccess2HsqlTypesMap());
	}

	public static boolean checkDDL(String sql) {
		if (sql == null)
			return false;
		return CHECK_DDL.matcher(sql).matches();
	}

	private static String convertLike(String sql) {
		Pattern ptfl = FIND_LIKE_PATTERN;
		Matcher matcher = ptfl.matcher(sql);
		if (matcher.find()) {
			return sql.substring(0, matcher.start(1))
					+ convertLike(matcher.group(1), matcher.group(2),
							matcher.group(3))
					+ convertLike(sql.substring(matcher.end(0)));
		} else
			return sql;
	}

	private static String convert2RegexMatches(String likeContent) {
		Pattern ptn = ACCESS_LIKE_ESCAPE_PATTERN;
		Matcher mtc = ptn.matcher(likeContent);
		if (mtc.find()) {
			return convert2RegexMatches(likeContent.substring(0, mtc.start(0)))
					+ mtc.group(0).substring(1, 2)
					+ convert2RegexMatches(likeContent.substring(mtc.end(0)));
		}
		return likeContent.replaceAll("#", "\\\\d").replaceAll("\\*", ".*")
				.replaceAll("_", ".")
				.replaceAll("(\\[)\\!(\\w\\-\\w\\])", "$1^$2")
				+ "')";
	}

	private static String convert2LikeCondition(String likeContent) {
		Pattern ptn = ACCESS_LIKE_ESCAPE_PATTERN;
		Matcher mtc = ptn.matcher(likeContent);
		if (mtc.find()) {
			return convert2LikeCondition(likeContent.substring(0, mtc.start(0)))
					+ mtc.group(0).substring(1, 2)
					+ convert2LikeCondition(likeContent.substring(mtc.end(0)));
		}
		return likeContent.replaceAll("\\*", "%").replaceAll("\\?", "_");
	}

	private static String convertLike(String conditionField, String closePar,
			String likeContent) {
		Pattern inter = ACCESS_LIKE_CHARINTERVAL_PATTERN;
		if (likeContent.indexOf("#") >= 0 || inter.matcher(likeContent).find()) {
			return "REGEXP_MATCHES(" + conditionField + ",'"
					+ convert2RegexMatches(likeContent) + closePar + " ";
		}
		return " " + conditionField + closePar + " like '"
				+ convert2LikeCondition(likeContent) + "'";
	}

	public static boolean isSupportsAccessLike() {
		return supportsAccessLike;
	}

	public static void setSupportsAccessLike(boolean supportsAccessLike) {
		SQLConverter.supportsAccessLike = supportsAccessLike;
	}

	public static boolean isXescaped(String identifier) {
		return xescapedIdentifiers.contains(identifier);
	}
	
	
}
