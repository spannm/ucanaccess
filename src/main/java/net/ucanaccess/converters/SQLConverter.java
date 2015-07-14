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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ucanaccess.jdbc.NormalizedSQL;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;

import com.healthmarketscience.jackcess.TableBuilder;

public class SQLConverter {
	private static final Pattern QUOTE_S_PATTERN = Pattern.compile("(')+");
	private static final Pattern DOUBLE_QUOTE_S_PATTERN = Pattern
			.compile("(\")+");
	
	private static final Pattern SELECT_FROM_PATTERN_START = Pattern.compile("[\\s\n\r]*(?i)SELECT[\\s\n\r]+");
	private static final Pattern SELECT_FROM_PATTERN_END= Pattern.compile("[\\s\n\r]*(?i)FROM[\\s\n\r\\[]+");
	private static final Pattern UNESCAPED_ALIAS= Pattern.compile("[\\s\n\r]*(?i)AS[\\s\n\r]*");
	
	private static final Pattern QUOTE_M_PATTERN = Pattern
			.compile("'(([^'])*)'");
	private static final Pattern DOUBLE_QUOTE_M_PATTERN = Pattern
			.compile("\"(([^\"])*)\"");
	private static final Pattern FIND_LIKE_PATTERN = Pattern
			.compile("[\\s\n\r\\(]*([\\w\\.]*)([\\s\n\r\\)]*)(?i)LIKE[\\s\n\r]*\'([^']*(?:'')*)\'");
	private static final Pattern ACCESS_LIKE_CHARINTERVAL_PATTERN = Pattern
			.compile("\\[(?:\\!*[a-zA-Z]\\-[a-zA-Z])+\\]");
	private static final Pattern ACCESS_LIKE_ESCAPE_PATTERN = Pattern
			.compile("\\[[\\*|_|#]\\]");
	private static final Pattern CHECK_DDL = Pattern
			.compile("^([\n\r\\s]*(?i)(create|alter|drop|enable|disable))[\n\r\\s]+.*");
	private static final Pattern KIND_OF_SUBQUERY = Pattern
			.compile("(\\[)(((?i) FROM )*((?i)SELECT )*([^\\]])*)(\\]\\.[\\s\n\r])");
	
	private static final Pattern NO_DATA_PATTERN = Pattern
			.compile(" (?i)WITH[\\s\n\r]+(?i)NO[\\s\n\r]+(?i)DATA");
	private static final Pattern NO_ALFANUMERIC = Pattern.compile("\\W");
	private static final String IDENTITY ="(\\W+)((?i)@@identity)(\\W*)";
	private static final Pattern SELECT_IDENTITY = Pattern.compile("(?i)select[\\s\n\r]+(?i)@@identity.*");
	private static final Pattern HAS_FROM = Pattern.compile("[\\s\n\r]+(?i)from[\\s\n\r]+");
	private static final Pattern FORMULA_DEPENDENCIES=Pattern.compile("\\[([^\\]]*)\\]");
	private static final String EXCLAMATION_POINT="(\\!)([\n\r\\s]*)([^\\=])";
	
	private static final String YES = "(\\W)((?i)YES)(\\W)";
	private static final String NO = "(\\W)((?i)NO)(\\W)";
	private static final String WITH_OWNERACCESS_OPTION = "(\\W)(?i)WITH[\\s\n\r]+(?i)OWNERACCESS[\\s\n\r]+(?i)OPTION(\\W)";
	private static final Pattern DIGIT_STARTING_IDENTIFIERS = Pattern
			.compile("(\\W)(([0-9])+(([_a-zA-Z])+([0-9])*)+)(\\W)");
	private static final String UNDERSCORE_IDENTIFIERS = "(\\W)((_)+([_a-zA-Z0-9])+)(\\W)";
	private static final String XESCAPED = "(\\W)((?i)X)((?i)_)(\\W)";
	private static final String KEYWORD_ALIAS = "([\\s\n\r]+(?i)AS[\\s\n\r]*)((?i)_)(\\W)";
	public static final String[] DEFAULT_CATCH = new String[] {
			"([\\s\n\r]*(?i)DEFAULT[\\s\n\r]+)(\'(?:[^']*(?:'')*)*\')([\\s\n\r\\)\\,])",
			"([\\s\n\r]*(?i)DEFAULT[\\s\n\r]+)(\"(?:[^\"]*(?:\"\")*)*\")([\\s\n\r\\)\\,])",
			"([\\s\n\r]*(?i)DEFAULT[\\s\n\r]+)([0-9\\.\\-\\+]+)([\\s\n\r\\)\\,])",
			"([\\s\n\r]*(?i)DEFAULT[\\s\n\r]+)([_0-9a-zA-Z]*\\([^\\)]*\\))([\\s\n\r\\)\\,])" };
	private static final Pattern QUOTED_ALIAS = Pattern
			.compile("([\\s\n\r]+(?i)AS[\\s\n\r]*)(\\[[^\\]]*\\])(\\W)");
	private static final String TYPES_TRANSLATE = "(\\W)(?i)_(\\W)";
	private static final String DATE_ACCESS_FORMAT = "(0[1-9]|[1-9]|1[012])/(0[1-9]|[1-9]|[12][0-9]|3[01])/(\\d\\d\\d\\d)";
	private static final String DATE_FORMAT = "(\\d\\d\\d\\d)-(0[1-9]|[1-9]|1[012])-(0[1-9]|[1-9]|[12][0-9]|3[01])";
	private static final String HHMMSS_ACCESS_FORMAT = "([0-9]|0[0-9]|1[0-9]|2[0-4]):([0-9]|[0-5][0-9]):([0-9]|[0-5][0-9])";
	private static final String UNION = "(;)([\\s\n\r]*)((?i)UNION)([\\s\n\r]*)";
	private static final String DISTINCT_ROW = "[\\s\n\r]+(?i)DISTINCTROW[\\s\n\r]+";
	private static final String DEFAULT_VARCHAR = "(\\W)(?i)VARCHAR([\\s\n\r,\\)])";
	private static final String BACKTRIK = "(`)([^`]*)(`)";
	private static final String DELETE_ALL ="((?i)DELETE[\\s\n\r]+)(\\*)([\\s\n\r]+(?i)FROM[\\s\n\r]+)";
	
	private static final Pattern ESPRESSION_DIGIT = Pattern
			.compile("([\\d]+)(?![\\.\\d])");
	public static final String BIG_BANG = "1899-12-30";
	public static final HashMap<String, String> noRomanCharacters = new HashMap<String, String>();
	private static final List<String> KEYWORDLIST = Arrays.asList("ALL", "AND",
			"ANY","ALTER", "AS", "AT", "AVG", "BETWEEN", "BOTH", "BY", "CALL", "CASE",
			"CAST","CHECK", "COALESCE", "CORRESPONDING", "CONVERT", "COUNT", "CREATE",
			"CROSS", "DEFAULT", "DISTINCT", "DROP", "ELSE", "EVERY", "EXISTS",
			"EXCEPT", "FOR","FOREIGN", "FROM", "FULL", "GRANT", "GROUP", "HAVING", "IN",
			"INNER", "INTERSECT", "INTO", "IS", "JOIN", "LEFT", "LEADING",
			"LIKE", "MAX", "MIN", "NATURAL", "NOT", "NULLIF", "ON", "ORDER",
			"OR", "OUTER", "PRIMARY", "REFERENCES", "RIGHT", "SELECT", "SET",
			"SOME", "STDDEV_POP", "STDDEV_SAMP", "SUM", "TABLE", "THEN", "TO",
			"TRAILING", "TRIGGER", "UNION", "UNIQUE", "USING", "VALUES",
			"VAR_POP", "VAR_SAMP", "WHEN", "WHERE", "WITH", "END", "DO",
			"CONSTRAINT"
			,"USER"
			);
	
	private static final List<String> PROCEDURE_KEYWORDLIST = Arrays.asList("NEW","ROW");
	private static ArrayList<String> whiteSpacedTableNames = new ArrayList<String>();
	private static final HashSet<String> xescapedIdentifiers = new HashSet<String>();
	private static final HashSet<String> alreadyEscapedIdentifiers = new HashSet<String>();
	private static final HashMap<String, String> identifiersContainingKeyword = new HashMap<String, String>();
	private static final HashSet<String> apostrophisedNames=new HashSet<String>();
	
	private static final HashSet<String> waFunctions = new HashSet<String>();
	
	private static boolean supportsAccessLike = true;
	private static boolean dualUsedAsTableName=false;
	static {
		noRomanCharacters.put("\u20ac", "EUR");
		noRomanCharacters.put("\u00B9", "1");
		noRomanCharacters.put("\u00B2", "2");
		noRomanCharacters.put("\u00B3", "3");
		noRomanCharacters.put("\u00BC", "1_4");
		noRomanCharacters.put("\u00BD", "1_2");
		noRomanCharacters.put("\u00BE", "3_4");
		noRomanCharacters.put("\u00D0", "D");
		noRomanCharacters.put("\u00D7", "X");
		noRomanCharacters.put("\u00DE", "P");
		noRomanCharacters.put("\u00F0", "O");
		noRomanCharacters.put("\u00FD", "Y");
		noRomanCharacters.put("\u00FE", "P");
	}
	
	
	public static boolean hasIdentity(String sql ){
		return sql.indexOf("@@")>0&& sql.toUpperCase().indexOf("@@IDENTITY")>0;
	}
	
		
	private static void aliases(String sql,NormalizedSQL nsql ){
		Matcher mtc=SELECT_FROM_PATTERN_START.matcher(sql);
		if(mtc.find()){
			
			int init=mtc.end();
			sql=sql.substring(init);
			mtc=SELECT_FROM_PATTERN_END.matcher(sql);
			if(mtc.find()){
				int end=mtc.start();
				sql=sql.substring(0, end);
				
				for(mtc=UNESCAPED_ALIAS.matcher(sql);mtc.find(); mtc=UNESCAPED_ALIAS.matcher(sql)){
					int e=mtc.end();
					
					sql=sql.substring(e)+" ";
					char[] sqlc=sql.toCharArray();
					if (sqlc[0]=='[')continue;
					StringBuffer sb=new StringBuffer();
					for(char c:sqlc){
						if(c==' ' || c=='\n'||c=='\r'||c==','){
							String key=SQLConverter.preEscapingIdentifier(sb.toString());
							nsql.put(key, sb.toString());
							break;
						}
						else sb.append(c);
					}
				}
				
			}
		}
	}
	
	
	public static String preprocess(String sql ,Object key){
		
		Matcher mtc=SELECT_IDENTITY.matcher(sql);
		Matcher mtc1=HAS_FROM.matcher(sql);
		String end=mtc.matches()&&!mtc1.find()?" FROM DUAL":"";
		if(key instanceof String)key="'"+key+"'";
		return sql.replaceAll(IDENTITY, "$1"+key+"$3")+end;
	}
	
	public static boolean isListedAsKeyword(String s){
		return KEYWORDLIST.contains(s.toUpperCase()) ;
	}

	private static int[] getQuoteGroup(String s) {
		if (s.indexOf("''") < 0) {
			Matcher mtc = QUOTE_M_PATTERN.matcher(s);
			return mtc.find() ? new int[] { mtc.start(), mtc.end() } : null;
		} else {
			int[] ret = new int[] { -1, -1 };
			Pattern pt = QUOTE_S_PATTERN;
			Matcher mc = pt.matcher(s);
			while (mc.find()) {
				int start = mc.start();
				int end = mc.end();
				if ((end - start) % 2 == 0) {
					if (ret[0] == -1) {
						return new int[] { mc.start(), mc.end() };
					}
					continue;
				} else {
					if (ret[0] == -1)
						ret[0] = mc.start();
					else {
						ret[1] = mc.end();
						return ret;
					}
				}
			}
			return null;
		}
	}

	private static int[] getDoubleQuoteGroup(String s) {
		if (s.indexOf("\"\"") < 0) {
			Matcher mtc = DOUBLE_QUOTE_M_PATTERN.matcher(s);
			return mtc.find() ? new int[] { mtc.start(), mtc.end() } : null;
		} else {
			int[] ret = new int[] { -1, -1 };
			Pattern pt = DOUBLE_QUOTE_S_PATTERN;
			Matcher mc = pt.matcher(s);
			while (mc.find()) {
				int start = mc.start();
				int end = mc.end();
				if ((end - start) % 2 == 0) {
					if (ret[0] == -1) {
						return new int[] { mc.start(), mc.end() };
					}
					continue;
				} else {
					if (ret[0] == -1)
						ret[0] = mc.start();
					else {
						ret[1] = mc.end();
						return ret;
					}
				}
			}
			return null;
		}
	}

	public static enum DDLType {
		CREATE_TABLE_AS_SELECT(
				Pattern
						.compile("[\\s\n\r]*(?i)create[\\s\n\r]+(?i)table[\\s\n\r]+(([_a-zA-Z0-9])*)[\\s\n\r]*(?)AS[\\s\n\r]*\\(\\s*(?)SELECT")), CREATE_TABLE(
				Pattern
						.compile("[\\s\n\r]*(?i)create[\\s\n\r]+(?i)table[\\s\n\r]+(([_a-zA-Z0-9])+|\\[([^\\]])*\\]|(`([^`])*`))")), DROP_TABLE(
				Pattern
						.compile("[\\s\n\r]*(?i)drop[\\s\n\r]+(?i)table[\\s\n\r]+(([_a-zA-Z0-9])+|\\[([^\\]])*\\]|`([^`])*`)")),
				DISABLE_AUTOINCREMENT(Pattern
						.compile("[\\s\n\r]*(?i)disable[\\s\n\r]+(?i)autoincrement[\\s\n\r]+(?i)on[\\s\n\r]+(([_a-zA-Z0-9])+|\\[([^\\]])*\\]|`([^`])*`)")),
				ENABLE_AUTOINCREMENT(Pattern
								.compile("[\\s\n\r]*(?i)enable[\\s\n\r]+(?i)autoincrement[\\s\n\r]+(?i)on[\\s\n\r]+(([_a-zA-Z0-9])+|\\[([^\\]])*\\]|`([^`])*`)"))
						;
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
		return sql.replaceAll("(\\W)(?i)currentUser\\s*\\(", "$1user(");
	}

	public static String restoreWorkAroundFunctions(String sql) {
		for (String waFun : waFunctions) {
			sql = sql.replaceAll("(\\W)(?i)" + waFun + "WA\\s*\\(", "$1"
					+ waFun + "(");
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
		for(String an:apostrophisedNames)
			sql= sql.replaceAll("(?i)"+Pattern.quote("["+an+"]"), "["+SQLConverter.escapeIdentifier(an)+"]");
		return sql;
	}

	public static NormalizedSQL convertSQL(String sql, boolean creatingQuery) {
		return convertSQL(sql, null, creatingQuery);
	}
	
		
	public static NormalizedSQL convertSQL(String sql, UcanaccessConnection conn,
			boolean creatingQuery) {
		NormalizedSQL nsql=new NormalizedSQL();
		sql = sql + " ";
		aliases(sql, nsql );
		sql = replaceBacktrik(sql);
		sql = replaceAposNames(sql);
		sql = convertUnion(sql);
		sql = convertAccessDate(sql);
		sql = convertQuotedAliases(sql,nsql);
		sql = escape(sql);
		sql = convertLike(sql);
		sql = replaceWhiteSpacedTables(sql);
		sql = replaceExclamationPoints(sql);
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
			sql = sql.substring(0, mtc.start()).replaceAll(NO, "$1false$3")
					+ sql.substring(mtc.start());
		} else {
			sql = sql.replaceAll(NO, "$1false$3");
		}
		return sql;
	}
	
	

	private static String convertQuotedAliases(String sql,NormalizedSQL nsql) {
		for (Matcher mtc = KIND_OF_SUBQUERY.matcher(sql); mtc.find(); mtc = KIND_OF_SUBQUERY 
				.matcher(sql)) {
			String g2 = mtc.group(2).trim();
			if (g2.endsWith(";"))
				g2 = g2.substring(0, g2.length() - 1);
			sql = sql.substring(0, mtc.start()) + "(" + g2 + ")"
					+ sql.substring(mtc.end());
		}
		HashSet<String> hs = new HashSet<String>();
		String sqle = sql;
		String sqlN = "";
		for (Matcher mtc = QUOTED_ALIAS.matcher(sqle); mtc.find(); mtc = QUOTED_ALIAS
				.matcher(sqle)) {
			String g2 = mtc.group(2);
			if (g2.indexOf('\'') >= 0 || g2.indexOf('"') >= 0) {
				hs.add(g2);
			}
			String value=g2.substring(1,g2.length()-1);
			nsql.put(SQLConverter.preEscapingIdentifier(value), value);
			sqlN += sqle.substring(0, mtc.start()) + mtc.group(1)
					+ g2.replaceAll("[\'\"]", "") + mtc.group(3);
			sqle = sqle.substring(mtc.end());
		}
		sql = sqlN + sqle;
		for (String escaped : hs) {
			sql = sql.replaceAll("\\["
					+ escaped.substring(1, escaped.length() - 1) + "\\]",
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

	public static NormalizedSQL convertSQL(String sql) {
		return convertSQL(sql, null, false);
	}

	public static NormalizedSQL convertSQL(String sql, UcanaccessConnection conn) {
		return convertSQL(sql, conn, false);
	}
	
	public static String convertAccessDate(String sql) {
		sql = sql.replaceAll("#" + DATE_ACCESS_FORMAT + "#",
				"Timestamp'$3-$1-$2 00:00:00'")
				// FORMAT MM/dd/yyyy
				.replaceAll(
						"#" + DATE_ACCESS_FORMAT + "\\s*("
								+ HHMMSS_ACCESS_FORMAT + ")#",
						"Timestamp'$3-$1-$2 $4'").replaceAll(
						"#" + DATE_ACCESS_FORMAT + "\\s*("
								+ HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#",
						"Timestamp'$3-$1-$2 $4'").replaceAll(
						"#" + DATE_ACCESS_FORMAT + "\\s*("
								+ HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#",
						"Timestamp'$3-$1-$2 $4'+ 12 Hour ")
				// FORMAT yyyy-MM-dd
				.replaceAll("#" + DATE_FORMAT + "#",
						"Timestamp'$1-$2-$3 00:00:00'").replaceAll(
						"#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT
								+ ")#", "Timestamp'$1-$2-$3 $4'").replaceAll(
						"#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT
								+ ")\\s*(?i)AM#", "Timestamp'$1-$2-$3 $4'")
				.replaceAll(
						"#" + DATE_FORMAT + "\\s*(" + HHMMSS_ACCESS_FORMAT
								+ ")\\s*(?i)PM#",
						"Timestamp'$1-$2-$3 $4'+ 12 Hour ").replaceAll(
						"#(" + HHMMSS_ACCESS_FORMAT + ")#",
						"Timestamp'" + BIG_BANG + " $1'").replaceAll(
						"#(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#",
						"Timestamp'" + BIG_BANG + " $1'").replaceAll(
						"#(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#",
						"Timestamp'" + BIG_BANG + " $1'+ 12 Hour");
		
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

	private static String convertIdentifiers(String sql) {
		int init;
		if ((init = sql.indexOf("[")) != -1) {
			int end = sql.indexOf("]");
			if (end < init)
				return convertResidualSQL(sql);
			String content = sql.substring(init + 1, end);
			if (content.indexOf(" ") > 0) {
				String tryContent = " " + content + " ";
				String tryConversion = convertXescaped(tryContent);
				if (!tryConversion.equalsIgnoreCase(tryContent)) {
					identifiersContainingKeyword.put(tryConversion.trim(),
							content.toUpperCase());
				}
			}
			boolean isKeyword = KEYWORDLIST.contains(content.toUpperCase());
			
			content = basicEscapingIdentifier(content).toUpperCase();
			String subs = !isKeyword
					&& (content.indexOf(" ") > 0 || NO_ALFANUMERIC.matcher(
							content).find()) ? "\"" : " ";
			sql = convertResidualSQL(sql.substring(0, init)) + subs + content
					+ subs + convertIdentifiers(sql.substring(end + 1));
		} else {
			sql = convertResidualSQL(sql);
		}
		return sql;
	}

	private static String convertResidualSQL(String sql) {
		sql = convertSQLTokens(sql);
		return replaceDigitStartingIdentifiers(sql.replaceAll(
				UNDERSCORE_IDENTIFIERS, "$1Z$2$5"));
	}

	private static String convertSQLTokens(String sql) {
		return  convertDeleteAll(replaceWorkAroundFunctions(convertOwnerAccess(replaceDistinctRow(convertYesNo(sql
				.replaceAll("&", "||"))))));
	}

	private static String replaceDigitStartingIdentifiers(String sql) {
		Matcher mtc = DIGIT_STARTING_IDENTIFIERS.matcher(sql);
		if (mtc.find()) {
			String prefix = (mtc.group(0).matches("\\.([0-9])+[Ee]([0-9])+\\s") || mtc
					.group(0).matches("\\.([0-9])+[Ee][-+]")) ? "" : "Z_";
			String build = mtc.group(1) + prefix + mtc.group(2) + mtc.group(7);
			sql = sql.substring(0, mtc.start()) + build
					+ replaceDigitStartingIdentifiers(sql.substring(mtc.end()));
		}
		return sql;
	}

	private static String convertXescaped(String sqlc) {
		for (String xidt : xescapedIdentifiers) {
			sqlc = sqlc.replaceAll(XESCAPED.replaceAll("_", xidt), "$1$3$4");
		}
		return sqlc;
	}

	private static String convertPartIdentifiers(String sql) {
		String sqlc = convertIdentifiers(sql);
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
		return sqlc;
	}

	private static String escape(String sql) {
		int li = Math.max(sql.lastIndexOf("\""), sql.lastIndexOf("'"));
		boolean enddq = sql.endsWith("\"") || sql.endsWith("'");
		String suff = enddq ? "" : sql.substring(li + 1);
		suff = convertPartIdentifiers(suff);
		String tsql = enddq ? sql : sql.substring(0, li + 1);
		int[] fd = getDoubleQuoteGroup(tsql);
		int[] fs = getQuoteGroup(tsql);
		if (fd != null || fs != null) {
			boolean inid = fs == null || (fd != null && fd[0] < fs[0]);
			String group, str;
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
		xescapedIdentifiers.removeAll(alreadyEscapedIdentifiers);
	}
	
	
	
	public static String procedureEscapingIdentifier(String name) {
		if (PROCEDURE_KEYWORDLIST.contains(name.toUpperCase())) {
			name = "\"" + name.toUpperCase() + "\"";
			
		}
		return name;
	}
	
	
	public static String preEscapingIdentifier(String name) {
		if(name.length()==0)return name;
		if (name.startsWith("~"))
			return null;
		String nl = name.toUpperCase();
		if (TableBuilder.isReservedWord(nl)) {
			xescapedIdentifiers.add(nl);
		}
		if(name.indexOf("'")>=0||name.indexOf("\"")>0){
			apostrophisedNames.add(name);
		}
				
		if (nl.startsWith("X") && TableBuilder.isReservedWord(nl.substring(1))) {
			alreadyEscapedIdentifiers.add(nl.substring(1));
		}
		
		String escaped = name;
		escaped = name.replaceAll(
				"\'", "").replaceAll("\"", "").replaceAll(Pattern.quote("\\"), "_");
    	
		if (Character.isDigit(escaped.trim().charAt(0))) {
			escaped = "Z_" + escaped.trim();
		}
		if (escaped.charAt(0) == '_') {
			escaped = "Z" + escaped;
		}
		if (dualUsedAsTableName&&escaped.equalsIgnoreCase("DUAL")) {
			escaped = "DUAL_13031971";
		}
		
		return escaped.toUpperCase();
	}
	
	private static String escapeKeywordIdentifier(String escaped,boolean quote) {
		if (KEYWORDLIST.contains(escaped.toUpperCase())) {
			escaped =quote? "\"" + escaped + "\"":"[" + escaped + "]";
		}
		return escaped;
	}
	
	public static String basicEscapingIdentifier(String name) {
		return escapeKeywordIdentifier(preEscapingIdentifier(name),true);
		
	}

	public static String escapeIdentifier(String name, Connection conn) throws SQLException {
		 return checkLang(escapeIdentifier(name),conn,true);
	}
	
	public static String completeEscaping(String escaped, boolean quote){
		return hsqlEscape(escapeKeywordIdentifier(escaped,quote),quote);
	}
	
	public static String completeEscaping(String escaped){
		return completeEscaping(escaped,true);
	}
	
	
	private static String hsqlEscape(String escaped,boolean quote){
		if (escaped.indexOf(" ") > 0) {
			escaped =quote? "\"" + escaped + "\"":"[" + escaped + "]";
		}
		return escaped;
	} 

	public static String checkLang(String name,Connection conn) throws SQLException{
		return checkLang(name,conn,true);
	}
	
	public static String checkLang(String name,Connection conn,boolean quote) throws SQLException{
		Statement st=null;
		try{
			String name0=name;
		  if(!quote){
			  name0=name.replaceAll(Pattern.quote("["), "\"").replaceAll(Pattern.quote("]"), "\"");
			  }
		  st=conn.createStatement();
		  st.execute("SELECT 1 AS "+name0+" from dual");
		  return name;
		}
		catch(SQLException e){
		    return quote?"\""+name+"\"":"["+name+"]";
		}
		
		finally{
			if(st!=null)st.close();
			}
	}


	public static String escapeIdentifier(String name) {
		String escaped = basicEscapingIdentifier(name);
		
		if (escaped == null)
			return null;
		return hsqlEscape(escaped,true);
	}
	
	
	private static String convertCreateTable(String sql,
			Map<String, String> types2Convert) throws SQLException {
		// padding for detecting the right exception
		sql += " ";
		for (Map.Entry<String, String> entry : types2Convert.entrySet()) {
			sql = sql.replaceAll(TYPES_TRANSLATE
					.replaceAll("_", entry.getKey()), "$1" + entry.getValue()
					+ "$2");
		}
		sql = sql.replaceAll(DEFAULT_VARCHAR, "$1VARCHAR(255)$2");
		return clearDefaultsCreateStatement(sql);
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

	private static String clearDefaultsCreateStatement(String sql)
			throws SQLException {
		if (sql.toUpperCase().indexOf("DEFAULT") < 0)
			return sql;
		int startDecl = sql.indexOf('(');
		int endDecl = sql.lastIndexOf(')');
		if (startDecl >= endDecl) {
			throw new UcanaccessSQLException(
					ExceptionMessages.INVALID_CREATE_STATEMENT);
		}
		for (String pattern : DEFAULT_CATCH)
			sql = sql.replaceAll(pattern, "$3");

		return sql;
	}

	public static String convertCreateTable(String sql) throws SQLException {
		return convertCreateTable(sql, TypesMap.getAccess2HsqlTypesMap());
	}

	public static boolean checkDDL(String sql) {
		if (sql == null)
			return false;
		return CHECK_DDL.matcher(sql.replaceAll("[\n\r]", " ")).matches();
	}

	private static String convertLike(String sql) {
		Pattern ptfl = FIND_LIKE_PATTERN;
		Matcher matcher = ptfl.matcher(sql);
		if (matcher.find()) {
			return sql.substring(0, matcher.start(1))
					+ convertLike(matcher.group(1), matcher.group(2), matcher
							.group(3))
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
				.replaceAll("_", ".").replaceAll("(\\[)\\!(\\w\\-\\w\\])",
						"$1^$2")
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

	public static String convertFormula(String sql) {
		//white space to allow replaceWorkAroundFunction pattern to work fine
		sql = convertFormula0(convertSQL(" "+sql).getSql());
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
			if(replace){
				return sql.substring(0, idx)+mtc.group(1)+"E0"+ convertDigit( sql.substring(idxe));
			}else 
				return sql.substring(0, idxe)+convertDigit( sql.substring(idxe));
		}else return sql;
		
	}

	private static String convertFormula0(String sql) {
		int li = Math.max(sql.lastIndexOf("\""), sql.lastIndexOf("'"));
		boolean enddq = sql.endsWith("\"") || sql.endsWith("'");
		String suff = enddq ? "" : sql.substring(li + 1);
		suff = convertDigit(suff);
		String tsql = enddq ? sql : sql.substring(0, li + 1);
		int[] fd = getDoubleQuoteGroup(tsql);
		int[] fs = getQuoteGroup(tsql);
		if (fd != null || fs != null) {
			boolean inid = fs == null || (fd != null && fd[0] < fs[0]);
			String group, str;
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
		int i = sql.indexOf("^");
		if (i < 0) {
			return sql;
		}
		while ((i = sql.indexOf("^")) >= 0) {
			int foi = firstOperandIndex(sql, i);
			int loi = i + secondOperandIndex(sql, i) + 1;
			sql = sql.substring(0, foi) + " (power(" + sql.substring(foi, i)
					+ "," + sql.substring(i + 1, loi + 1) + "))"
					+ sql.substring(loi + 1);
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
			if (c == ' ')
				continue;

			if (foundType) {
				if (field && c == ']') {
					return j;
				} else if (digit && !Character.isDigit(c) && c != '.') {
					return j - 1;
				} else if (group) {
					if (c == '(')
						countPar++;
					if (c == ')') {
						countPar--;
						if (countPar == 0)
							return j;
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
					if (j + 1 < ca.length && ca[j + 1] != '('
							&& ca[j + 1] != '[') {
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

			if (c == ' ')
				continue;
			if (foundType) {
				if (field && c == '[') {
					return j;
				} else if (digit && !Character.isDigit(c) && c != '.') {
					if ((c == '+' || c == '-') && j > 0
							&& (ca[j - 1] == '+' || ca[j - 1] == '-')) {
						return j;
					}

					return j + 1;
				} else if (group) {
					if (c == ')')
						countPar++;

					if (c == '(') {
						countPar--;
						if (countPar == 0)
							return j;
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

	public static int asUnsigned(byte a) {
		int b = ((a & 0xFF));
		return ((b));
	}
	
	
	public static Set<String> getFormulaDependencies(String formula){
		Matcher mtc=FORMULA_DEPENDENCIES.matcher(formula);
		HashSet<String> fd=new HashSet<String> ();
		while(mtc.find()){
		  fd.add(escapeIdentifier(mtc.group(1)));
		}
		return fd;
	}


	static boolean isDualUsedAsTableName() {
		return dualUsedAsTableName;
	}


	static void setDualUsedAsTableName(boolean dualUsedAsTableName) {
		SQLConverter.dualUsedAsTableName = dualUsedAsTableName;
	}

	
}
