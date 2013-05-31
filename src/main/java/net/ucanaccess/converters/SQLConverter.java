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
import java.util.HashSet;


import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ucanaccess.jdbc.UcanaccessConnection;

import com.healthmarketscience.jackcess.Database;

public class SQLConverter {
	private static final Pattern	 QUOTE_G_PATTERN = Pattern
				.compile("\'(((([^'])*('')*))*)\'");
	 
	private static final Pattern DOUBLE_QUOTE_G_PATTERN = Pattern
			.compile("\"(((([^\"])*(\"\")*))*)\"");
	
	private static final Pattern FIND_LIKE_PATTERN = Pattern
			.compile("[\\s\\(]*([\\w\\.]*)([\\s\\)]*)(?i)like\\s*\\'(.*)'");
	
	private static final Pattern ACCESS_LIKE_CHARINTERVAL_PATTERN = Pattern
			.compile("\\[[a-zA-Z]\\-[a-zA-Z]\\]|\\[\\![a-zA-Z]\\-[a-zA-Z]\\]");
	
	private static final Pattern ACCESS_LIKE_ESCAPE_PATTERN = Pattern
	.compile("\\[[\\*|_|#]\\]");
	
	private static final Pattern FROM_PATTERN = Pattern
	.compile("\\w*(?i)from\\w*");
	
	
	private static final String WA_CURRENT_USER = "(\\W)(?i)currentUser\\s*\\(";
	private static List<String> waFunctions=new ArrayList<String>();
	private static final String DIGIT_STARTING_IDENTIFIERS = "(\\W)(([0-9])+([_a-zA-Z])+)(\\W)";
	private static final String UNDERSCORE_IDENTIFIERS = "(\\W)((_)+([_a-zA-Z])+)(\\W)";
	private static final String XESCAPED = "(\\W)((?i)_)(\\W)";
	private static final String TYPES_TRANSLATE = "(\\W)(?i)_(\\W)";
	private static final String DATE_ACCESS_FORMAT = "(0[1-9]|[1-9]|1[012])/(0[1-9]|[1-9]|[12][0-9]|3[01])/(\\d\\d\\d\\d)";
	private static final String DATE_FORMAT = "(\\d\\d\\d\\d)-(0[1-9]|[1-9]|1[012])-(0[1-9]|[1-9]|[12][0-9]|3[01])";
	private static final String HHMMSS_ACCESS_FORMAT = "(0[0-9]|1[0-9]|2[0-4]):([0-5][0-9]):([0-5][0-9])";
	private static final String DFUNCTIONS_WHERE="(?i)_\\s*\\(\\s*[\'\"](.*)[\'\"]\\,\\s*[\'\"](.*)[\'\"]\\,\\s*[\'\"](.*)[\'\"]\\s*\\)";
	private static final  String DFUNCTIONS_NO_WHERE="(?i)_\\s*\\(\\s*[\'\"](.*)[\'\"]\\,\\s*[\'\"](.*)[\'\"]\\s*\\)";
	private static final  List<String>  DFUNCTIONLIST=Arrays.asList("COUNT","MAX","MIN","SUM","AVG","LAST","FIRST");
	public  static final String BIG_BANG = "1899-12-30";
	private static  final  List<String>  NO_SQL_RESERVED_WORDS=Arrays.asList( "APPLICATION", "ASSISTANT",   "COLUMN", "COMPACTDATABASE",  "CONTAINER", 
			"CREATEDATABASE", "CREATEFIELD", "CREATEGROUP", "CREATEINDEX", "CREATEOBJECT", "CREATEPROPERTY", "CREATERELATION", "CREATETABLEDEF", "CREATEUSER",
			"CREATEWORKSPACE","DESCRIPTION", "DISALLOW",  "DOCUMENT",  "ECHO",   "ERROR", "EXIT",  "FIELD", "FIELDS", "FILLCACHE",  "FORM", "FORMS", 
			"GENERAL", "GETOBJECT", "GETOPTION", "GOTOPAGE",  "IDLE",  "IMP",  "INDEXES", "INSERTTEXT",  "LASTMODIFIED", "LEVEL", "LOGICAL", "LOGICAL1", 
			"MACRO", "MODULE", "MOVE", "NAME", "NEWPASSWORD",  "OFF", "OPENRECORDSET", "OPTION", "OWNERACCESS", "PARAMETER", "PARAMETERS", "PARTIAL", 
			"PROPERTY", "QUERIES",  "QUIT", "RECALC", "RECORDSET", "REFRESH", "REFRESHLINK", "REGISTERDATABASE", "REPAINT", "REPAIRDATABASE",
			"REPORT", "REPORTS", "REQUERY", "SCREEN", "SECTION", "SETFOCUS", "SETOPTION",  "TABLEDEF", "TABLEDEFS", "TABLEID",    "USER", "VALUE",
			"WORKSPACE",  "YEAR");
			
	private static final  List<String>  KEYWORDLIST=Arrays.asList("ALL","AND ","ANY ","AS","AT","AVG",
										"BETWEEN","BOTH","BY", "CALL","CASE","CAST","COALESCE","CORRESPONDING","CONVERT","COUNT","CREATE","CROSS",
										"DEFAULT","DISTINCT","DROP","ELSE","EVERY","EXISTS",
										"EXCEPT","FOR","FROM","FULL","GRANT"," GROUP","HAVING",
										"IN","INNER ","INTERSECT","INTO","IS","JOIN","LEFT","LEADING","LIKE",
										"MAX ","MIN","NATURAL","NOT","NULLIF","ON","ORDER","OR","OUTER","PRIMARY","REFERENCES","RIGHT","SELECT",
										"SET","SOME","STDDEV_POP","STDDEV_SAMP","SUM","TABLE","THEN","TO",
										"TRAILING","TRIGGER","UNION ","UNIQUE","USING","VALUES","VAR_POP","VAR_SAMP","WHEN","WHERE","WITH");
	private static  ArrayList<String> whiteSpacedTableNames=new ArrayList<String>();
	private static final  HashSet<String>  xescapedIdentifiers=new HashSet<String>();
	
	
	private static boolean supportsAccessLike=true;
	
	


	public static enum DDLType {
		CREATE_TABLE_AS_SELECT(	Pattern	.compile("\\s*(?i)create\\s*(?i)table\\s*(([_a-zA-Z0-9])*)\\s*(?)AS\\s*\\(\\s*(?)SELECT")), 
				CREATE_TABLE(Pattern.compile("\\s*(?i)create\\s*(?i)table\\s*(([_a-zA-Z0-9])*)")), 
				DROP_TABLE(	Pattern	.compile("\\s*(?i)drop\\s*(?i)table\\s*(([_a-zA-Z0-9])*)"));
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
	
	 static void addWAFunctionName(String name){
		   waFunctions.add(name);
	   }



	public static DDLType getDDLType(String s) {
		return DDLType.getDDLType(s);
	}

	private static String replaceWorkAroundFunctions(String sql) {
		
		for (String waFun:waFunctions){
			sql=sql.replaceAll("(\\W)(?i)"+waFun+"\\s*\\(", "$1"+waFun+"WA(");
		}
		sql=sql.replaceAll("(\\W)(?i)STDEV\\s*\\(", "$1STDDEV_SAMP(");
		sql=sql.replaceAll("(\\W)(?i)STDEVP\\s*\\(", "$1STDDEV_POP(");
		sql=sql.replaceAll("(\\W)(?i)VAR\\s*\\(", "$1VAR_SAMP(");
		sql=sql.replaceAll("(\\W)(?i)VARP\\s*\\(", "$1VAR_POP(");
		return sql.replaceAll(WA_CURRENT_USER,"$1user(");

	}
	
	public static String convertSQL(String sql, boolean creatingQuery) {
		return convertSQL( sql, null, creatingQuery);
	}

	public static String convertSQL(String sql,UcanaccessConnection conn, boolean creatingQuery) {
		sql=sql+" ";
		sql = convertAccessDate(sql);
		sql = replaceWorkAroundFunctions(sql);
		sql = convertDFunctions(sql);
		sql = escape(sql);
		sql = convertLike(sql);
		sql=replaceWhiteSpacedTables(sql);
		sql=replaceDistinctRow(sql);
		if(!creatingQuery){
			Pivot.checkAndRefreshPivot(sql,conn);
		}
		sql = sql.trim();
		return sql;
	}
	
	
	 private static String replaceDistinctRow(String sql) {
			return sql.replaceAll("\\s+(?i)distinctrow\\s+", " DISTINCT ");
	}

	
	
	  
	 static void addWhiteSpacedTableNames(String name) {
		  name=basicEscapingIdentifier(name);
		  if(whiteSpacedTableNames.contains(name))
			  return;
		  for(String alrIn:whiteSpacedTableNames){
			  if(name.contains(alrIn)){
				  whiteSpacedTableNames.add(whiteSpacedTableNames.indexOf(alrIn),name);
				  return;
			  }
	      }
		  whiteSpacedTableNames.add(name);
	}

	public static String convertSQL(String sql) {
		return convertSQL(sql,null, false);
	}
     public static String convertSQL(String sql, UcanaccessConnection conn) {
    	 return convertSQL(sql,conn, false);
	}

	public static String convertAccessDate(String sql) {
		sql = sql.replaceAll("#" + DATE_ACCESS_FORMAT + "#", "Timestamp'$3-$1-$2 00:00:00'")
				//FORMAT MM/dd/yyyy
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
				//FORMAT yyyy-MM-dd	
					    .replaceAll("#" + DATE_FORMAT + "#", "Timestamp'$1-$2-$3 00:00:00'")
				       .replaceAll(
						"#" + DATE_FORMAT + "\\s*("
								+ HHMMSS_ACCESS_FORMAT + ")#",
						"Timestamp'$1-$2-$3 $4'").replaceAll(
						"#" + DATE_FORMAT + "\\s*("
								+ HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#",
						"Timestamp'$1-$2-$3 $4'").replaceAll(
						"#" + DATE_FORMAT + "\\s*("
								+ HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#",
						"Timestamp'$1-$2-$3 $4'+ 12 Hour ")
							
						
						.replaceAll(
						"#(" + HHMMSS_ACCESS_FORMAT + ")#",
						"Timestamp'" + BIG_BANG + " $1'")
						.replaceAll(
						"#(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)AM#",
						"Timestamp'" + BIG_BANG + " $1'")
						.replaceAll(
						"#(" + HHMMSS_ACCESS_FORMAT + ")\\s*(?i)PM#",
						"Timestamp'" + BIG_BANG + " $1'+ 12 Hour");
		;
		return sql;
	}
	
	private static String replaceWhiteSpacedTables(String sql){
		String[] sqls=sql.split("'",-1);
		StringBuffer sb=new StringBuffer();
		String cm="";
		for(int i=0;i<sqls.length;++i){
			sb.append(cm).append(i%2==0?replaceWhiteSpacedTableNames0(sqls[i]):sqls[i]);
			cm="'";
		}
		return sb.toString();
	}
	
	
	private static String replaceWhiteSpacedTableNames0(String sql){
		if(whiteSpacedTableNames.size()==0){
			return sql;
		}
		StringBuffer sb=new StringBuffer(" (");
		String or="";
		for(String bst:whiteSpacedTableNames){
			sb.append(or).append("(?i)"+bst);
			or="|";
		}
		//workaround o.o. and  l.o.
		for(String bst:whiteSpacedTableNames){
			String dw=bst.replaceAll(" ", "  ");
			sql=sql.replaceAll(dw, bst);
		}
		sb.append(")");
		sql=sql.replaceAll(sb.toString()," \"$1\"");
		return sql;
	}
	public static String escapeKeyword(String identifier){
		
		for(String bst:KEYWORDLIST){
			identifier=identifier.replaceAll("(\\W)(?i)"+bst+"(\\W)"," $1\""+bst+"\"$2");
		}
		return identifier;
	}
	
	
	private static String convertIdentifiers(String sql) {
		int init;
		while ((init = sql.indexOf("[")) != -1) {
			
			int end = sql.indexOf("]");
			if(end<init)return sql;
			String content=basicEscapingIdentifier(sql.substring(init + 1, end)).toUpperCase();
			
			String subs=content.indexOf(" ")>0?"\"":" ";
			
			sql = sql.substring(0, init) + subs
					+content + subs
					+ sql.substring(end + 1);
		}
		return sql;
	}
	
	

	private static String convertIdentifiersSWDigit(String sql) {
		String sqlc=  convertIdentifiers(sql)
				.replaceAll(DIGIT_STARTING_IDENTIFIERS,
				"$1Z_" + "$2$5")
				.replaceAll(UNDERSCORE_IDENTIFIERS,
				"$1Z" + "$2$5");
		
		for (String xidt:xescapedIdentifiers){
				sqlc=sqlc.replaceAll(XESCAPED.replaceAll("_",xidt), "$1X" + "$2$3");
		}
		return sqlc;
	}
   
	
   private static String escape(String sql) {
		Pattern pd = DOUBLE_QUOTE_G_PATTERN;
		Pattern ps = QUOTE_G_PATTERN;
		int li = Math.max(sql.lastIndexOf("\""), sql.lastIndexOf("'"));
		boolean enddq = sql.endsWith("\"") || sql.endsWith("'");
		String suff = enddq ? "" : sql.substring(li + 1);
		suff = suff.replaceAll("&", "||");
		suff = convertIdentifiersSWDigit(suff);
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
			str = str.replaceAll("&", "||");
			str = convertIdentifiersSWDigit(str);
			tsql = str + "'" + group + "'" + escape(tsql.substring(mcr.end(0)));
		} else {
			tsql = convertIdentifiersSWDigit(tsql);
		}
		return tsql + suff;
	}

	public static String basicEscapingIdentifier(String name) {
		if (name.startsWith("~"))
			return null;
		String nl=name.toUpperCase();
		
		if(Database.isReservedWord(nl)&&NO_SQL_RESERVED_WORDS.contains(nl)){
			xescapedIdentifiers.add(nl);
		}
		if(KEYWORDLIST.contains(nl)){
			name= "\""+name+"\"";
		}
		String escaped = Database
				.escapeIdentifier(name//.replaceAll(" ", "_")
				.replaceAll("[/\\\\$%^:-]", "_").replaceAll("~", "M_").replaceAll("\\.",
						"_")).replaceAll("\'","").replaceAll("\\+", "");
		
		if (Character.isDigit(escaped.trim().charAt(0))) {
			escaped = "Z_" + escaped.trim();
		}
		if (escaped.charAt(0)=='_') {
			escaped = "Z" + escaped;
		}
		
		return escaped.toUpperCase();
	}
	
	
	
	public static String escapeIdentifier(String name) {
		String escaped=basicEscapingIdentifier(name);
		if(escaped==null)return null;
		if(escaped.indexOf(" ")>0){
			escaped="\""+escaped+"\"";
		}
		return escaped;
	}


	private static String convertCreateTable(String sql,
			Map<String, String> types2Convert) {
		// padding for detecting the right exception
		sql += " ";
		for (Map.Entry<String, String> entry : types2Convert.entrySet()) {
			sql = sql.replaceAll(TYPES_TRANSLATE
					.replaceAll("_", entry.getKey()), "$1" + entry.getValue()
					+ "$2");
			
			sql = sql.replaceAll("(\\W)(?i)VARCHAR\\s*w*(\\)|,)","$1VARCHAR(255)$2");
		}
		return sql;
	}
	
	private  static String convertDFunctions(String sql) {
		boolean hasFrom=FROM_PATTERN.matcher(sql).find();
		String init=hasFrom?" (SELECT ":"";
		String end=hasFrom?" ) ":"";
		
		for(String s:DFUNCTIONLIST){
			sql=sql.replaceAll(
					DFUNCTIONS_WHERE.replaceFirst("_", "D"+s), 
					init+s+"($1) FROM $2 WHERE $3     "+end);
			sql=sql.replaceAll(
					DFUNCTIONS_NO_WHERE.replaceFirst("_", "D"+s), 
					init+s+"($1) FROM $2    "+end);
		
		}
		return sql;
	}


	public static String convertCreateTable(String sql) {
		return convertCreateTable(sql, TypesMap.getAccess2HsqlTypesMap());
	}

	public static boolean checkDDL(String sql) {
		if (sql == null)
			return false;
		else {
			String lcsql = sql.toLowerCase().trim();
			return lcsql.startsWith("create ") || lcsql.startsWith("alter ")
					|| lcsql.startsWith("drop ");
		}
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
	
	
	private static String convert2RegexMatches(String likeContent){
		Pattern ptn=ACCESS_LIKE_ESCAPE_PATTERN;
		Matcher mtc=ptn.matcher(likeContent);
		if(mtc.find()){
			return
			convert2RegexMatches(likeContent.substring(0, mtc.start(0)))+
			mtc.group(0).substring(1, 2)+
			convert2RegexMatches(likeContent.substring( mtc.end(0)));
		}
		return likeContent.replaceAll("#", "\\\\d").replaceAll("\\*",
		".*").replaceAll("_", ".").replaceAll(
		"(\\[)\\!(\\w\\-\\w\\])", "$1^$2") + "')";
	}
	
	private static String convert2LikeCondition(String likeContent){
		Pattern ptn=ACCESS_LIKE_ESCAPE_PATTERN;
		Matcher mtc=ptn.matcher(likeContent);
		if(mtc.find()){
			return
			convert2LikeCondition(likeContent.substring(0, mtc.start(0)))+
			mtc.group(0).substring(1, 2)+
			convert2LikeCondition(likeContent.substring( mtc.end(0)));
		}
		
		return likeContent.replaceAll("\\*", "%").replaceAll("\\?", "_");
	}

	private static String convertLike(String conditionField, String closePar,
			String likeContent) {
		Pattern inter = ACCESS_LIKE_CHARINTERVAL_PATTERN;
		
		
		
		if (likeContent.indexOf("#") >= 0 || inter.matcher(likeContent).find()) {
				return "REGEXP_MATCHES("
					+ conditionField
					+ ",'"
					+ convert2RegexMatches( likeContent)
					+ closePar + " ";
		}
		return " " + conditionField + " like '"
				+ convert2LikeCondition(likeContent)

				+ "'";
	}
		
	public static boolean isSupportsAccessLike() {
		return supportsAccessLike;
	}

	public static void setSupportsAccessLike(boolean supportsAccessLike) {
		SQLConverter.supportsAccessLike = supportsAccessLike;
	}



	public static boolean contains(String identifier) {
		return xescapedIdentifiers.contains(identifier);
	}



	

}
