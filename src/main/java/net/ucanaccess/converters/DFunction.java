/*
Copyright (c) 2013 Marco Amadei.

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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ucanaccess.jdbc.UcanaccessConnection;

public class DFunction {
	private Connection conn;
	private String sql;
	private static final Pattern FROM_PATTERN = Pattern
			.compile("\\w*(?i)FROM\\w*");
	private static final String SELECT_FROM = "(?i)SELECT(.*\\W)(?i)FROM(.*)";
	private static final String DFUNCTIONS_WHERE = "(?i)_\\s*\\(\\s*[\'\"](.*)[\'\"]\\,\\s*[\'\"](.*)[\'\"]\\,\\s*[\'\"](.*)[\'\"]\\s*\\)";
	private static final String DFUNCTIONS_WHERE_DYNAMIC = "(?i)_\\s*\\(\\s*[\'\"](.*)[\'\"]\\,\\s*[\'\"](.*)[\'\"]\\,(.*)\\)";
	private static final String DFUNCTIONS_NO_WHERE = "(?i)_\\s*\\(\\s*[\'\"](.*)[\'\"]\\,\\s*[\'\"](.*)[\'\"]\\s*\\)";
	private static final String IDENTIFIER = "(\\W)((?i)_)(\\W)";
	private static final List<String> DFUNCTIONLIST = Arrays.asList("COUNT",
			"MAX", "MIN", "SUM", "AVG", "LAST", "FIRST","LOOKUP");

	public DFunction(Connection conn, String sql) {
		this.conn = conn;
		this.sql = sql.replaceAll("\n", " ").replaceAll("\r", " ");
	}

	private String convertDFunctions() {
		String sql0 = sql;
		try {
			boolean hasFrom = FROM_PATTERN.matcher(sql).find();
			String init = hasFrom ? " (SELECT " : "";
			String end = hasFrom ? " ) " : "";
			for (String s : DFUNCTIONLIST) {
				
				String fun="D" + s;
				s=s.equalsIgnoreCase("lookup")?" ":s;
				sql0 = sql0.replaceAll(
						DFUNCTIONS_WHERE.replaceFirst("_", fun), init + s
								+ "($1) FROM $2 WHERE $3     " + end);
				sql0 = sql0.replaceAll(
						DFUNCTIONS_NO_WHERE.replaceFirst("_", fun), init
								+ s + "($1) FROM $2    " + end);
				Pattern dfd = Pattern.compile(DFUNCTIONS_WHERE_DYNAMIC
						.replaceFirst("_", fun));
				for (Matcher mtc = dfd.matcher(sql0); mtc.find(); mtc = dfd
						.matcher(sql0)) {
					StringBuffer sb = new StringBuffer();
					String g3 = mtc.group(3);
					String tableN = mtc.group(2).trim();
					String alias = tableN.startsWith("[")
							& tableN.endsWith("]") ? "[" + unpad(tableN)
							+ "_DALIAS]" : tableN + "_DALIAS";
					String tn = tableN.startsWith("[") & tableN.endsWith("]") ? unpad(tableN)
							: tableN;
					sb.append(init).append(s).append("(").append(mtc.group(1))
							.append(") FROM ").append(tableN).append(" AS ")
							.append(alias).append(" WHERE ");
					if (g3.indexOf("&") > 0) {
						String[] pts = g3.split("&", -1);
						for (String tkn : pts) {
							if (isQuoted(tkn)) {
								tkn = tkn.trim();
								sb.append(unpad(tkn));
							} else {
								tkn+=" ";
								for (String cln : getColumnNames(tn
										.toUpperCase())) {
									String oppn = IDENTIFIER.replaceFirst("_",
											cln);
									Pattern op = Pattern.compile(oppn);
									Matcher mtcop=op.matcher(tkn);
									if (!mtcop.find())
										continue;
									String pref=mtcop.group(1);
									if(pref.equals(".")||
											(pref.equals("[")&&mtcop.start(1)>0&&tkn.charAt(mtcop.start(1)-1)=='.'))continue;
									tkn = tkn.replaceAll(oppn,pref.equals("[")?resolveAmbiguosTableName(cln)+".$1$2$3": "$1"
											+ resolveAmbiguosTableName(cln)
											+ ".$2$3");
								}
								sb.append(tkn);
							}
						}
					}
					sb.append(end);
					sql0 = sql0
							.replaceFirst(DFUNCTIONS_WHERE_DYNAMIC
									.replaceFirst("_", fun), sb.toString());
				}
			}
		} catch (SQLException e) {
		}
		return sql0;
	}

	private String resolveAmbiguosTableName(String identifier) {
		Statement st = null;
		try {
			String f4t = SQLConverter.convertSQL(this.sql.replaceFirst(
					SELECT_FROM, "SELECT " + identifier + " FROM $2 "));
			st = conn.createStatement();
			ResultSetMetaData rsmd = st.executeQuery(f4t).getMetaData();
			String tableN = rsmd.getTableName(1);
			if (tableN == null || tableN.trim().length() == 0)
				return identifier;
			return tableN;
		} catch (SQLException e) {
			return identifier;
		} finally {
			if (st != null)
				try {
					st.close();
				} catch (SQLException e) {
				}
		}
	}

	private List<String> getColumnNames(String tableName) throws SQLException {
		ArrayList<String> ar = new ArrayList<String>();
		if (conn == null) {
			UcanaccessConnection conu = UcanaccessConnection.getCtxConnection();
			if (conu == null)
				return ar;
			conn = conu.getHSQLDBConnection();
		}
		DatabaseMetaData dbmd = conn.getMetaData();
		ResultSet rs = dbmd.getColumns(null, null, tableName, null);
		while (rs.next()) {
			ar.add(rs.getString("COLUMN_NAME"));
		}
		return ar;
	}

	private static boolean isQuoted(String g3) {
		g3 = g3.trim();
		return g3.startsWith("'") && g3.endsWith("'")
				&& (g3.substring(1, g3.length() - 1).indexOf('\'') < 0)
				|| g3.startsWith("\"") && g3.endsWith("\"")
				&& (g3.substring(1, g3.length() - 1).indexOf('"') < 0);
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
