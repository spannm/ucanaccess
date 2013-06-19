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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Logger;

public class Pivot {
	private String transform;
	private String select;
	private String from;
	private String expression;
	private String pivot;
	private List<String> pivotIn;
	private final Pattern PIVOT = Pattern
			.compile("(?i)TRANSFORM(.*\\W)(?i)SELECT(.*\\W)(?i)FROM(.*\\W)(?i)PIVOT(.*)");
	private final Pattern PIVOT_EXPR = Pattern
			.compile("(.*)(?i)IN\\s*\\((.*)\\)");
	private final Pattern PIVOT_AGGR = Pattern
			.compile("((?i)SUM|(?i)MAX|(?)MIN|(?)FIRST|(?)LAST|(?i)AVG|(?)COUNT|(?)STDEV|(?)VAR)\\s*\\((.*)\\)");
	private final Pattern PIVOT_CN = Pattern.compile("[\"'#](.*)[\"'#]");
	private final String PIVOT_GROUP_BY = "(?i)GROUP\\s*(?i)BY";
	private String aggregateFun;
	private Connection conn;
	private boolean pivotInCondition = true;
	private String originalQuery;
	private final static HashMap<String, String> pivotMap = new HashMap<String, String>();

	public Pivot(Connection conn) {
		this.conn = conn;
	}

	public void registerPivot(String name) {
		if (!this.pivotInCondition)
			pivotMap.put(name, this.originalQuery);
	}

	public static void checkAndRefreshPivot(String currSql,UcanaccessConnection conu) {
		for (String name : pivotMap.keySet()) {
			Pattern ptrn = Pattern.compile("(\\W)(?i)" + name + "(\\W)");
			Matcher mtc = ptrn.matcher(currSql);
			if (mtc.find()) {
				Statement st = null;
				try {
					if(conu==null)
					conu = UcanaccessConnection
							.getCtxConnection();
					if (conu == null)
						return;
					Connection conh = conu.getHSQLDBConnection();
					Pivot pivot = new Pivot(conh);
					
					if(!pivot.parsePivot(pivotMap.get(name))) return;
					String sqlh = pivot.toSQL();
					if(sqlh==null) return;
					st = conh.createStatement();
					String escqn=name.indexOf(' ')>0?"["+name+"]":name;
					st.executeUpdate(SQLConverter.convertSQL("DROP VIEW " + escqn,true));
					StringBuffer sb = new StringBuffer("CREATE VIEW ")
							.append(escqn).append(" AS ").append(sqlh);
					String v = SQLConverter.convertSQL(sb.toString(),true);
					st.executeUpdate(v);
				} catch (Exception e) {
					Logger. logWarning(e.getMessage());
				} finally {
					if (st != null) {
						try {
							st.close();
						} catch (SQLException e) {
						}
					}
				}
			}
		}
	}

	public boolean parsePivot(String originalQuery) {
		this.originalQuery=originalQuery;
		originalQuery = originalQuery.replaceAll("\n", " ").replaceAll("\r", " ").trim();
		if (originalQuery.endsWith(";"))
			originalQuery = originalQuery.substring(0, originalQuery.length() - 1);
		Matcher mtc = PIVOT.matcher(originalQuery);
		if (mtc.groupCount() < 4)
			return false;
		if (mtc.matches()) {
			this.transform = mtc.group(1);
			Matcher aggr = PIVOT_AGGR.matcher(this.transform);
			if (aggr.find()) {
				if (aggr.groupCount() < 2)
					return false;
				this.aggregateFun = aggr.group(1);
				this.expression = aggr.group(2);
			} else
				return false;
			this.select = mtc.group(2);
			this.from = mtc.group(3);
			String pe = mtc.group(4);
			Matcher mtcExpr = PIVOT_EXPR.matcher(pe);
			if (mtcExpr.find()) {
				if (mtcExpr.groupCount() < 2)
					return false;
				this.pivot = mtcExpr.group(1);
				this.pivotIn = Arrays.asList(mtcExpr.group(2).split(","));
			} else {
				this.pivot = pe;
			}
			return true;
		} else
			return false;
	}

	private void appendCaseWhen(StringBuffer sb, String condition, String cn) {
		sb.append(this.aggregateFun).append("(CASE WHEN ").append(condition)
				.append(" THEN ").append(this.expression).append(" END) AS ")
				.append(cn);
	}

	public String verifySQL() {
		StringBuffer sb = new StringBuffer();
		String[] fromS = this.from.split(PIVOT_GROUP_BY);
		sb.append("SELECT DISTINCT ").append(this.pivot).append(" AS PIVOT ");
		sb.append(" FROM ").append(fromS[0]).append(" GROUP BY ")
				.append(this.pivot).append(",").append(fromS[1]);
		return SQLConverter.convertSQL(sb.toString());
	}

	public boolean prepare() {
		try {
			this.pivotIn = new ArrayList<String>();
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(verifySQL());
			int i=0;
			while (rs.next()) {
				String frm=format(rs.getObject("PIVOT"));
				if(frm!=null)
				this.pivotIn.add(frm);
				i++;
				if(i>1000){
					return false;
				}
			}
			return true;
		} catch (Exception e) {
			
			return false;
		}
	}

	private String format(Object cln) {
		if (cln==null) return null;
		if (cln instanceof Date) {
			SimpleDateFormat sdf = new SimpleDateFormat("#MM/dd/yyyy HH:mm:ss#");
			String clns = sdf.format((Date) cln);
			if (clns.endsWith(" 00:00:00#")) {
				clns = clns.replaceAll(" 00:00:00", "");
			}
			return clns;
		}
		if (cln instanceof String) {
			return "'" + cln.toString().replaceAll("\'","''") + "'";
		}
		return cln.toString();
	}

	private String replaceComma(String cn) {
		cn=cn.replaceAll("\n", " ").replaceAll("\r", " ");
		Matcher dcm = PIVOT_CN.matcher(cn);
		
		if (dcm.matches()) {
			cn = dcm.group(1);
		}
		
		cn=cn.replaceAll("\'","").replaceAll("\"", "");
		
		return "[" + cn + "]";
	}

	public String toSQL() {
		if (this.pivotIn == null) {
			if (!prepare())
				return null;
			this.pivotInCondition = false;
		} 
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT ");
		sb.append(this.select);
		for (String s : this.pivotIn) {
			sb.append(",");
			appendCaseWhen(sb, this.pivot + "=" + s, replaceComma(s));
		}
		sb.append(" FROM ").append(this.from);
		
		return sb.toString();
	}
}
