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

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.ext.FunctionType;
import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.util.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.IndexData.ColumnDescriptor;
import com.healthmarketscience.jackcess.query.Query;

public class LoadJet {
	private static int y = 0;

	private final class FunctionsLoader {
		private HashSet<String> functionsDefinition = new HashSet<String>();

		private void addAggregates() {
			functionsDefinition.add(getAggregate("LONGVARCHAR", "last"));
			functionsDefinition.add(getAggregate("DECIMAL(100,10)", "last"));
			functionsDefinition.add(getAggregate("LONGVARCHAR", "first"));
			functionsDefinition.add(getAggregate("DECIMAL(100,10)", "first"));
		}

		private void addFunction(String functionName, String methodName,
				String returnType, String... parTypes) {
			StringBuffer funDef = new StringBuffer();
			if (DBReference.is2xx()) {
				funDef.append("CREATE FUNCTION ").append(functionName)
						.append("(");
				String comma = "";
				for (int i = 0; i < parTypes.length; i++) {
					funDef.append(comma).append("par").append(i).append(" ")
							.append(parTypes[i]);
					comma = ",";
				}
				funDef.append(")");
				funDef.append(" RETURNS ");
				funDef.append(returnType);
				funDef.append("  LANGUAGE JAVA DETERMINISTIC NO SQL  EXTERNAL NAME 'CLASSPATH:");
				funDef.append(methodName).append("'");
			} else {
				funDef.append("CREATE ALIAS ").append(functionName)
						.append(" FOR \"").append(methodName).append("\"");
			}
			functionsDefinition.add(funDef.toString());
		}

		private void addFunctions(Class<?> clazz) throws SQLException {
			Method[] mths = clazz.getDeclaredMethods();
			HashMap<String, String> tmap = TypesMap.getAccess2HsqlTypesMap();
			for (Method mth : mths) {
				Annotation[] ants = mth.getAnnotations();
				for (Annotation ant : ants) {
					if (ant.annotationType().equals(FunctionType.class)) {
						FunctionType ft = (FunctionType) ant;
						String methodName = clazz.getName() + "."
								+ mth.getName();
						String functionName = ft.functionName();
						if (functionName == null)
							functionName = methodName;
						AccessType[] acts = ft.argumentTypes();
						AccessType ret = ft.returnType();
						String retTypeName = ret.name();
						String returnType = tmap.containsKey(retTypeName) ? tmap
								.get(retTypeName) : retTypeName;
						if (AccessType.TEXT.equals(ret)) {
							returnType += "(255)";
						}
						String[] args = new String[acts.length];
						for (int i = 0; i < args.length; i++) {
							String typeName = acts[i].name();
							args[i] = tmap.containsKey(typeName) ? tmap
									.get(typeName) : typeName;
							if (AccessType.TEXT.equals(acts[i])) {
								args[i] += "(255)";
							}
						}
						if (ft.namingConflict()) {
							SQLConverter.addWAFunctionName(functionName);
							functionName += "WA";
						}
						addFunction(functionName, methodName, returnType, args);
					}
				}
			}
			createFunctions();
		}

		private void createFunctions() throws SQLException {
			for (String functionDef : functionsDefinition) {
				execCreate(functionDef);
			}
			functionsDefinition.clear();
		}

		private String getAggregate(String type, String fun) {
			String createLast = "CREATE AGGREGATE FUNCTION "
					+ fun
					+ "(IN val "
					+ type
					+ ", IN flag BOOLEAN, INOUT register  "
					+ type
					+ ", INOUT counter INT) "
					+ "  RETURNS  "
					+ type
					+ "  NO SQL  LANGUAGE JAVA "
					+ "  EXTERNAL NAME 'CLASSPATH:net.ucanaccess.converters.FunctionsAggregate."
					+ fun + "'";
			return createLast;
		}

		private void loadMappedFunctions() throws SQLException {
			addFunctions(Functions.class);
			addAggregates();
			createFunctions();
		}
	}

	private final class LogsFlusher {
		private void dumpList(List<String> logs) {
			dumpList(logs, false);
		}

		private void dumpList(List<String> logs, boolean cr) {
			String comma = "";
			StringBuffer sb = new StringBuffer();
			String crs = cr ? System.getProperty("line.separator") : "";
			for (String log : logs) {
				sb.append(comma).append(log).append(crs);
				comma = ", ";
			}
			Logger.log(sb.toString());
			logs.clear();
		}
	}

	private final class TablesLoader {
		private ArrayList<String> unresolvedTables = new ArrayList<String>();

		private String commaSeparated(List<ColumnDescriptor> columns) {
			String comma = "";
			StringBuffer sb = new StringBuffer(" (");
			for (ColumnDescriptor cd : columns) {
				sb.append(comma)
						.append(SQLConverter.escapeIdentifier(cd.getColumn()
								.getName()));
				comma = ",";
			}
			return sb.append(") ").toString();
		}

		private void createSyncrTable(Table t) throws SQLException, IOException {
			String tn = t.getName();
			String ntn = SQLConverter.escapeIdentifier(tn);
			StringBuffer sbC = new StringBuffer("CREATE CACHED TABLE ").append(
					ntn).append("(");
			List<Column> lc = t.getColumns();
			String comma = "";
			ArrayList<String> arTrigger = new ArrayList<String>();
			for (Column cl : lc) {
				String htype = cl.getType().equals(DataType.TEXT) ? "VARCHAR("
						+ cl.getLengthInUnits() + ")" : TypesMap.map2hsqldb(cl
						.getType());
				sbC.append(comma)
						.append(SQLConverter.escapeIdentifier(cl.getName()))
						.append(" ").append(htype);
				PropertyMap pm = cl.getProperties();
				Object required = pm.getValue(PropertyMap.REQUIRED_PROP);
				if (required != null && ((Boolean) required)) {
					sbC.append(" NOT NULL ");
				}
				comma = ",";
			}
			sbC.append(")");
			execCreate(sbC.toString());
			for (String trigger : arTrigger) {
				execCreate(trigger);
			}
		}

		private void defaultValues(Table t) throws SQLException, IOException {
			String tn = t.getName();
			String ntn = SQLConverter.escapeIdentifier(tn);
			List<Column> lc = t.getColumns();
			ArrayList<String> arTrigger = new ArrayList<String>();
			for (Column cl : lc) {
				PropertyMap pm = cl.getProperties();
				String ncn=SQLConverter
						.escapeIdentifier(cl
								.getName());
				Object defaulT = pm.getValue(PropertyMap.DEFAULT_VALUE_PROP);
				if (defaulT != null) {
					String cdefaulT = SQLConverter.convertSQL(" "
							+ defaulT.toString());
					if(cl.getType().equals(DataType.BOOLEAN)&&("=yes".equalsIgnoreCase(cdefaulT)||"yes".equalsIgnoreCase(cdefaulT)))
						cdefaulT="true";
					if(cl.getType().equals(DataType.BOOLEAN)&&("=no".equalsIgnoreCase(cdefaulT)||"no".equalsIgnoreCase(cdefaulT)))
						cdefaulT="false";
					String guidExp = "GenGUID()";
					if (!guidExp.equals(defaulT)) {
						if (!tryDefault(cdefaulT)) {
							Logger.logWarning("Unknown expression:" + defaulT+ " default value of  column "+cl.getName()+" table "+cl.getTable().getName());
						} else {
							if (cdefaulT.endsWith(")")) {
								arTrigger
										.add("CREATE TRIGGER DEFAULT_TRIGGER"
												+ (y++)
												+ " BEFORE INSERT ON "
												+ ntn
												+ "  REFERENCING NEW ROW AS NEW FOR EACH ROW IF NEW."
												+ ncn
												+ " IS NULL THEN "
												+ "SET NEW."
												+ ncn
												+ "= " + cdefaulT + " ; END IF");
							} else
								arTrigger
								.add("alter table "+ntn+" alter column "+ncn+" set default "+cdefaulT);
						}
					}
				}
			}
			for (String trigger : arTrigger) {
				execCreate(trigger);
			}
		}

		private void loadForeignKey(Index idx) throws IOException, SQLException {
			String ntn = SQLConverter
					.escapeIdentifier(idx.getTable().getName());
			String nin = SQLConverter.escapeIdentifier(idx.getName());
			nin = (ntn + "_" + nin).replaceAll("\"", "").replaceAll("\\W", "_");
			String colsIdx = commaSeparated(idx.getColumns());
			String colsIdxRef = commaSeparated(idx.getReferencedIndex()
					.getColumns());
			StringBuffer ci = new StringBuffer("ALTER TABLE ").append(ntn);
			ci.append(" ADD CONSTRAINT ").append(nin);
			String nrt = SQLConverter.escapeIdentifier(idx.getReferencedIndex()
					.getTable().getName());
			ci.append(" FOREIGN KEY ").append(colsIdx).append(" REFERENCES ")
					.append(nrt).append(colsIdxRef);
			if (idx.getReference().isCascadeDeletes()) {
				ci.append(" ON DELETE CASCADE ");
			}
			if (idx.getReference().isCascadeUpdates()) {
				ci.append(" ON UPDATE CASCADE ");
			}
			execCreate(ci.toString());
			loadedIndexes.add("FK on " + ntn + " Columns:" + colsIdx
					+ " References " + nrt + " Columns:" + colsIdxRef);
		}

		private void loadIndex(Index idx) throws IOException, SQLException {
			String ntn = SQLConverter
					.escapeIdentifier(idx.getTable().getName());
			String nin = SQLConverter.escapeIdentifier(idx.getName());
			nin = (ntn + "_" + nin).replaceAll("\"", "").replaceAll("\\W", "_");
			boolean uk = idx.isUnique();
			boolean pk = idx.isPrimaryKey();
			StringBuffer ci = new StringBuffer("ALTER TABLE ").append(ntn);
			String colsIdx = commaSeparated(idx.getColumns());
			if (pk) {
				ci.append(" ADD PRIMARY KEY ").append(colsIdx);
			} else if (uk) {
				ci.append(" ADD CONSTRAINT ").append(nin);
				ci.append(" UNIQUE ").append(colsIdx);
			} else {
				ci = new StringBuffer("CREATE INDEX ").append(nin)
						.append(" ON ").append(ntn).append(colsIdx);
			}
			try {
				execCreate(ci.toString());
			} catch (Exception e) {
				Logger.logWarning(e.getMessage());
				return;
			}
			String pre = pk ? "Primary Key " : uk ? "Index Unique " : "Index";
			loadedIndexes.add(pre + " on " + ntn + " Columns:" + colsIdx);
		}

		private void loadIndexes() throws SQLException, IOException {
			for (String tn : dbIO.getTableNames()) {
				if (!this.unresolvedTables.contains(tn))
					this.loadTableIndexes(tn);
			}
			for (String tn : dbIO.getTableNames()) {
				if (!this.unresolvedTables.contains(tn))
					this.loadTableFKs(tn);
			}
		}

		private void loadTable(Table t) throws SQLException, IOException {
			String tn = t.getName();
			if (tn.indexOf(" ") > 0) {
				SQLConverter.addWhiteSpaceTables(tn);
			}
			String ntn = SQLConverter.escapeIdentifier(tn);
			if (ntn == null)
				return;
			createSyncrTable(t);
			loadTableData(t);
			defaultValues(t);
			triggersGenerator.synchronisationTriggers(ntn,
					hasAutoNumberColumn(t));
			loadedTables.add(ntn);
		}

		private void loadTableData(Table t) throws IOException, SQLException {
			PreparedStatement ps = null;
			try {
				for (int i = 0; i < t.getRowCount(); i++) {
					ArrayList<Object> values = new ArrayList<Object>();
					Map<String, Object> row = t.getNextRow();
					if (row == null)
						continue;
					if (ps == null)
						ps = sqlInsert(t, row);
					Set<Entry<String, Object>> se = row.entrySet();
					for (Entry<String, Object> en : se) {
						values.add(value(en.getValue()));
					}
					execInsert(ps, values);
				}
			} finally {
				if (ps != null)
					ps.close();
			}
		}

		private void loadTableFKs(String tableName) throws IOException,
				SQLException {
			Table table = dbIO.getTable(tableName);
			for (Index idx : table.getIndexes()) {
				if (idx.isForeignKey() && !idx.getReference().isPrimaryTable())
					loadForeignKey(idx);
			}
		}

		private void loadTableIndexes(String tableName) throws IOException,
				SQLException {
			Table table = dbIO.getTable(tableName);
			for (Index idx : table.getIndexes()) {
				if (!idx.isForeignKey())
					loadIndex(idx);
			}
		}

		private void loadTables() throws SQLException, IOException {
			for (String tn : dbIO.getTableNames()) {
				Table t = null;
				try {
					t = dbIO.getTable(tn);
				} catch (Exception e) {
					Logger.logWarning(e.getMessage());
					this.unresolvedTables.add(tn);
				}
				if (t != null)
					loadTable(t);
			}
		}

		private PreparedStatement sqlInsert(Table t, Map<String, Object> row)
				throws IOException, SQLException {
			String tn = t.getName();
			String ntn = SQLConverter.escapeIdentifier(tn);
			String comma = "";
			StringBuffer sbI = new StringBuffer(" INSERT INTO ").append(ntn)
					.append(" (");
			StringBuffer sbE = new StringBuffer(" VALUES( ");
			Set<Entry<String, Object>> se = row.entrySet();
			comma = "";
			for (Entry<String, Object> en : se) {
				sbI.append(comma).append(
						SQLConverter.escapeIdentifier(en.getKey()));
				sbE.append(comma).append(" ? ");
				comma = ",";
			}
			sbI.append(") ");
			sbE.append(")");
			sbI.append(sbE);
			return conn.prepareStatement(sbI.toString());
		}

		private Object value(Object value) {
			if (value == null)
				return null;
			if (value instanceof Float) {
				BigDecimal bd = new BigDecimal(value.toString());
				return bd;
			}
			 if(value instanceof Date &&!(value instanceof Timestamp)){
	                Timestamp ts= new Timestamp(((Date)value).getTime());
	                return ts;
	         }
			return value;
		}
	}

	private final class TriggersLoader {
		private final static String DEFAULT_TRIGGERS_PACKAGE = "net.ucanaccess.triggers";

		private void loadTrigger(String tableName, String namePrefix,
				String when, String className) throws SQLException {
			String q0 = DBReference.is2xx() ? "" : " QUEUE 0  ";
			String triggerName = namePrefix + "_"
					+ (tableName.replaceAll("\"", "").replaceAll(" ", "_"));
			execCreate("CREATE TRIGGER " + triggerName + "  " + when + " ON "
					+ tableName + "   FOR EACH ROW	" + q0 + "   CALL \""
					+ className + "\" ");
		}

		private void loadTriggerNP(String tableName, String namePrefix,
				String when, String className) throws SQLException {
			loadTrigger(tableName, namePrefix, when, DEFAULT_TRIGGERS_PACKAGE
					+ "." + className);
		}

		private void synchronisationTriggers(String tableName,
				boolean hasAutoNumberColumn) throws SQLException {
			loadTriggerNP(tableName, "genericInsert", "AFTER INSERT",
					"TriggerInsert");
			loadTriggerNP(tableName, "genericUpdate", "AFTER UPDATE",
					"TriggerUpdate");
			loadTriggerNP(tableName, "genericDelete", "AFTER DELETE",
					"TriggerDelete");
			if (hasAutoNumberColumn) {
				loadTriggerNP(tableName, "autonumber", "BEFORE INSERT",
						"TriggerAutoNumber");
				loadTriggerNP(tableName, "autonumber_validate",
						"BEFORE UPDATE", "TriggerAutoNumber");
			}
		}
	}

	private final class ViewsLoader {
		private HashSet<String> notLoaded = new HashSet<String>();

		private boolean loadView(Query q) throws SQLException {
			String qnn = SQLConverter.basicEscapingIdentifier(q.getName());
			if (qnn.indexOf(" ") > 0) {
				SQLConverter.addWhiteSpaceTables(q.getName());
			}
			String escqn=qnn.indexOf(' ')>0?"["+qnn+"]":qnn;
			String querySQL=q.toSQLString();
			Pivot pivot=null;
			if(q.getType().equals(Query.Type.CROSS_TAB)){		
				pivot=new Pivot(conn);
				if(!pivot.parsePivot(querySQL)||(querySQL=pivot.toSQL())==null){
					this.notLoaded.add(q.getName());
					return false;
				}
							
			}
			StringBuffer sb = new StringBuffer("CREATE VIEW ").append(escqn)
					.append(" AS ").append(querySQL);
			String v = null;
			try {
				v = SQLConverter.convertSQL(sb.toString(), true);
				execCreate(v);
     			loadedQueries.add(qnn);
				if(pivot!=null){
					pivot.registerPivot(qnn);
				}
				return true;
			} catch (Exception e) {
				this.notLoaded.add(q.getName());
				if (!err) {
					Logger.log("Error occured while loading:" + q.getName());
					Logger.log("Converted view was :" + v);
					err = true;
				}
				return false;
			}
		}

		private void loadViews() throws SQLException, IOException {
			List<Query> lq = null;
			try {
				lq = dbIO.getQueries();
				Iterator<Query> it=lq.iterator();
				while(it.hasNext()) {
					Query q=it.next();
					if (!q.getType().equals(Query.Type.SELECT)
							&& !q.getType().equals(Query.Type.UNION)
								&& !q.getType().equals(Query.Type.CROSS_TAB)
							) {
						it.remove();
					}
				}
				queryPorting(lq);
			} catch (Exception e) {
				this.notLoaded.add("");
			}
		}

		private void queryPorting(List<Query> lq) throws SQLException {
			ArrayList<String> arn = new ArrayList<String>();
			for (Query q : lq) {
				arn.add(q.getName().toLowerCase());
			}
			boolean heavy = false;
			while (lq.size() > 0) {
				ArrayList<Query> arq = new ArrayList<Query>();
				for (Query q : lq) {
					String qtxt = q.toSQLString().toLowerCase();
					boolean foundDep = false;
					if (!heavy)
						for (String name : arn) {
							if (qtxt.indexOf(name) != -1) {
								foundDep = true;
								break;
							}
						}
					if (!foundDep && loadView(q)) {
						arq.add(q);
						arn.remove(q.getName());
					}
				}
				if (arq.size() == 0) {
					if (heavy) {
						break;
					} else {
						heavy = true;
					}
				}
				lq.removeAll(arq);
			}
		}
	}

	private Connection conn;
	private Database dbIO;
	private boolean err;
	private FunctionsLoader functionsLoader = new FunctionsLoader();
	private ArrayList<String> loadedIndexes = new ArrayList<String>();
	private ArrayList<String> loadedQueries = new ArrayList<String>();
	private ArrayList<String> loadedTables = new ArrayList<String>();
	private LogsFlusher logsFlusher = new LogsFlusher();
	private TablesLoader tablesLoader = new TablesLoader();
	private TriggersLoader triggersGenerator = new TriggersLoader();
	private ViewsLoader viewsLoader = new ViewsLoader();

	public LoadJet(Connection conn, Database dbIO) {
		super();
		this.conn = conn;
		this.dbIO = dbIO;
	}

	private static boolean hasAutoNumberColumn(Table t) {
		List<Column> lc = t.getColumns();
		for (Column cl : lc) {
			if (cl.isAutoNumber()) {
				return true;
			}
		}
		return false;
	}

	public void addFunctions(Class<?> clazz) throws SQLException {
		this.functionsLoader.addFunctions(clazz);
	}

	public void createSyncrTable(Table t) throws SQLException, IOException {
		tablesLoader.createSyncrTable(t);
	}

	private void execCreate(String expression) throws SQLException {
		Statement st = null;
		try {
			st = conn.createStatement();
			st.executeUpdate(SQLConverter.escapeKeywords(expression));
		} finally {
			if (st != null)
				st.close();
		}
	}

	private void execInsert(PreparedStatement st, ArrayList<Object> values)
			throws SQLException {
		int i = 1;
		for (Object value : values) {
			st.setObject(i++, value);
		}
		st.execute();
	}

	public SQLWarning getLoadingWarnings() {
		if (this.viewsLoader.notLoaded.size() == 0
				&& this.tablesLoader.unresolvedTables.size() == 0) {
			return null;
		}
		SQLWarning sqlw = null;
		for (String s : this.viewsLoader.notLoaded) {
			String message = s.length() > 0 ? "Cannot load view " + s
					: "Cannot load views ";
			if (sqlw == null) {
				sqlw = new SQLWarning(message);
			} else {
				sqlw.setNextWarning(new SQLWarning(message));
			}
		}
		for (String s : this.tablesLoader.unresolvedTables) {
			String message = "Cannot resolve table " + s;
			if (sqlw == null) {
				sqlw = new SQLWarning(message);
			} else {
				sqlw.setNextWarning(new SQLWarning(message));
			}
		}
		return sqlw;
	}

	public void loadDB() throws SQLException, IOException {
		try {
			this.functionsLoader.loadMappedFunctions();
			this.tablesLoader.loadTables();
			this.tablesLoader.loadIndexes();
			this.viewsLoader.loadViews();
		} finally {
			Logger.log("Loaded Tables:");
			logsFlusher.dumpList(this.loadedTables);
			Logger.log("Loaded Queries:");
			logsFlusher.dumpList(this.loadedQueries);
			Logger.log("Loaded Indexes:");
			logsFlusher.dumpList(this.loadedIndexes, true);
			conn.close();
		}
	}

	public void loadTableData(Table t) throws IOException, SQLException {
		tablesLoader.loadTableData(t);
	}

	public void synchronisationTriggers(String tableName,
			boolean hasAutoNumberColumn) throws SQLException {
		this.triggersGenerator.synchronisationTriggers(tableName,
				hasAutoNumberColumn);
	}

	private boolean tryDefault(Object defaulT) throws SQLException {
		Statement st = null;
		try {
			st = conn.createStatement();
			st.executeQuery("SELECT " + defaulT
					+ " FROM   INFORMATION_SCHEMA.SYSTEM_TABLES  where 2=1");
			return true;
		} catch (Exception e) {
			return false;
		} finally {
			if (st != null)
				st.close();
		}
	}
}
