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
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hsqldb.error.ErrorCode;


import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.ext.FunctionType;
import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.util.Logger;
import net.ucanaccess.util.Logger.Messages;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.PropertyMap.Property;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.impl.IndexImpl;
import com.healthmarketscience.jackcess.query.Query;

public class LoadJet {
	private static int namingCounter = 0;

	private final class FunctionsLoader {
		private HashSet<String> functionsDefinition = new HashSet<String>();

		private void addAggregates() {
			functionsDefinition.add(getAggregate("LONGVARCHAR", "last"));
			functionsDefinition.add(getAggregate("DECIMAL(100,10)", "last"));
			functionsDefinition.add(getAggregate("BOOLEAN", "last"));
			functionsDefinition.add(getAggregate("TIMESTAMP", "last"));
			functionsDefinition.add(getAggregate("LONGVARCHAR", "first"));
			functionsDefinition.add(getAggregate("DECIMAL(100,10)", "first"));
			functionsDefinition.add(getAggregate("BOOLEAN", "first"));
			functionsDefinition.add(getAggregate("TIMESTAMP", "first"));
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
				execCreate(functionDef,true);
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
		private static final int HSQL_FK_ALREADY_EXISTS = -ErrorCode.X_42528;   // -5528;
		private static final int HSQL_UK_ALREADY_EXISTS= -ErrorCode.X_42522;//-5522
		private static final String SYSTEM_SCHEMA = "SYS";
		private ArrayList<String> unresolvedTables = new ArrayList<String>();
		private static final String  EXPRESSION="Expression";
		private static final String RESULT_TYPE = "ResultType";
		private String commaSeparated(List<? extends Index.Column> columns) {
			String comma = "";
			StringBuffer sb = new StringBuffer(" (");
			for (Index.Column cd : columns) {
				sb.append(comma)
						.append(SQLConverter.escapeIdentifier(cd.getColumn()
								.getName()));
				comma = ",";
			}
			return sb.append(") ").toString();
		}
		
		private String schema(String name,boolean systemTable){
			if(systemTable){
				return SYSTEM_SCHEMA+"."+name;
			}
			return name;
		}
		
		
		private DataType getReturnType(Column cl) throws IOException{
			if(cl.getProperties().get(EXPRESSION)==null||cl.getProperties().get(RESULT_TYPE)==null)
				return null;
			byte pos=(Byte)	cl.getProperties().get(RESULT_TYPE).getValue();
			return DataType.fromByte(pos);
		
		}
		
		
		private String getHsqldbColumnType(Column cl) throws IOException{
			String htype ;
			DataType dtyp=cl.getType();
			DataType rtyp= getReturnType( cl);
			boolean calcType=false;
			if( rtyp!=null){
				dtyp=rtyp;
				calcType=true;
			}
						
			if( dtyp.equals(DataType.TEXT)){
				int ln=ff1997?cl.getLength():cl.getLengthInUnits();
				htype="VARCHAR("
					+ ln + ")" ;
				}
			else if(dtyp.equals(DataType.NUMERIC)&&(cl.getScale()>0||calcType)){
				if(calcType){
					htype="NUMERIC(100 ,4)";
				}
				else{
					htype="NUMERIC("+
						(cl.getPrecision()>0?cl.getPrecision():100)+","+
						cl.getScale()+")";
				}
			}
			else if(dtyp.equals(DataType.FLOAT)&&calcType){
				htype="NUMERIC("+
				(cl.getPrecision()>0?cl.getPrecision():100)+","+
				4+")";
			}
			else{
				htype=TypesMap.map2hsqldb(dtyp);
			}
			return htype;
		}
		
		
		private String getCalculatedFieldTrigger(String ntn,Column cl) throws IOException{
			DataType dt=getReturnType( cl);
			String fun=null;
			if(isNumeric(dt)){
				fun="formulaToNumeric";
			}else if(isBoolean(dt)){
				fun="formulaToBoolean";
			}else if(isDate(dt)){
				fun="formulaToDate";
			}else if(isTextual(dt)){
				fun="formulaToText";
			}
			String call= fun==null?"%s":fun+"(%s,'"+dt.name()+"')";
			
			String trg="CREATE TRIGGER expr%d %s ON "+ntn+
			   " REFERENCING NEW as newrow FOR EACH ROW "+
			   " BEGIN  ATOMIC "+
			   " SET newrow."+SQLConverter.escapeIdentifier(cl.getName())+" = "+call+
			    	
			    "; END ";
			
			return trg;
		}
		
		private boolean isNumeric(DataType dt){
			return typeGroup(dt,DataType.NUMERIC,DataType.MONEY,DataType.DOUBLE,DataType.FLOAT,DataType.LONG,DataType.INT,DataType.BYTE);
		}
		
		private boolean isDate(DataType dt){
			return typeGroup(dt,DataType.SHORT_DATE_TIME);
		}
		
		private boolean isBoolean(DataType dt){
			return typeGroup(dt,DataType.BOOLEAN);
		}
		
		private boolean isTextual(DataType dt){
			return typeGroup(dt,DataType.MEMO,DataType.TEXT);
		}
		
		private boolean typeGroup(DataType dt,DataType...gr){
			for(DataType el:gr){
				if(el.equals(dt))return true;
			}
			return false;
		}

		
		private void createSyncrTable(Table t,boolean systemTable) throws SQLException, IOException {
			String tn = t.getName();
			String ntn =schema( SQLConverter.escapeIdentifier(tn),systemTable);
			StringBuffer sbC = new StringBuffer("CREATE  CACHED TABLE ").append(
					ntn).append("(");
			List<? extends Column> lc = t.getColumns();
			String comma = "";
			ArrayList<String> arTrigger = new ArrayList<String>();
			for (Column cl : lc) {
				if("USER".equalsIgnoreCase(cl.getName())){
					Logger.logParametricWarning(Messages.USER_AS_COLUMNNAME,t.getName());
				}
				String expr=getExpression(cl);
				if(expr!=null){
					String tgr=getCalculatedFieldTrigger(ntn,cl);
					arTrigger.add(String.format(tgr,namingCounter++,"before insert",SQLConverter.convertFormula(expr)));
				}
				
				String htype= getHsqldbColumnType(cl) ;
				sbC.append(comma)
						.append(SQLConverter.escapeIdentifier(cl.getName()))
						.append(" ").append(htype);
				
				
				PropertyMap pm = cl.getProperties();
				Object required = pm.getValue(PropertyMap.REQUIRED_PROP);
				if (required != null&&
						required instanceof Boolean && ((Boolean) required)		
						) {
					sbC.append(" NOT NULL ");
				}
				comma = ",";
			}
			sbC.append(")");
			execCreate(sbC.toString(),true);
			for (String trigger : arTrigger) {
				try{
					execCreate(trigger,false);
				}catch(SQLException e){
					Logger.logWarning(e.getMessage());
					break;
				}
			}
		}

		private String getExpression(Column cl) throws IOException {
			PropertyMap map=cl.getProperties();
             Property exprp=map.get( EXPRESSION);
             
             if(exprp!=null){
            	Table tl=cl.getTable();
				String expr= SQLConverter.convertPowOperator((String)exprp.getValue());
				for(Column cl1:tl.getColumns()){
					expr=expr.replaceAll("\\[(?i)("+Pattern.quote(cl1.getName())+")\\]","newrow.$0");
				}
			    return expr;
			}
			return null;
		}

		private  void defaultValues(Table t) throws SQLException, IOException {
			String tn = t.getName();
			 String ntn = SQLConverter.escapeIdentifier(tn);
			List<? extends Column> lc = t.getColumns();
			ArrayList<String> arTrigger = new ArrayList<String>();
			for (Column cl : lc) {
				PropertyMap pm = cl.getProperties();
				String ncn = SQLConverter.escapeIdentifier(cl.getName());
				Object defaulT = pm.getValue(PropertyMap.DEFAULT_VALUE_PROP);
				if (defaulT != null) {
					String cdefaulT = SQLConverter.convertSQL(" "
							+ defaulT.toString());
					if (cdefaulT.trim().startsWith("=")) {
						cdefaulT = cdefaulT.trim().substring(1);
					}
					if (cl.getType().equals(DataType.BOOLEAN)
							&& ("=yes".equalsIgnoreCase(cdefaulT) || "yes"
									.equalsIgnoreCase(cdefaulT)))
						cdefaulT = "true";
					if (cl.getType().equals(DataType.BOOLEAN)
							&& ("=no".equalsIgnoreCase(cdefaulT) || "no"
									.equalsIgnoreCase(cdefaulT)))
						cdefaulT = "false";
					if(
							(cl.getType().equals(DataType.MEMO)||
							cl.getType().equals(DataType.TEXT))&&
							(!defaulT.toString().startsWith("\"")||
							 !defaulT.toString().endsWith("\"")		
							)
							
					){
						cdefaulT="'"+cdefaulT.replaceAll("'","''")+"'";
					}
					String guidExp = "GenGUID()";
					if (!guidExp.equals(defaulT)) {
						if (!tryDefault(cdefaulT)) {
							
							Logger.logParametricWarning(Messages.UNKNOWN_EXPRESSION,
							""+ defaulT, cl.getName(), cl.getTable().getName());
						} else {
							
							if(cl.getType()==DataType.TEXT
									&&
								defaulT.toString().startsWith("'")&&defaulT.toString().endsWith("'")&&
								defaulT.toString().length()>cl.getLengthInUnits()
							){
								Logger.logParametricWarning(Messages.DEFAULT_VALUES_DELIMETERS,
										""+defaulT,cl.getName(),cl.getTable().getName(),""+cl.getLengthInUnits());
							}
								arTrigger
										.add("CREATE TRIGGER DEFAULT_TRIGGER"
												+ (namingCounter++)
												+ " BEFORE INSERT ON "
												+ ntn
												+ "  REFERENCING NEW ROW AS NEW FOR EACH ROW IF NEW."
												+ ncn + " IS NULL THEN "
												+ "SET NEW." + ncn + "= "
												+ cdefaulT + " ; END IF");

						}
					}
				}
			}
			for (String trigger : arTrigger) {
				execCreate(trigger,true);
			}
		}

		private void loadForeignKey(Index idxi) throws IOException, SQLException {
			IndexImpl idx=(IndexImpl)idxi;
			String ntn = SQLConverter
					.escapeIdentifier(idx.getTable().getName());
			if(ntn==null)return;
			String nin = SQLConverter.escapeIdentifier(idx.getName());
			nin = (ntn + "_" + nin).replaceAll("\"", "").replaceAll("\\W", "_");
			String colsIdx = commaSeparated(idx.getColumns());
			String colsIdxRef = commaSeparated(idx.getReferencedIndex()
					.getColumns());
			StringBuffer ci = new StringBuffer("ALTER TABLE ").append(ntn);
			ci.append(" ADD CONSTRAINT ").append(nin);
			String nrt = SQLConverter.escapeIdentifier(idx.getReferencedIndex()
					.getTable().getName());
			if(nrt==null)return;
			ci.append(" FOREIGN KEY ").append(colsIdx).append(" REFERENCES ")
					.append(nrt).append(colsIdxRef);
			//riw
			
			if (idx.getReference().isCascadeDeletes()) {
				ci.append(" ON DELETE CASCADE ");
			}
			if (idx.getReference().isCascadeUpdates()) {
				ci.append(" ON UPDATE CASCADE ");
			}
			try {
				execCreate(ci.toString(),true);
			} catch (SQLException e) {
				if (e.getErrorCode() == HSQL_FK_ALREADY_EXISTS) {
					Logger.log(e.getMessage());
				} else
					throw e;
			}
			loadedIndexes.add("FK on " + ntn + " Columns:" + colsIdx
					+ " References " + nrt + " Columns:" + colsIdxRef);
		}

		private void loadIndex(Index idx) throws IOException, SQLException {
			String ntn = SQLConverter
					.escapeIdentifier(idx.getTable().getName());
			if(ntn==null)return;
			String nin = SQLConverter.escapeIdentifier(idx.getName());
			nin = (ntn + "_" + nin).replaceAll("\"", "").replaceAll("\\W", "_");
			boolean uk = idx.isUnique();
			boolean pk = idx.isPrimaryKey();
			if(uk&&idx.getColumns().size()==1){
				Column cl=idx.getColumns().get(0).getColumn();
				DataType dt=cl.getType();
				if(dt.equals(DataType.COMPLEX_TYPE))return;
			}
			
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
				execCreate(ci.toString(),true);
			} 
			catch (SQLException e) {
				if( HSQL_UK_ALREADY_EXISTS== e.getErrorCode())return ;
				if (idx.isUnique()) {
					for (Index.Column cd : idx.getColumns()) {
						if (cd.getColumn().getType()
								.equals(DataType.COMPLEX_TYPE)) {
							return;
						}
					}
				}
				Logger.logWarning(e.getMessage());
				return;
			}
			catch (Exception e) {
				
				Logger.logWarning(e.getMessage());
				return;
			}
			String pre = pk ? "Primary Key " : uk ? "Index Unique " : "Index";
			loadedIndexes.add(pre + " on " + ntn + " Columns:" + colsIdx);
			
		}

		private void loadIndexes() throws SQLException, IOException {
			for (String tn : dbIO.getTableNames()) {
				if (!this.unresolvedTables.contains(tn)){
					this.loadTableIndexes(tn);
					conn.commit();
				}
					
			}
			for (String tn : dbIO.getTableNames()) {
				if (!this.unresolvedTables.contains(tn)){
					this.loadTableFKs(tn);
					conn.commit();
				}
			}
		}
		
		private void loadTable(Table t) throws SQLException, IOException {
			loadTable(t,false) ;
		}

		private void loadTable(Table t, boolean systemTable) throws SQLException, IOException {
			String tn = t.getName();
			if (tn.indexOf(" ") > 0) {
				SQLConverter.addWhiteSpacedTableNames(tn);
			}
			String ntn = SQLConverter.escapeIdentifier(tn);
			if (ntn == null)
				return;
			List<Integer> cfil=calculatedFieldIndexList( t) ;
			createSyncrTable(t,systemTable);
			loadTableData(t,cfil,systemTable);
			if(!systemTable){
				defaultValues(t);
				triggersGenerator.synchronisationTriggers(ntn,
					hasAutoNumberColumn(t),hasAppendOnly(t));
				loadedTables.add(ntn);
			}
		}
		private boolean hasAppendOnly(Table t) {
			for (Column c:t.getColumns()){
				if(c.isAppendOnly()){
					return true;
				}
			}
			return false;
		}

	   private void loadTableData(Table t,List<Integer> cfil, boolean systemTable) throws IOException, SQLException {
			PreparedStatement ps = null;
			try {
				for (int i = 0; i < t.getRowCount(); i++) {
					ArrayList<Object> values = new ArrayList<Object>();
					
					Map<String, Object> row = t.getNextRow();
					if (row == null)
						continue;
					if (ps == null)
						ps = sqlInsert(t, row,cfil,systemTable);
					Set<Entry<String, Object>> se = row.entrySet();
					
					for (Entry<String, Object> en : se) {
						Column cl=t.getColumn(en.getKey());
						if(cl!=null&&cfil.contains(cl.getColumnIndex()))continue;
						values.add(value(en.getValue()));
					}
					execInsert(ps, values);
					if((i>0&&i%1000==0)||
							i==t.getRowCount()-1		){
						conn.commit();
					}
				}
			} finally {
				if (ps != null)
					ps.close();
			}
		}

		private void loadTableFKs(String tableName) throws IOException,
				SQLException {
			Table table = dbIO.getTable(tableName);
			for (Index idxi : table.getIndexes()) {
				//riw
				IndexImpl idx=(IndexImpl)idxi;
				if (idx.isForeignKey() && !idx.getReference().isPrimaryTable())
					loadForeignKey(idx);
				}
		}

		private void loadTableIndexes(String tableName) throws IOException,
				SQLException {
			Table table = dbIO.getTable(tableName);
			for (Index idx : table.getIndexes()) {
				if (!idx.isForeignKey()&&idx.isPrimaryKey()){
					loadIndex(idx);
				}
			}
			//PK before avoid issues on reload with keepMirror parameter
			for (Index idx : table.getIndexes()) {
				if (!idx.isForeignKey()&&!idx.isPrimaryKey()){
					loadIndex(idx);
				}
			}
			
			//to be removed in the 2.0.7.2
			if(hasCalculatedFields( table) ){
				execCreate("SET TABLE "+SQLConverter.escapeIdentifier(table.getName())+" READONLY TRUE ",false);
			}
		}
		private boolean hasCalculatedFields(Table t) throws IOException{
			return calculatedFieldIndexList(t).size()>0; 
		}
		
		
		private List<Integer> calculatedFieldIndexList(Table t) throws IOException{
			ArrayList<Integer> ari=new ArrayList<Integer>(); 
			for(Column cl:t.getColumns()){
				if(cl.getProperties().get(EXPRESSION)!=null)ari.add(cl.getColumnIndex());
			}
			return ari;
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
			if(sysSchema){
			createSystemSchema();
			for (String tn : dbIO.getSystemTableNames()) {
				Table t = null;
				try {
					t = dbIO.getSystemTable(tn);
				
				if (t != null){
					loadTable(t,true);
					execCreate("SET TABLE "+schema(SQLConverter.escapeIdentifier(t.getName()),true)+" READONLY TRUE ",false);
					execCreate("GRANT SELECT  ON "+schema(SQLConverter.escapeIdentifier(t.getName()),true)+" TO PUBLIC ",false);
				}
		    	} catch (Exception ignore) {}
		    	}
			}
			
		}

		private void createSystemSchema() throws SQLException {
			execCreate("CREATE SCHEMA "+SYSTEM_SCHEMA+" AUTHORIZATION DBA",false);
			
			
		}

		private PreparedStatement sqlInsert(Table t, Map<String, Object> row,List<Integer> cfil, boolean systemTable)
				throws IOException, SQLException {
			String tn = t.getName();
			String ntn = schema(SQLConverter.escapeIdentifier(tn),systemTable);
			String comma = "";
			StringBuffer sbI = new StringBuffer(" INSERT INTO ").append(ntn)
					.append(" (");
			StringBuffer sbE = new StringBuffer(" VALUES( ");
			Set<Entry<String, Object>> se = row.entrySet();
			comma = "";
			for (Entry<String, Object> en : se) {
				String cn=en.getKey();
				Column cl=t.getColumn(cn);
				if(cl!=null&&cfil.contains(cl.getColumnIndex()))continue;
				sbI.append(comma).append(
						SQLConverter.escapeIdentifier(cn));
				sbE.append(comma).append(" ? ");
				comma = ",";
			}
			sbI.append(") ");
			sbE.append(")");
			sbI.append(sbE);
			
			return conn.prepareStatement(sbI.toString());
		}

		private Object value(Object value) throws SQLException {
			if (value == null)
				return null;
			if (value instanceof Float) {
				BigDecimal bd = new BigDecimal(value.toString());
				return bd;
			}
			if (value instanceof Date && !(value instanceof Timestamp)) {
				Timestamp ts = new Timestamp(((Date) value).getTime());
				return ts;
			}
			if (value instanceof ComplexValueForeignKey) {
				try {
					return ComplexBase.convert((ComplexValueForeignKey) value);
				} catch (IOException e) {
					throw new UcanaccessSQLException(e);
				}
			}
			if(value instanceof Byte){
				return SQLConverter.asUnsigned((Byte)value);
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
					+ className + "\" ",true);
		}

		private void loadTriggerNP(String tableName, String namePrefix,
				String when, String className) throws SQLException {
			loadTrigger(tableName, namePrefix, when, DEFAULT_TRIGGERS_PACKAGE
					+ "." + className);
		}

		private void synchronisationTriggers(String tableName,
				boolean hasAutoNumberColumn, boolean hasAutoAppendOnly) throws SQLException {
			loadTriggerNP(tableName, "genericInsert", "AFTER INSERT",
					"TriggerInsert");
			loadTriggerNP(tableName, "genericUpdate", "AFTER UPDATE",
					"TriggerUpdate");
			loadTriggerNP(tableName, "genericDelete", "AFTER DELETE",
					"TriggerDelete");
			if (hasAutoAppendOnly) {
				loadTriggerNP(tableName, "appendOnly", "BEFORE INSERT",
						"TriggerAppendOnly");
				loadTriggerNP(tableName, "appendOnly_upd", "BEFORE UPDATE",
						"TriggerAppendOnly");
			}
			if (hasAutoNumberColumn) {
				loadTriggerNP(tableName, "autonumber", "BEFORE INSERT",
						"TriggerAutoNumber");
				loadTriggerNP(tableName, "autonumber_validate",
						"BEFORE UPDATE", "TriggerAutoNumber");
			}
		}
	}

	private final class ViewsLoader {
		private HashMap<String,String> notLoaded = new HashMap<String,String>();
		private static final int OBJECT_ALREADY_EXISTS=-ErrorCode.X_42504;
		
		
		private boolean loadView(Query q) throws SQLException {
			return loadView(q,null);
		}
		
		private boolean loadView(Query q, String queryWKT) throws SQLException {
			String qnn = SQLConverter.basicEscapingIdentifier(q.getName());
			if(qnn==null){
				return false;
			}
			if (qnn.indexOf(" ") > 0) {
				SQLConverter.addWhiteSpacedTableNames(q.getName());
			}
			String escqn = qnn.indexOf(' ') > 0 ? "[" + qnn + "]" : qnn;
			String querySQL =queryWKT==null? q.toSQLString():queryWKT;
			Pivot pivot = null;
			if (q.getType().equals(Query.Type.CROSS_TAB)) {
				pivot = new Pivot(conn);
				if (!pivot.parsePivot(querySQL)
						|| (querySQL = pivot.toSQL()) == null) {
					this.notLoaded.put(q.getName(),"cannot load this query");
					return false;
				}
			}
			querySQL =new DFunction(conn, querySQL ).toSQL();
			StringBuffer sb = new StringBuffer("CREATE VIEW ").append(escqn)
					.append(" AS ").append(querySQL);
			String v = null;
			try {
				v = SQLConverter.convertSQL(sb.toString(), true);
				if(v.trim().endsWith(";"))
					v=v.trim().substring(0, v.length()-1);
				execCreate(v,false);
				loadedQueries.add(qnn);
				this.notLoaded.remove(qnn);
				if (pivot != null) {
					pivot.registerPivot(qnn);
				}
				return true;
			} 
			
			catch (Exception e) {
				if(e instanceof SQLSyntaxErrorException&&queryWKT==null&&((SQLSyntaxErrorException)e).getErrorCode()==OBJECT_ALREADY_EXISTS){
					return loadView( q, solveAmbiguous(querySQL));
				}
				String cause=UcanaccessSQLException.explaneCause(e);
				
				this.notLoaded.put(q.getName(),": "+cause);
				
				
				if (!err) {
					Logger.log("Error occured while loading:" + q.getName());
					Logger.log("Converted view was :" + v);
					Logger.log("Error message was :" + e.getMessage());
					err = true;
				}
				return false;
			}
		}
	
		private  String solveAmbiguous(String sql) {
			try{
				sql=sql.replaceAll("[\n\r]", " ");
				Pattern pt=Pattern.compile("(.*)[\n\r\\s]*(?i)SELECT([\n\r\\s].*[\n\r\\s])(?i)FROM([\n\r\\s])(.*)");
				Matcher mtc =pt.matcher(sql);
				if(mtc.find()){
					String select =mtc.group(2);
					String pre=mtc.group(1)==null?"":mtc.group(1);
					String[] splitted=select.split(",",-1);
					StringBuffer sb=new StringBuffer (pre+" select ");
					LinkedList<String> lkl=new LinkedList<String>();
				
				for(String s:splitted){
					int j=s.lastIndexOf(".");
					
					Pattern aliasPt=Pattern
					.compile("[\\s\n\r]+(?i)AS[\\s\n\r]+");
					boolean alias=aliasPt.matcher(s).find();
					if(j<0||alias){
						lkl.add(s);
					}else{
						String k=s.substring(j+1);
						if(lkl.contains(k)){
							int idx=lkl.indexOf(k);
							String old=lkl.get(lkl.indexOf(k));
							lkl.remove(old);
							lkl.add(idx,splitted[idx]+ " AS ["+splitted[idx].trim() +"]");
							lkl.add(s + " AS ["+s.trim()+"]");
						}else{
							lkl.add(k);
						}
					}
				}
				String comma="";
				for(String s:lkl){
					sb.append(comma).append(s);
					
					comma=",";
				}
				sb.append(" FROM ").append(mtc.group(4)); 
			
				return sb.toString();
			}
			else return sql;
				}catch(Exception e){
					return sql;
			}
		}
		
		private void loadViews() throws SQLException, IOException {
			List<Query> lq = null;
			try {
				lq = dbIO.getQueries();
				Iterator<Query> it = lq.iterator();
				while (it.hasNext()) {
					Query q = it.next();
					if (!q.getType().equals(Query.Type.SELECT)
							&& !q.getType().equals(Query.Type.UNION)
							&& !q.getType().equals(Query.Type.CROSS_TAB)) {
						  	it.remove();
					}
						
					
				}
				queryPorting(lq);
			} catch (Exception e) {
				this.notLoaded.put("","");
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
					String qtxt =null;
					boolean qryGot=true;
					try{
					 qtxt = q.toSQLString().toLowerCase();
					}catch(Exception ignore){
						qryGot=false;
					}
					boolean foundDep = false;
					if (qryGot&&!heavy)
						for (String name : arn) {
							if (qtxt.indexOf(name) != -1) {
								foundDep = true;
								break;
							}
						}
					if (qryGot&&!foundDep && loadView(q)) {
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
	private boolean sysSchema;
	private boolean ff1997;

	public LoadJet(Connection conn, Database dbIO) {
		super();
		this.conn = conn;
		this.dbIO = dbIO;
		try {
			this.ff1997= FileFormat.V1997.equals(this.dbIO.getFileFormat());
		} catch (Exception ignore) {
			// Logger.logWarning(e.getMessage());
		}
	}
	
	public void loadDefaultValues(Table t) throws SQLException, IOException{
		this.tablesLoader.defaultValues(t);
	}

	private static boolean hasAutoNumberColumn(Table t) {
		List<? extends Column> lc = t.getColumns();
		for (Column cl : lc) {
			if (cl.isAutoNumber()||DataType.BOOLEAN.equals(cl.getType())) {
				return true;
			}
		}
		return false;
	}

	public void addFunctions(Class<?> clazz) throws SQLException {
		this.functionsLoader.addFunctions(clazz);
	}

	private void execCreate(String expression, boolean logging) throws SQLException {
		Statement st = null;
		try {
			
			st = conn.createStatement();
			st.executeUpdate(expression);
		} catch(SQLException e){
			if(logging&&e.getErrorCode()!=TablesLoader.HSQL_FK_ALREADY_EXISTS)
			Logger.log("Cannot execute:"+expression+" "+e.getMessage());
			
			throw e;
		}
		
		finally {
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
		for (String s : this.viewsLoader.notLoaded.keySet()) {
			String message = s.length() > 0 ? "Cannot load view " + s+ " "+this.viewsLoader.notLoaded.get(s)
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
			conn.commit();
			SQLConverter.cleanEscaped();
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



	public void synchronisationTriggers(String tableName,
			boolean hasAutoNumberColumn,boolean hasAppendOnly) throws SQLException {
		this.triggersGenerator.synchronisationTriggers(tableName,
				hasAutoNumberColumn,hasAppendOnly);
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

	public void setSysSchema(boolean sysSchema) {
		this.sysSchema=sysSchema;
		
	}
	

}
