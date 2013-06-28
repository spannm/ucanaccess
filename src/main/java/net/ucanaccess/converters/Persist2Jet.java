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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import net.ucanaccess.commands.InsertCommand;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.complex.UnsupportedValue;
import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.jdbc.DBReference;
import net.ucanaccess.jdbc.OnReloadReferenceListener;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;
import org.hsqldb.HsqlDateTime;
import org.hsqldb.SessionInterface;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimestampData;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Table;

public class Persist2Jet {
	private static HashMap<String, List<String>> columnNamesCache = new HashMap<String, List<String>>();
	static {
		DBReference.addOnReloadRefListener(new OnReloadReferenceListener() {

			public void onReload() {
				columnNamesCache.clear();
			}
		});
	}

	public Map<String, Object> getRowPattern(Object[] varr, Table t)
			throws SQLException {
		String ntn = SQLConverter.basicEscapingIdentifier(t.getName()).toUpperCase();
		LinkedHashMap<String, Object> vl = new LinkedHashMap<String, Object>();
		int i = 0;
		Iterator<String> it = getColumnNames(ntn).iterator();
		while (it.hasNext()) {
			vl.put(it.next(), varr[i++]);
		}
		if (i == 0) {
			throw new SQLException("Cannot read table's metadata");
		}
		return escapeIdentifiers(vl, t);
	}
	
	
	public Object[] getValues(Map<String, Object> rowPattern, Table t)
	throws SQLException {
		Object[] values=new Object[rowPattern.size()];
		int i=0;
		for(Object obj:rowPattern.values()){
			values[i++]=obj;
		}
		return values;
	}
	

	private List<String> getColumnNames(String ntn) throws SQLException {
		UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
		if (!columnNamesCache.containsKey(ntn)) {
			ArrayList<String> ar = new ArrayList<String>();
			ResultSet rs = conn.getMetaData().getColumns(null, "PUBLIC", ntn,
					null);
			while (rs.next()) {
				ar.add(rs.getString("COLUMN_NAME").toUpperCase());
			}
			columnNamesCache.put(ntn, ar);
		}
		return columnNamesCache.get(ntn);
	}

	public void convertRowTypes(Object[] values, Table t)
			throws SQLException {
		try {
			List<Column> columns=t.getColumns();
			Iterator<Column> it = columns.iterator();
			for (int i = 0; i < values.length; ++i) {
				Object value = values[i];
				Column column = it.next();
				if (value != null) {
					if (value instanceof TimestampData) {
						if (column.getType().equals(DataType.SHORT_DATE_TIME)) {
							TimestampData ts = (TimestampData) value;
							TimeZone zone = TimeZone.getDefault();
							GregorianCalendar cal = new GregorianCalendar(zone);
							long millis = HsqlDateTime.convertMillisToCalendar(
									cal, ts.getSeconds() * 1000);
							java.sql.Timestamp val = new java.sql.Timestamp(
									millis);
							val.setNanos(ts.getNanos());
							values[i] = val;
						}
					}
					if (value instanceof BlobData) {
						BlobData bd = (BlobData) value;
						JDBCConnection hsqlConn = (JDBCConnection) UcanaccessConnection
								.getCtxConnection().getHSQLDBConnection();
						SessionInterface si = hsqlConn.getSession();
						long length = bd.length(si);
						values[i] = ((BlobData) value).getBytes(si, 0,
								(int) length);
					}
					if(value instanceof JavaObjectData){
						JavaObjectData jod=(JavaObjectData)value;
						Object obj=jod.getObject();
						if(obj instanceof ComplexBase[]&&!(obj instanceof UnsupportedValue[]))
							values[i] =obj;
						else throw new UcanaccessSQLException(ExceptionMessages.UNSUPPORTED_TYPE);
					}
				}
			}
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	private LinkedHashMap<String, Object> escapeIdentifiers(
			LinkedHashMap<String, Object> map, Table t) {
		List<Column> colums = t.getColumns();
		LinkedHashMap<String, Object> vl = new LinkedHashMap<String, Object>();
		for (Column cl : colums) {
			String key = cl.getName();
			vl.put(key, map.get(SQLConverter.escapeIdentifier(key)
					.toUpperCase()));
		}
		return vl;
	}

	private List<Column> getColumns(String tableName, String[] types)
			throws SQLException {
		UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
		ArrayList<Column> arcl = new ArrayList<Column>();
		ResultSet rs = conn.getMetaData().getColumns(null, "PUBLIC",
				tableName.toUpperCase(), null);
		int i = 0;
		while (rs.next()) {
			Column cl = new Column();
			cl.setName(rs.getString("COLUMN_NAME"));
			short length = (short) rs.getInt("COLUMN_SIZE");
			byte precision=(byte) rs.getInt("DECIMAL_DIGITS");
			DataType dt = null;
			if (length == 0&&types!=null) {
				if (types[i]
						.equalsIgnoreCase(AccessType.MEMO.name())) {
					dt = DataType.MEMO;
					cl.setType(dt);
				}
				if (types[i]
						.equalsIgnoreCase(AccessType.TEXT.name())) {
					dt = DataType.TEXT;
					cl.setType(dt);
				}
			}
			
			if (    types!=null&&types[i]!=null&&(
					types[i].equalsIgnoreCase(AccessType.LONG.name())||
					types[i].equalsIgnoreCase(AccessType.BYTE.name())||
					types[i].equalsIgnoreCase(AccessType.CURRENCY.name())||
					types[i].equalsIgnoreCase(AccessType.INTEGER.name())||
					types[i].equalsIgnoreCase(AccessType.SINGLE.name())||
					types[i].equalsIgnoreCase(AccessType.DOUBLE.name())||
					types[i].equalsIgnoreCase(AccessType.YESNO.name())||
					types[i].equalsIgnoreCase(AccessType.DATETIME.name())||
					types[i].equalsIgnoreCase(AccessType.COUNTER.name())
			)
			){
			dt = TypesMap.map2Jackcess(AccessType.valueOf(types[i].toUpperCase()));
			cl.setType(dt);
			cl.setLengthInUnits((short)dt.getFixedSize());
			}
			
		
			
			if (dt == null) {
				dt = DataType.fromSQLType(rs.getInt("DATA_TYPE"), length);
				cl.setType(dt);
				if(length>0&&dt.equals(DataType.TEXT))
				cl.setLengthInUnits(length);
				if(precision>0)
				cl.setPrecision(precision);
			}

			arcl.add(cl);
			if (types != null) {
				if (types[i]
						.equalsIgnoreCase(AccessType.COUNTER.name()))
					cl.setAutoNumber(true);
				if (types[i]
						.equalsIgnoreCase(AccessType.GUID.name())) {
					cl.setType(DataType.GUID);
					cl.setAutoNumber(true);
				}
			}
			
			++i;
		}
		return arcl;
	}

	private List<IndexBuilder> getIndexBuilders(String tableName)
			throws SQLException {
		ArrayList<IndexBuilder> arcl = new ArrayList<IndexBuilder>();
		IndexBuilder ibpk = getIndexBuilderPK(tableName);
		addIndexBuildersSimple(tableName, arcl);
		checkPK(arcl, ibpk);
		if (ibpk != null)
			arcl.add(ibpk);
		return arcl;
	}

	private void checkPK(ArrayList<IndexBuilder> arcl, IndexBuilder ibpk) {
		if (ibpk == null)
			return;
		Iterator<IndexBuilder> itib = arcl.iterator();
		List<IndexBuilder.Column> clspk = ibpk.getColumns();
		ArrayList<String> columnNamesPK = new ArrayList<String>();
		for (IndexBuilder.Column clpk : clspk) {
			columnNamesPK.add(clpk.getName().toUpperCase());
		}
		while (itib.hasNext()) {
			IndexBuilder ib = itib.next();
			List<IndexBuilder.Column> cls = ib.getColumns();
			if (cls.size() != clspk.size())
				continue;
			boolean clsPK = true;
			for (IndexBuilder.Column cl : cls) {
				if (!columnNamesPK.contains(cl.getName().toUpperCase())) {
					clsPK = false;
					break;
				}
			}
			if (clsPK) {
				itib.remove();
			}
		}
	}

	private IndexBuilder getIndexBuilderPK(String tableName)
			throws SQLException {
		UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
		ResultSet pkrs = conn.getMetaData().getPrimaryKeys(null, "PUBLIC",
				tableName.toUpperCase());
		IndexBuilder indpk = null;
		while (pkrs.next()) {
			if (indpk == null) {
				String indexName = IndexBuilder.PRIMARY_KEY_NAME;
				indpk = new IndexBuilder(indexName);
				indpk.setPrimaryKey();
				
			}
			indpk.addColumns(pkrs.getString("COLUMN_NAME"));
		}
		return indpk;
	}

	private void addIndexBuildersSimple(String tableName,
			ArrayList<IndexBuilder> arcl) throws SQLException {
		UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
		ResultSet idxrs = conn.getMetaData().getIndexInfo(null, "PUBLIC",
				tableName, false, false);
		HashMap<String, IndexBuilder> hi = new HashMap<String, IndexBuilder>();
		for (IndexBuilder ib : arcl) {
			hi.put(ib.getName(), ib);
		}
		while (idxrs.next()) {
			String colName = idxrs.getString("COLUMN_NAME");
			String indexName = idxrs.getString("INDEX_NAME");
			boolean unique = !idxrs.getBoolean("NON_UNIQUE");
			String ad = idxrs.getString("ASC_OR_DESC");
			boolean asc = ad == null || ad.equals("A");
			if (!hi.containsKey(indexName)) {
				IndexBuilder ib = new IndexBuilder(indexName);
				if (unique)
					ib.setUnique();
				arcl.add(ib);
				hi.put(indexName, ib);
			}
			IndexBuilder toIdx = hi.get(indexName);
			toIdx.addColumns(asc, colName);
		}
	}

	public void createTable(String tableName, String[] types)
			throws IOException, SQLException {
		UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
		Database db = conn.getDbIO();
		db.doUseBigIndex();
		db.createTable(tableName, getColumns(tableName, types),
				getIndexBuilders(tableName));
		Table table = db.getTable(tableName);
		Statement st = null;
		try {
			st = conn.createStatement();
			ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
			List<String> clns = this.getColumnNames(tableName);
			while (rs.next()) {
				Object[] record = new Object[clns.size()];
				int i = 0;
				for (String columnName : clns) {
					record[i++] = rs.getObject(columnName);
				}
				new InsertCommand(table, record, null).persist();
			}
		} finally {
			if (st != null) {
				st.close();
			}
		}
	}

	public void dropTable(String tableName) throws IOException {
		UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
		Database db = conn.getDbIO();
		Table t = db.getTable(tableName);
		if (t == null)
			return;
		while (t.getNextRow() != null) {
			t.deleteCurrentRow();
		}
		Table cat = db.getSystemCatalog();
		Map<String, Object> row;
		while ((row = cat.getNextRow()) != null) {
			String name = (String) row.get("Name");
			if (name != null && name.equalsIgnoreCase(tableName)) {
				Integer id = (Integer) row.get("Id");
				Table tsa = db.getSystemTable("MSysACEs");
				HashMap<String, Object> rowtsa = new HashMap<String, Object>();
				rowtsa.put("ObjectId", id);
				Cursor cur = Cursor.createCursor(tsa);
				if (cur.findNextRow(rowtsa)) {
					cur.deleteCurrentRow();
				}
				cat.deleteCurrentRow();
				Table srs = db.getSystemTable("MSysRelationships");
				while ((row = srs.getNextRow()) != null) {
					String szObject = (String) row.get("szObject");
					String szReferencedObject = (String) row
							.get("szReferencedObject");
					if ((szObject != null && szObject
							.equalsIgnoreCase(tableName))
							|| (szReferencedObject != null && szReferencedObject
									.equalsIgnoreCase(tableName))) {
						srs.deleteCurrentRow();
					}
				}
			}
		}
		conn.reloadDbIO();
	}
}
