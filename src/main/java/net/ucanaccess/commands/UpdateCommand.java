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
package net.ucanaccess.commands;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.complex.Version;
import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.Table.ColumnOrder;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;

public class UpdateCommand extends AbstractCursorCommand {
	private List<Column> blobColumns;
	private String execId;
	private IndexSelector indexSelector;
	private Object[] modifiedRow;
	private Map<String, Object> rowPattern;
	private Table table;
	private List<? extends Column> tableColumns;
	private boolean isRollbacking;

	public UpdateCommand(Table table, Map<String, Object> map,
			Object[] modifiedRow, String execId) {
		super();
		this.tableColumns = table.getColumns();
		this.indexSelector = new IndexSelector(table);
		this.rowPattern = map;
		this.modifiedRow = modifiedRow;
		this.execId = execId;
		checkBlob(modifiedRow);
		this.table = table;
	}

	private void checkBlob(Object[] newRow2) {
		for (int i = 0; i < newRow2.length; i++) {
			if (newRow2[i] instanceof org.hsqldb.types.BlobData) {
				if (blobColumns == null) {
					blobColumns = new ArrayList<Column>();
				}
				blobColumns.add(tableColumns.get(i));
			}
		}
	}

	public String getExecId() {
		return execId;
	}

	public IndexSelector getIndexSelector() {
		return indexSelector;
	}

	public Map<String, Object> getRowPattern() {
		return rowPattern;
	}

	public String getTableName() {
		return this.table.getName();
	}

	public TYPES getType() {
		return TYPES.UPDATE;
	}

	public IFeedbackAction persist() throws SQLException {
		try {
			Cursor cur = indexSelector.getCursor();
			if (cur.findNextRow(rowPattern)) {
				if (this.blobColumns != null) {
					for (Column col : this.blobColumns) {
						Object val = cur.getCurrentRowValue(col);
						modifiedRow[tableColumns.indexOf(col)] = val;
					}
				}
				updateComplex(cur);
				persist(cur);

			}
		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}
		return null;
	}

	

	public void persistCurrentRow(Cursor cur) throws IOException, SQLException {
		if (this.blobColumns != null) {
			for (Column col : this.blobColumns) {
				Object val = cur.getCurrentRowValue(col);
				modifiedRow[tableColumns.indexOf(col)] = val;

			}
		}
		updateComplex(cur);
		persist(cur);
	}

	private void updateComplex(Cursor cur) throws IOException {
		int j = 0;

		for (Column cl : this.tableColumns) {
			if (cl.getType() == DataType.COMPLEX_TYPE) {
				ComplexValueForeignKey rowFk = (ComplexValueForeignKey) cl
						.getRowValue(cur.getCurrentRow());

				if (modifiedRow[j] instanceof Attachment[]) {
					rowFk.deleteAllValues();
					Attachment[] atcs = (Attachment[]) modifiedRow[j];
					for (Attachment atc : atcs) {
						rowFk.addAttachment(atc.getUrl(), atc.getName(),
								atc.getType(), atc.getData(),
								atc.getTimeStamp(), atc.getFlags());

					}
				} else if (modifiedRow[j] instanceof SingleValue[]) {
					rowFk.deleteAllValues();
					SingleValue[] vs = (SingleValue[]) modifiedRow[j];
					for (SingleValue v : vs) {
						rowFk.addMultiValue(v.getValue());
					}

				} else if (modifiedRow[j] instanceof Version[]) {

					Version[] vs = (Version[]) modifiedRow[j];
					Version v = vs[0];
					List<com.healthmarketscience.jackcess.complex.Version> oldV = rowFk
							.getVersions();
					String vn = v.getValue();
					String vo = oldV.size() > 0 ? oldV.get(0).getValue() : null;
					Date upTime = isRollbacking ? new Date() : v
							.getModifiedDate();

					if ((vn != null && vo == null)
							|| (vo != null && vn == null)
							|| (vo != null && vn != null && !vo.equals(vn)))
						rowFk.addVersion(vn, upTime);

				}
			}
			j++;
		}
	}

	public IFeedbackAction rollback() throws SQLException {
		Persist2Jet p2a = new Persist2Jet();

		UpdateCommand urev = new UpdateCommand(this.table, p2a.getRowPattern(
				this.modifiedRow, this.table), p2a.getValues(
				this.getRowPattern(), this.table), this.execId);
		urev.isRollbacking = true;
		return urev.persist();
	}
	
	private void persist(Cursor cur) throws IOException, SQLException {
		Object[] mr=this.modifiedRow;
		if (table.getDatabase().getColumnOrder()
				.equals(ColumnOrder.DISPLAY)) {
			Object[] newRowReorded=new Object[this.modifiedRow.length];
			int j=0;
			for (Column cli : table.getColumns()){
				newRowReorded[cli.getColumnIndex()]=this.modifiedRow[j];
				j++;
			}
			mr=newRowReorded;
		} 
		cur.updateCurrentRow(mr);
	}
	
}
