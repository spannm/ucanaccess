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
import java.util.Arrays;
import java.util.List;

import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.ComplexBase;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.complex.Version;
import net.ucanaccess.converters.Persist2Jet;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.triggers.AutoNumberManager;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.Table.ColumnOrder;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.impl.ColumnImpl;


public class InsertCommand implements ICommand {
	private Database dbIO;
	private String execId;
	private Object[] newRow;
	private Table table;
	private String tableName;

	public InsertCommand(String tableName, Database dbIO, Object[] newRow,
			String execId) {
		super();
		this.tableName = tableName;
		this.dbIO = dbIO;
		this.newRow = newRow;
		this.execId = execId;
	}

	public InsertCommand(Table table, Object[] newRow, String execId) {
		super();
		this.table = table;
		this.tableName = table.getName();
		this.newRow = newRow;
		this.execId = execId;
	}

	public String getExecId() {
		return execId;
	}

	public String getTableName() {
		return tableName;
	}

	public TYPES getType() {
		return TYPES.INSERT;
	}

	private Object[] mementoRow() {
		Object[] clone = new Object[newRow.length];
		int i = 0;
		for (Object obj : newRow) {
			clone[i] = obj;
			++i;
		}
		return clone;
	}

	private void initComplex() {
		for (int i = 0; i < newRow.length; ++i) {
			if (newRow[i] instanceof ComplexBase) {
				newRow[i] = Column.AUTO_NUMBER;
			}
		}
	}

	public IFeedbackAction persist() throws SQLException {
		try {
			AutoNumberAction ana=null;
			if (table == null)
				table = this.dbIO.getTable(this.tableName);
			Object[] memento = mementoRow();
			initComplex();
			int j = 0;
			List<? extends Column> lc= table.getColumns();
			if (table.getDatabase().getColumnOrder()
					.equals(ColumnOrder.DISPLAY)) {
				Object[] newRowReorded=new Object[newRow.length];
				Column[] cllReorded=new Column[newRow.length];
				for (Column cli : table.getColumns()){
					newRowReorded[cli.getColumnIndex()]=newRow[j];
					memento[cli.getColumnIndex()]=newRow[j];
					 cllReorded[cli.getColumnIndex()]=cli;
					j++;
				}
				newRow=newRowReorded; 
				lc=Arrays.asList(cllReorded);
			}
			
			
			table.addRow(newRow);
			j = 0;
			for (Column cli : lc) {
				ColumnImpl cl=(ColumnImpl)cli;
				if (cl.isAutoNumber()
						&& !memento[j].equals(newRow[j])
						&& !cl.getAutoNumberGenerator().getType()
						.equals(DataType.COMPLEX_TYPE)
				) {
					
					if(cl.getAutoNumberGenerator().getType()
						.equals(DataType.LONG)){
						AutoNumberManager.reset(cl, (Integer) newRow[j]);
					}
					ana= new AutoNumberAction(table, memento, newRow);
				} 

			if (cl.getType() == DataType.COMPLEX_TYPE) {
				ComplexValueForeignKey rowFk = (ComplexValueForeignKey) cl
							.getRowValue(newRow);
					if (memento[j] instanceof Attachment[]) {
						Attachment[] atcs = (Attachment[]) memento[j];
						for (Attachment atc : atcs) {
							rowFk.addAttachment(atc.getUrl(), atc.getName(),
									atc.getType(), atc.getData(),
									atc.getTimeStamp(), atc.getFlags());
							
						}
					} else if (memento[j] instanceof SingleValue[]) {
						SingleValue[] vs = (SingleValue[]) memento[j];
						for (SingleValue v : vs) {
							rowFk.addMultiValue(v.getValue());
						}
						
					} else if (memento[j] instanceof Version[]) {
						Version[] vs = (Version[]) memento[j];
						for (Version v : vs) {
							rowFk.addVersion(v.getValue(),v.getModifiedDate());
						}
					}
				}
				++j;
			}
			return ana;
		} catch (IOException e) {
			throw new UcanaccessSQLException(e);
		}
	}

	public IFeedbackAction rollback() throws SQLException {
		if (this.table != null) {
			DeleteCommand dc = new DeleteCommand(this.table,
					new Persist2Jet().getRowPattern(this.newRow, this.table),
					this.execId);
			return dc.persist();
		} else
			// a drop table cleans all
			return null;
	}
}
