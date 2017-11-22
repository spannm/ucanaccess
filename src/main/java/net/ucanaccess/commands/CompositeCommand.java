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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.Cursor;

import net.ucanaccess.jdbc.UcanaccessSQLException;

public class CompositeCommand implements ICommand {
    private List<ICursorCommand> composite     = new ArrayList<ICursorCommand>();
    private Map<String, Object>  currentRow;
    private String               execId;
    private IndexSelector        indexSelector;
    private List<ICursorCommand> rollbackCache = new ArrayList<ICursorCommand>();

    public CompositeCommand() {
    }

    public boolean add(ICursorCommand c4io) {
        if (this.indexSelector == null) {
            this.indexSelector = c4io.getIndexSelector();
            this.execId = c4io.getExecId();
        }
        return composite.add(c4io);
    }

    public List<ICursorCommand> getComposite() {
        return composite;
    }

    @Override
    public String getExecId() {
        return this.execId;
    }

    @Override
    public String getTableName() {
        return composite.get(0).getTableName();
    }

    @Override
    public TYPES getType() {
        return TYPES.COMPOSITE;
    }

    public boolean moveToNextRow(Cursor cur, Set<String> columnNames) throws IOException {
        boolean hasNext = cur.moveToNextRow();
        if (hasNext) {
            this.currentRow = cur.getCurrentRow(columnNames);
        }
        return hasNext;
    }

    @Override
    public IFeedbackAction persist() throws SQLException {
        try {
            Cursor cur = indexSelector.getCursor();
            cur.beforeFirst();
            Set<String> columnNames = composite.get(0).getRowPattern().keySet();
            while (composite.size() > 0 && moveToNextRow(cur, columnNames)) {
                Iterator<ICursorCommand> it = composite.iterator();
                while (it.hasNext()) {
                    ICursorCommand comm = it.next();
                    if (comm.currentRowMatches(cur, this.currentRow)) {
                        comm.persistCurrentRow(cur);
                        it.remove();
                        rollbackCache.add(comm);
                        break;
                    }
                }
            }
            return null;
        } catch (IOException e) {
            throw new UcanaccessSQLException(e);
        }
    }

    @Override
    public IFeedbackAction rollback() throws SQLException {
        for (ICursorCommand ic : this.rollbackCache) {
            ic.rollback();
        }
        return null;
    }
}
