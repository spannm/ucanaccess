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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import net.ucanaccess.converters.SQLConverter;
import net.ucanaccess.jdbc.BlobKey;
import net.ucanaccess.jdbc.UcanaccessConnection;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Table;

public class BlobAction implements IFeedbackAction {
    private Table            table;
    private boolean          containsBlob;
    private HashSet<BlobKey> keys = new HashSet<BlobKey>();

    public BlobAction(Table _table, Object[] newValues) throws SQLException {
        this.table = _table;

        if (!BlobKey.hasPrimaryKey(_table)) {
            return;
        }
        Index pk = _table.getPrimaryKeyIndex();
        HashSet<String> hsKey = new HashSet<String>();
        for (Index.Column icl : pk.getColumns()) {
            hsKey.add(icl.getName());
        }
        HashSet<String> hsBlob = new HashSet<String>();
        int i = 0;
        HashMap<String, Object> keyMap = new HashMap<String, Object>();
        for (Column cl : _table.getColumns()) {
            if (cl.getType().equals(DataType.OLE) && newValues[i] != null) {
                containsBlob = true;
                hsBlob.add(cl.getName());
            }
            if (hsKey.contains(cl.getName())) {
                keyMap.put(cl.getName(), newValues[i]);
            }
            ++i;
        }
        for (String cln : hsBlob) {
            keys.add(new BlobKey(keyMap, table.getName(), cln));
        }

    }

    @Override
    public void doAction(ICommand toChange) throws SQLException {
        if (containsBlob) {
            UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
            Connection connHsqldb = conn.getHSQLDBConnection();
            PreparedStatement ps = null;
            for (BlobKey bkey : keys) {
                String sql = "UPDATE " + SQLConverter.escapeIdentifier(table.getName(), connHsqldb) + " SET "
                        + SQLConverter.escapeIdentifier(bkey.getColumnName(), connHsqldb) + "=? WHERE ";
                StringBuilder sb = new StringBuilder();
                String and = "";
                ArrayList<Object> values = new ArrayList<Object>();
                for (Map.Entry<String, Object> me : bkey.getKey().entrySet()) {
                    sb.append(and).append(SQLConverter.escapeIdentifier(me.getKey(), connHsqldb)).append(" = ?");
                    values.add(me.getValue());
                    and = " AND ";
                }
                sql += sb.toString();
                conn.setFeedbackState(true);

                try {
                    conn.setFeedbackState(true);
                    ps = connHsqldb.prepareStatement(sql);
                    ps.setObject(1, bkey.getBytes());
                    int j = 2;
                    for (Object value : values) {
                        ps.setObject(j, value);
                        ++j;
                    }
                    ps.executeUpdate();
                    conn.setFeedbackState(false);
                } finally {
                    if (ps != null) {
                        ps.close();
                    }
                }
            }
        }
    }

}
