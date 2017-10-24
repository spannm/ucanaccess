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
package net.ucanaccess.triggers;

import java.sql.SQLException;
import java.util.UUID;

import org.hsqldb.Trigger;
import org.hsqldb.types.JavaObjectData;

import net.ucanaccess.complex.Attachment;
import net.ucanaccess.complex.SingleValue;
import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.util.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.impl.ColumnImpl;

public class TriggerAutoNumber extends TriggerBase {
    public static int           autorandom   = -1;
    private static final String GUID_PATTERN =
            "\\s*[{]?([\\p{XDigit}]{8})-([\\p{XDigit}]{4})-([\\p{XDigit}]{4})-([\\p{XDigit}]{4})-([\\p{XDigit}]{12})[}]?\\s*";

    @Override
    public void fire(int type, String name, String tableName, Object[] oldR, Object[] newR) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        if (conn.isFeedbackState()) {
            return;
        }
        try {
            Table t = getTable(tableName, conn);
            if (t == null) {
                throw new RuntimeException(Logger.getMessage("TABLE_DOESNT_EXIST") + " :" + tableName);
            }
            int i = 0;
            for (Column cli : t.getColumns()) {
                ColumnImpl cl = (ColumnImpl) cli;
                if (cli.getType().equals(DataType.COMPLEX_TYPE) && (newR[i] == null || "".equals(newR[i]))) {
                    if (cli.getComplexInfo().getType().equals(ComplexDataType.ATTACHMENT)) {
                        newR[i] = new JavaObjectData(new Attachment[0]);
                    } else if (cli.getComplexInfo().getType().equals(ComplexDataType.MULTI_VALUE)) {
                        newR[i] = new JavaObjectData(new SingleValue[0]);
                    }
                } else

                if (cl.isAutoNumber()) {
                    if (type == Trigger.INSERT_BEFORE_ROW) {

                        if (t.isAllowAutoNumberInsert()) {
                            if (cl.getAutoNumberGenerator().getType().equals(DataType.LONG) && newR[i] != null) {
                                AutoNumberManager.bump(cl, (Integer) newR[i]);
                            }
                        } else {
                            if (cl.getAutoNumberGenerator().getType().equals(DataType.GUID)) {

                                if (newR[i] == null) {
                                    newR[i] = "{" + UUID.randomUUID().toString().toUpperCase() + "}";
                                }
                                conn.setGeneratedKey(newR[i]);
                            } else if (cl.getAutoNumberGenerator().getType().equals(DataType.LONG)) {
                                int keyg = AutoNumberManager.getNext(cl);
                                newR[i] = keyg;
                                conn.setGeneratedKey(newR[i]);
                            }
                        }
                    } else if (type == Trigger.UPDATE_BEFORE_ROW
                            && cl.getAutoNumberGenerator().getType().equals(DataType.LONG)) {
                        if (!oldR[i].equals(newR[i])) {
                            throw new RuntimeException("Cannot update autoincrement column");
                        }
                    }

                    else if (cl.getAutoNumberGenerator().getType().equals(DataType.GUID)) {
                        validateGUID(newR[i]);
                    }
                } else if (DataType.BOOLEAN.equals(cl.getType())) {
                    if (newR[i] == null) {
                        newR[i] = false;
                    }
                }
                ++i;
            }
        } catch (Exception e) {
            throw new TriggerException(e.getMessage());
        }
    }

    private void validateGUID(Object guid) throws SQLException {
        if (guid != null && guid instanceof String) {
            String guidS = (String) guid;
            if (guidS.length() != 38 || !guidS.matches(GUID_PATTERN)) {
                throw new SQLException("Invalid guid format " + guidS);
            }
        }
    }
}
