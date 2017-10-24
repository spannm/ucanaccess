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

import net.ucanaccess.commands.DeleteCommand;
import net.ucanaccess.jdbc.UcanaccessConnection;

import com.healthmarketscience.jackcess.Table;

public class TriggerDelete extends TriggerBase {
    @Override
    public void fire(int type, String name, String tableName, Object[] oldR, Object[] newR) {
        checkContext();
        UcanaccessConnection conn = UcanaccessConnection.getCtxConnection();
        String execId = UcanaccessConnection.getCtxExcId();
        try {
            Table t = getTable(tableName, conn);
            super.convertRowTypes(oldR, t);
            DeleteCommand c4j = new DeleteCommand(t, getRowPattern(oldR, t), execId);
            conn.add(c4j);
        } catch (Exception e) {
            throw new TriggerException(e.getMessage());
        }
    }
}
