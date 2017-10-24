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

package net.ucanaccess.converters;

import static com.healthmarketscience.jackcess.impl.query.QueryFormat.NEWLINE;

import java.util.ArrayList;
import java.util.List;

import com.healthmarketscience.jackcess.impl.query.AppendQueryImpl;

public class AppendQueryTemp extends AppendQueryImpl {

    public AppendQueryTemp(AppendQueryImpl impl) {
        super(impl.getName(), impl.getRows(), impl.getObjectId(), impl.getObjectFlag());
    }

    @Override
    protected void toSQLString(StringBuilder builder) {
        builder.append("INSERT INTO ");
        toOptionalQuotedExpr(builder, getTargetTable(), true);
        toRemoteDb(builder, getRemoteDbPath(), getRemoteDbType());
        builder.append(NEWLINE);
        List<String> values = getValues();
        if (!values.isEmpty()) {
            List<String> decl = getInsertDeclaration();
            if (decl.size() > 0) {
                builder.append("(").append(getInsertDeclaration()).append(")");
            }
            builder.append(" VALUES (").append(values).append(')');
        } else {
            List<String> decl = getSelectDeclaration();
            if (decl.size() > 0) {
                builder.append("(").append(getSelectDeclaration()).append(")");
            }
            toSQLSelectString(builder, true);
        }
    }

    private List<String> getInsertDeclaration() {
        return new RowFormatter(getDeclaration(getValueRows())) {
            @Override
            protected void format(StringBuilder builder, Row row) {
                String column = row.name2;
                if (!(column.startsWith("[") && column.endsWith("]"))) {
                    column = "[" + row.name2 + "]";
                }
                builder.append(column);
            }
        }.format();
    }

    private List<String> getSelectDeclaration() {
        return new RowFormatter(getDeclaration(getColumnRows())) {
            @Override
            protected void format(StringBuilder builder, Row row) {
                String column = row.name2;
                if (!(column.startsWith("[") && column.endsWith("]"))) {
                    column = "[" + row.name2 + "]";
                }
                builder.append(column);
            }
        }.format();
    }

    private List<Row> getDeclaration(List<Row> valueRows) {
        ArrayList<Row> ardc = new ArrayList<Row>();
        for (Row row : valueRows) {
            if (row.name2 != null) {
                ardc.add(row);
            }
        }
        return ardc;
    }

}
