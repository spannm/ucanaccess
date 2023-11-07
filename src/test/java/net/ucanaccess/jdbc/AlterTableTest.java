package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.Index.Column;
import net.ucanaccess.test.AccessVersion;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.util.HibernateSupport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

class AlterTableTest extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return TEST_DB_DIR + "badDb.accdb";
    }

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements(
            "CREATE TABLE AAAn (baaaa TEXT(3) PRIMARY KEY, A INTEGER, C TEXT(4))",
            "CREATE TABLE [AAA n] (baaaa TEXT(3), A INTEGER, C TEXT(4), b YESNO, d DATETIME DEFAULT NOW(), e NUMERIC(8,3), [f f] TEXT)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testRename(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            st.execute("ALTER TABLE [??###] RENAME TO [1GIà GIà]");
            assertThatThrownBy(() -> st.execute("ALTER TABLE T4 RENAME TO [1GIà GIà]"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("object name already exists");
            checkQuery("SELECT * FROM [1GIà GIà]");
            dumpQueryResult("SELECT * FROM [1GIà GIà]");
            getLogger().debug("After having renamed a few tables ...");
            dumpQueryResult("SELECT * FROM UCA_METADATA.TABLES");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testAddColumn(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            st.execute("ALTER TABLE AAAn RENAME TO [GIà GIà]");
            st.execute("INSERT INTO [GIà GIà] (baaaa) values('chi')");
            checkQuery("SELECT * FROM [GIà GIà] ORDER BY c");
            dumpQueryResult("SELECT * FROM [GIà GIà] ORDER BY c");
            st.execute("ALTER TABLE [GIà GIà] RENAME TO [22 alterTableTest123]");
            checkQuery("SELECT * FROM [22 alterTableTest123] ORDER BY c");
            dumpQueryResult("SELECT * FROM [22 alterTableTest123] ORDER BY c");
            st.execute("ALTER TABLE [22 alterTableTest123] ADD COLUMN [ci ci] TEXT(100) NOT NULL DEFAULT 'PIPPO'");
            st.execute("ALTER TABLE [22 alterTableTest123] ADD COLUMN [健康] decimal (23,5) ");
            st.execute("ALTER TABLE [22 alterTableTest123] ADD COLUMN [£健康] numeric (23,6) default 13.031955 NOT NULL");
            assertThatThrownBy(() -> st.execute("ALTER TABLE [22 alterTableTest123] ADD COLUMN defaultwwwdefault numeric (23,6) NOT NULL"))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("When adding a new column");
            st.execute("ALTER TABLE [22 alterTableTest123] ADD COLUMN [ci ci1] DATETIME NOT NULL DEFAULT NOW()");
            st.execute("ALTER TABLE [22 alterTableTest123] ADD COLUMN [ci ci2] YESNO");
            st.execute("INSERT INTO [22 alterTableTest123] (baaaa) VALUES('cha')");
            st.execute("ALTER TABLE [22 alterTableTest123] ADD COLUMN Memo Memo ");
            st.execute("ALTER TABLE [22 alterTableTest123] ADD COLUMN ole OLE ");
            dumpQueryResult("SELECT * FROM [22 alterTableTest123] ORDER BY c");
            checkQuery("SELECT * FROM [22 alterTableTest123] ORDER BY c");

            dumpQueryResult("SELECT * FROM Sample");
            checkQuery("SELECT * FROM Sample");
            st.executeUpdate("UPDATE sample SET Description='wRRRw'");
            dumpQueryResult("SELECT * FROM Sample");
            checkQuery("SELECT * FROM Sample");
            st.execute("ALTER TABLE Sample ADD COLUMN dt DATETIME DEFAULT NOW() ");
            dumpQueryResult("SELECT * FROM Sample");
            checkQuery("SELECT * FROM Sample");
            st.execute("Update sample set Description='ww'");
            dumpQueryResult("SELECT * FROM Sample");
            checkQuery("SELECT * FROM Sample");

            dumpQueryResult("SELECT * FROM UCA_METADATA.Columns");

            createFK();

            st.execute("ALTER TABLE Sample ADD COLUMN website HYPERLINK");
            ResultSet rs = ucanaccess.getMetaData().getColumns(null, null, "Sample", "website");
            rs.next();
            assertEquals("HYPERLINK", rs.getString("ORIGINAL_TYPE"));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testCreateIndex(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            String tbl = "AAA n";
            st.execute("CREATE UNIQUE INDEX [èèè 23] on [" + tbl + "] (a ASC, c ASC)");
            st.execute("INSERT INTO [" + tbl + "] (a, C) VALUES (24, 'su')");
            assertThatThrownBy(() -> st.execute("INSERT INTO [" + tbl + "] (a, C) VALUES (24, 'su')"))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("unique constraint or index violation");
            Database db = ucanaccess.getDbIO();
            db.getTable(tbl).getIndexes().stream().filter(Index::isUnique).filter(idx -> "èèè 23".equals(idx.getName())).findFirst()
                .ifPresentOrElse(idx -> assertThat(idx.getColumns().stream().map(Column::getName)).contains("A", "C"), () -> fail("Unique index not found"));

            st.execute("CREATE INDEX [健 康] on [" + tbl + "] (c DESC)");
            db.getTable(tbl).getIndexes().stream().filter(idx -> !idx.isUnique()).filter(idx -> "健 康".equals(idx.getName())).findFirst()
                .ifPresentOrElse(idx -> assertEquals("C", idx.getColumns().get(0).getName()), () -> fail("Index not found"));

            st.execute("CREATE INDEX [%健 康] on [" + tbl + "] (b, d, e)");
            db.getTable(tbl).getIndexes().stream().filter(idx -> !idx.isUnique()).filter(idx -> "%健 康".equals(idx.getName())).findFirst()
                .ifPresentOrElse(idx -> assertThat(idx.getColumns().stream().map(Column::getName))
                    .containsExactly("b", "d", "e"), () -> fail("Index not found"));

            st.execute("CREATE INDEX ciao on Sample (Description)");
            db.getTable("Sample").getIndexes().stream().filter(idx -> !idx.isUnique()).filter(idx -> "ciao".equals(idx.getName())).findFirst()
                .ifPresentOrElse(idx -> assertThat(idx.getColumns().stream().map(Column::getName))
                    .containsExactly("Description"), () -> fail("Index not found"));
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testCreatePk(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            String tbl = "AAA n";
            st.execute("ALTER TABLE [" + tbl + "] ADD PRIMARY KEY (baaaa, a)");
            Database db = ucanaccess.getDbIO();
            assertThat(db.getTable(tbl).getPrimaryKeyIndex().getColumns().stream().map(Column::getName))
                .containsExactly("baaaa", "A");
            
            st.execute("ALTER TABLE Sample ADD PRIMARY KEY (RegionId)");
            assertThat(db.getTable("Sample").getPrimaryKeyIndex().getColumns().stream().map(Column::getName))
                .containsExactly("RegionId");
        }
    }

    void createFK() throws SQLException, IOException {
        try (Statement st = ucanaccess.createStatement()) {
            // test case: constraint name specified
            st.execute("ALTER TABLE [AAA n] ADD CONSTRAINT [pippo1] FOREIGN KEY (c) REFERENCES [22 alterTableTest123] (baaaa) ON DELETE CASCADE");
            Database db = ucanaccess.getDbIO();
            Table tb = db.getTable("AAA n");
            Table tbr = db.getTable("22 alterTableTest123");
            assertThat(tb.getForeignKeyIndex(tbr).getColumns().stream().map(Column::getName))
                .containsExactly("C");

            // also verify that the Relationship name was actually used in the Access database ...
            tb = db.getSystemTable("MSysRelationships");
            IndexCursor crsr = CursorBuilder.createCursor(tb.getIndex("szRelationship"));
            Row r = crsr.findRowByEntry("pippo1");
            assertNotNull(r);
            // ... and the right name was used in the HSQLDB database
            Connection hsqldbConn = ucanaccess.getHSQLDBConnection();
            Statement hsqldbStmt = hsqldbConn.createStatement();
            ResultSet hsqldbRs =
                hsqldbStmt.executeQuery("SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
                    + "WHERE CONSTRAINT_TYPE='FOREIGN KEY' AND TABLE_NAME='AAA N'");
            hsqldbRs.next();
            assertEquals("AAA N_PIPPO1", hsqldbRs.getString(1));

            // now test dropping the foreign key
            // first try with Hibernate mode inactive
            HibernateSupport.setActive(false);
            assertThatThrownBy(() -> st.execute("ALTER TABLE [AAA n] DROP CONSTRAINT [pippo1]"))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("DROP CONSTRAINT is only supported for Hibernate hbm2ddl");
            // now try again with Hibernate mode active
            HibernateSupport.setActive(true);
            st.execute("ALTER TABLE [AAA n] DROP CONSTRAINT [pippo1]");
            // and verify that it actually got dropped
            assertThatThrownBy(() -> st.execute("ALTER TABLE [AAA n] DROP CONSTRAINT [pippo1]")) // again
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("object not found");
            HibernateSupport.setActive(null);

            // test case: constraint name not specified
            st.execute("ALTER TABLE Son ADD FOREIGN KEY (integer, txt) REFERENCES Father(id, txt) ON DELETE CASCADE");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testDoubleRelationship(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        // repro code from https://stackoverflow.com/q/49160150/2144390
        try (Statement st = ucanaccess.createStatement()) {
            String tableToBeReferenced = "PersonsTable";
            String tableWithTheReferences = "RelationShipsTable";

            st.execute("CREATE TABLE " + tableToBeReferenced + " (ID AUTOINCREMENT NOT NULL PRIMARY KEY, "
                + "Name VARCHAR(255))");

            st.execute("CREATE TABLE " + tableWithTheReferences + " (ID LONG NOT NULL PRIMARY KEY, "
                + "RelationShip VARCHAR(255) NOT NULL DEFAULT 'FRIENDS', "
                + "Person1Id LONG NOT NULL, "
                + "Person2Id LONG NOT NULL)");

            // reference #1
            st.execute("ALTER TABLE " + tableWithTheReferences
                + " ADD CONSTRAINT FOREIGN_KEY_1 FOREIGN KEY (Person1Id) REFERENCES "
                + tableToBeReferenced + "(ID) ON DELETE CASCADE");

            // reference #2
            st.execute("ALTER TABLE " + tableWithTheReferences
                + " ADD CONSTRAINT FOREIGN_KEY_2 FOREIGN KEY (Person2Id) REFERENCES "
                + tableToBeReferenced + "(ID) ON DELETE CASCADE");        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testMiscellaneous(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            st.execute("ALTER TABLE tx ADD CONSTRAINT pk PRIMARY KEY ([i d]) ");
            st.execute("ALTER TABLE tx ADD COLUMN [my best friend] LONG ");
            st.execute("ALTER TABLE tx ADD COLUMN [my worst friend] SINGLE ");
            st.execute("ALTER TABLE tx ADD COLUMN [Is Pippo] TEXT(100) ");
            st.execute("ALTER TABLE tx ADD COLUMN [Is not Pippo] TEXT DEFAULT \"what's this?\"");

            st.execute("CREATE TABLE tx1 (n1 long, [n 2] text)");
            st.execute("ALTER TABLE tx1 add primary key (n1, [n 2])");
            st.execute("ALTER TABLE tx ADD FOREIGN KEY ([my best friend], [Is Pippo]) REFERENCES tx1(n1, [n 2]) ON DELETE CASCADE");
            st.execute("INSERT INTO tx1 values(1,\"ciao\")");
            st.execute("INSERT INTO tx ([my best friend], [my worst friend], [Is Pippo]) values(1, 2, \"ciao\")");
            checkQuery("SELECT COUNT(*) FROM tx", 1);
            st.execute("DELETE FROM tx1");
            checkQuery("SELECT COUNT(*) FROM tx");
            checkQuery("SELECT COUNT(*) FROM tx", 0);
            st.execute("DROP TABLE tx ");
            st.execute("DROP TABLE tx1 ");

            st.execute("CREATE TABLE tx (id COUNTER PRIMARY KEY, [my best friend] LONG, [my worst friend] SINGLE, [Is Pippo] TEXT(100), [Is not Pippo] TEXT default \"what's this?\")");
            st.execute("CREATE TABLE tx1 (n1 LONG, [n 2] TEXT)");
            st.execute("ALTER TABLE tx1 ADD PRIMARY KEY (n1, [n 2])");
            st.execute("ALTER TABLE tx ADD FOREIGN KEY ([my best friend], [Is Pippo]) REFERENCES tx1(n1, [n 2]) ON DELETE SET NULL");
            st.execute("INSERT INTO tx1 VALUES(1, \"ciao\")");
            st.execute("INSERT INTO tx ([my best friend], [my worst friend], [Is Pippo]) VALUES(1, 2, \"ciao\")");
            checkQuery("SELECT COUNT(*) FROM tx", 1);
            st.execute("DELETE FROM tx1");
            checkQuery("SELECT COUNT(*) FROM tx", 1);
            checkQuery("SELECT * FROM tx", 1, null, 2.0, null, "what's this?");
            st.execute("CREATE UNIQUE INDEX IDX111 ON tx ([my best friend])");

            assertThatThrownBy(() -> st.execute("INSERT INTO tx ([my best friend], [my worst friend], [Is Pippo]) values(1, 2, \"ciao\")"))
                .isInstanceOf(UcanaccessSQLException.class)
                .hasMessageContaining("integrity constraint violation: foreign key no parent");
        }
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2007"})
    void testSqlErrors(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (Statement st = ucanaccess.createStatement()) {
            st.execute("CREATE TABLE tx2 (id COUNTER, [my best friend] LONG, [my worst friend] SINGLE, [Is Pippo] TEXT(100), [Is not Pippo] TEXT DEFAULT \"what's this?\")");
            st.execute("INSERT INTO tx2 ([my best friend], [my worst friend], [Is Pippo]) VALUES(1, 2, \"ciao\")");
            
            Map.of(
                "ALTER TABLE tx2 ADD CONSTRAINT PRIMARY KEY ([i d]) ", "unexpected token: PRIMARY",
                "ALTER TABLE tx2 ADD COLUMN [my best friend] ", "unexpected end of statement",
                "ALTER TABLE tx2 ADD CONSTRAINT FOREIGN KEY ([my best friend], [Is Pippo]) REFERENCES tx1(n1, [n 2]) ON DELETE CASCADE", "type not found or user lacks privilege: FOREIGN",
                "DROP TABLE tx2 CASCADE", "Feature not supported.",
                "ALTER TABLE tx2 ADD CONSTRAINT PRIMARY KEY (id)", "unexpected token: PRIMARY",
                "ALTER TABLE tx2 ALTER COLUMN [my best friend] SET DEFAULT 33", "Feature not supported.",
                "ALTER TABLE tx2 DROP COLUMN [my best friend]", "Feature not supported.",
                "ALTER TABLE tx2 ADD COLUMN [1 my best friend] LONG NOT NULL", "x2 already contains one or more records(1 records)").entrySet().stream().forEach(
                    e -> {
                        String ddl = e.getKey();
                        String expectedMessage = e.getValue();
                        assertThatThrownBy(() -> st.execute(ddl))
                            .isInstanceOf(UcanaccessSQLException.class)
                            .hasMessageEndingWith(expectedMessage);
                    });
        }
    }

}
