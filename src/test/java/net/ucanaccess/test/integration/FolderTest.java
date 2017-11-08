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
package net.ucanaccess.test.integration;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessDriver;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionDefaultTest;

@RunWith(Parameterized.class)
public class FolderTest extends AccessVersionDefaultTest {

    public FolderTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Test
    public void testFolderContent() throws SQLException, IOException, ClassNotFoundException {
        Statement st = null;
        String folderPath = System.getProperty("accessFolder");
        if (folderPath == null) {
            return;
        }
        File folder = new File(folderPath);
        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
        for (File fl : folder.listFiles()) {
            try {
                String url = UcanaccessDriver.URL_PREFIX + fl.getAbsolutePath();
                Connection conn = DriverManager.getConnection(url);
                SQLWarning sqlw = conn.getWarnings();
                getLogger().info("open {}", fl.getAbsolutePath());
                while (sqlw != null) {
                    getLogger().info(sqlw.getMessage());
                    sqlw = sqlw.getNextWarning();
                }

            } catch (Exception _ex) {
                getLogger().info("error {}", fl.getAbsolutePath());
            } finally {
                if (st != null) {
                    st.close();
                }
            }
        }
    }
}
