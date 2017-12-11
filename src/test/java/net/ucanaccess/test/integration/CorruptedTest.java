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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2007Test;

@RunWith(Parameterized.class)
public class CorruptedTest extends AccessVersion2007Test {

    private final static ByteArrayOutputStream ERR_CONTENT = new ByteArrayOutputStream();

    @BeforeClass
    public static void setUpStreams() {
        System.setErr(new PrintStream(ERR_CONTENT));
    }

    @AfterClass
    public static void cleanUpStreams() {
        System.setErr(System.err);
    }

    public CorruptedTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/corrupted.accdb"; // Access 2007
    }

    @Test
    public void testCorrupted() {
        getLogger().info("UcanaccessConnection: {}", ucanaccess);
        String err = "WARNING:integrity constraint violation: foreign key no parent; BABY_DADDYBABY table: BABY"
                + System.lineSeparator()
                + "WARNING:Detected Foreign Key constraint breach, table Baby, record Row[162:1][{ID=2,fk1=34}]: making the table Baby  readonly "
                + System.lineSeparator()
                + "WARNING:Detected Not Null constraint breach, table NotNull, record Row[140:0][{ID=1,notnull=<null>,vvv=gg,fk1=34}]: making the table NotNull  readonly "
                + System.lineSeparator()
                + "WARNING:integrity constraint violation: foreign key no parent; NOTNULL_DADDYNOTNULL table: NOTNULL"
                + System.lineSeparator()
                + "WARNING:Detected Foreign Key constraint breach, table NotNull, record Row[140:3][{ID=4,notnull=t,vvv=t,fk1=2}]: making the table NotNull  readonly "
                + System.lineSeparator()
                + "WARNING:Detected Unique constraint breach, table UK, record Row[181:1][{ID=2,uk=1}]: making the table UK  readonly";
        assertEquals(new String(err), new String(ERR_CONTENT.toByteArray()).trim());
    }
}
