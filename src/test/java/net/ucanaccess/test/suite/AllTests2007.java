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
package net.ucanaccess.test.suite;

import net.ucanaccess.test.ComplexTest;
import net.ucanaccess.test.UcanaccessTestBase;

import com.healthmarketscience.jackcess.Database.FileFormat;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests2007 {
    public static Test suite() throws ClassNotFoundException {
        UcanaccessTestBase.setDefaultFileFormat(FileFormat.V2007);
        TestSuite ts = AllTestsBase.suite();
        ts.addTestSuite(ComplexTest.class);
        return ts;
    }
}
