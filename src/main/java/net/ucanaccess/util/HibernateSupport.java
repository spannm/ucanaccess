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
package net.ucanaccess.util;

/**
 * Utilities for interoperability with the UCanAccess Hibernate dialect.
 *
 * https://sourceforge.net/projects/ucanaccess-hibernate-dialect/
 *
 * @author Gord
 *
 */
public class HibernateSupport {

    private static final String UCA_HIBERNATE_ISACTIVE_PROPERTY =
            "net.ucanaccess.hibernate.dialect.UCanAccessDialect.isActive";

    private static Boolean active = null;

    /**
     * Checks the Java system property set by the constructor of the UCanAccessDialect class, indicating that we are
     * running under a Hibernate process.
     * 
     * @return whether the property was found and its value was "true"
     */
    public static Boolean isActive() {
        if (active == null) {
            active = System.getProperty(UCA_HIBERNATE_ISACTIVE_PROPERTY, "false").equals("true");
        }
        return active;
    }

    /**
     * Sets the Hibernate "active" status so tests can validate the Hibernate-specific code.
     */
    public static void setActive(Boolean value) {
        active = value;
    }
}
