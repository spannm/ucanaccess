package net.ucanaccess.util;

/**
 * Utilities for interoperability with the UCanAccess Hibernate dialect.
 *
 * https://sourceforge.net/projects/ucanaccess-hibernate-dialect/
 *
 * @author Gord
 *
 */
public final class HibernateSupport {

    private static final String UCA_HIBERNATE_ISACTIVE_PROPERTY =
        "net.ucanaccess.hibernate.dialect.UCanAccessDialect.isActive";

    private static Boolean active = null;

    private HibernateSupport() {
        // this is a utility class, so no instantiation allowed
        throw new UnsupportedOperationException();
    }

    /**
     * Checks the Java system property set by the constructor of the UCanAccessDialect class, indicating that we are
     * running under a Hibernate process.
     *
     * @return whether the property was found and its value was "true"
     */
    public static boolean isActive() {
        if (active == null) {
            active = System.getProperty(UCA_HIBERNATE_ISACTIVE_PROPERTY, "false").equalsIgnoreCase("true");
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
