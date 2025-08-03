package net.ucanaccess.util;

/**
 * A utility class providing methods for interoperability with the UCanAccess Hibernate dialect.
 * <p>
 * This class is designed to check for the presence of the Hibernate dialect at runtime.
 * <p>
 * For more information on the Hibernate dialect, see the project page:
 * <a href="https://sourceforge.net/projects/ucanaccess-hibernate-dialect/">UCanAccess Hibernate dialect</a>
 *
 * @author Gord Thompson
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
     * Checks a Java system property to determine if the application is running with the UCanAccess Hibernate dialect.
     * The property is set by the constructor of the {@code UCanAccessDialect} class.
     *
     * @return {@code true} if the property was found and its value is "true", otherwise {@code false}.
     */
    public static boolean isActive() {
        if (active == null) {
            active = System.getProperty(UCA_HIBERNATE_ISACTIVE_PROPERTY, "false").equalsIgnoreCase("true");
        }
        return active;
    }

    /**
     * Sets the Hibernate "active" status for the current process.
     * This method is primarily intended for use in test environments to simulate the presence of the Hibernate dialect.
     *
     * @param value The new active status.
     */
    public static void setActive(Boolean value) {
        active = value;
    }
}
