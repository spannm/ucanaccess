package net.ucanaccess.type;

/**
 * Valid values of driver property {@code columnOrder}.
 *
 * @author Markus Spann
 * @since v5.1.0
 */
public enum ColumnOrder {

    DATA,
    DISPLAY;

    public static ColumnOrder parse(String value) {
        if (value == null) {
            return null;
        }
        String val = value.strip().toLowerCase();
        for (ColumnOrder ver : values()) {
            if (val.equalsIgnoreCase(ver.name())) {
                return ver;
            }
        }
        return null;
    }

}
