package net.ucanaccess.type;

/**
 * Valid values of driver property {@link Property} {@code columnOrder}.
 *
 * @author Markus Spann
 * @since v5.1.0
 */
public enum ColumnOrder {

    DATA,
    DISPLAY;

    public static ColumnOrder parse(String _val) {
        if (_val == null) {
            return null;
        }
        String val = _val.strip().toLowerCase();
        for (ColumnOrder ver : values()) {
            if (val.equalsIgnoreCase(ver.name())) {
                return ver;
            }
        }
        return null;
    }

}
