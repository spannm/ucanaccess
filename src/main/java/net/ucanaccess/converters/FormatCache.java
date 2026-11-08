package net.ucanaccess.converters;

import java.text.DecimalFormat;

public final class FormatCache {
    private static DecimalFormat noArgs;

    private FormatCache() {
    }

    public static DecimalFormat getNoArgs() {
        if (noArgs == null) {
            noArgs = new DecimalFormat();
        }
        return noArgs;
    }
}
