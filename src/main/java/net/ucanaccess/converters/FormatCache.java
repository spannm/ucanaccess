package net.ucanaccess.converters;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

public final class FormatCache {
    private static DecimalFormat noArgs;
    private static DecimalFormat zpzz;
    private static DecimalFormat sharp;
    private static DecimalFormat noGrouping;
    private static NumberFormat  currencyDefault;

    private static final Map<String, DecimalFormat> CACHE = new HashMap<String, DecimalFormat>();

    private FormatCache() {
    }

    public static synchronized DecimalFormat getDecimalFormat(String _pattern) {
        if (!CACHE.containsKey(_pattern)) {
            DecimalFormat dc = new DecimalFormat(_pattern);
            dc.setRoundingMode(RoundingMode.HALF_UP);
            CACHE.put(_pattern, dc);
        }
        return CACHE.get(_pattern);
    }

    public static DecimalFormat getNoArgs() {
        if (noArgs == null) {
            noArgs = new DecimalFormat();
        }
        return noArgs;
    }

    public static DecimalFormat getZpzz() {
        if (zpzz == null) {
            zpzz = new DecimalFormat("0.00");
            zpzz.setRoundingMode(RoundingMode.HALF_UP);
        }
        return zpzz;
    }

    public static DecimalFormat getSharp() {
        if (sharp == null) {
            sharp = new DecimalFormat("###,###.##");
        }
        return sharp;
    }

    public static DecimalFormat getNoGrouping() {
        if (noGrouping == null) {
            noGrouping = new DecimalFormat();
            noGrouping.setGroupingUsed(false);
        }
        return noGrouping;
    }
    
    public static NumberFormat getCurrencyDefault() {
        if (currencyDefault == null) {
            currencyDefault = NumberFormat.getCurrencyInstance();
        }
        return currencyDefault;
    }
}
