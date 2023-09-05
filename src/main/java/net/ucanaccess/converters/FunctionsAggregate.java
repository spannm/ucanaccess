package net.ucanaccess.converters;

import java.math.BigDecimal;
import java.sql.Timestamp;

public final class FunctionsAggregate {

    private FunctionsAggregate() {
    }

    public static Object first(Object in, Boolean flag, Object[] register, Integer[] counter) {
        if (flag) {
            return register[0];
        }
        if (register[0] == null) {
            register[0] = in;
        }
        if (counter[0] == null) {
            counter[0] = 0;
        }
        counter[0] = counter[0] + 1;

        return null;
    }

    public static BigDecimal first(BigDecimal in, Boolean flag, BigDecimal[] register, Integer[] counter) {
        return (BigDecimal) first(in, flag, (Object[]) register, counter);

    }

    public static String first(String in, Boolean flag, String[] register, Integer[] counter) {
        return (String) first(in, flag, (Object[]) register, counter);
    }

    public static Boolean first(Boolean in, Boolean flag, Boolean[] register, Integer[] counter) {
        return (Boolean) first(in, flag, (Object[]) register, counter);
    }

    public static Timestamp first(Timestamp in, Boolean flag, Timestamp[] register, Integer[] counter) {
        return (Timestamp) first(in, flag, (Object[]) register, counter);
    }

    public static Object last(Object in, Boolean flag, Object[] register, Integer[] counter) {
        if (flag) {
            return register[0];
        }
        register[0] = in;
        if (counter[0] == null) {
            counter[0] = 0;
        }
        counter[0] = counter[0] + 1;

        return null;
    }

    public static BigDecimal last(BigDecimal in, Boolean flag, BigDecimal[] register, Integer[] counter) {
        return (BigDecimal) last(in, flag, (Object[]) register, counter);

    }

    public static String last(String in, Boolean flag, String[] register, Integer[] counter) {
        return (String) last(in, flag, (Object[]) register, counter);
    }

    public static Boolean last(Boolean in, Boolean flag, Boolean[] register, Integer[] counter) {
        return (Boolean) last(in, flag, (Object[]) register, counter);
    }

    public static Timestamp last(Timestamp in, Boolean flag, Timestamp[] register, Integer[] counter) {
        return (Timestamp) last(in, flag, (Object[]) register, counter);
    }
}
