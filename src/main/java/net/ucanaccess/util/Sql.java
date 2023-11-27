package net.ucanaccess.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * <p>
 *   An immutable sql statement string created from multiple tokens
 *   in order to write inline sql statements in an easy-to-read fashion
 *   spread out over multiple lines of code.
 * </p>
 *
 * <p>
 *   The class implements {@link CharSequence} and thus can be used as a drop-in
 *   alternative wherever API supports {@code CharSequence} rather than {@code String}.
 * </p>
 *
 * Please note that the validity of the statement is never checked,
 * and that {@code null} or empty inputs are permitted (no run-time exceptions).<br>
 *
 * The input of multiple tokens is formatted into a single String by
 * removing leading and trailing whitespace and concatenating
 * non-empty tokens by a single space character.
 * Further, any trailing semicolons are removed from the resulting sql string.
 *
 * <p>Example:</p>
 *
 * <pre>
 *     String tblName = "table";
 *     Sql.of("SELECT COUNT(*)",
 *            "FROM", tblName,
 *            " WHERE cond1 = :cond1",
 *            "   AND cond2 = :cond2");
 * </pre>
 *
 * @author Markus Spann
 * @since v5.1.0
 */
public final class Sql implements CharSequence {

    private static final Sql EMPTY_SQL = new Sql("");

    /** The internal sql string. Cannot be null. */
    private final String str;

    private Sql(String sql) {
        str = sql;
    }

    public static Sql of(CharSequence... _tokens) {
        return _tokens == null ? EMPTY_SQL : of(Arrays.asList(_tokens));
    }

    public static Sql of(Iterable<? extends CharSequence> _tokens) {
        return _tokens == null ? EMPTY_SQL : new Sql(format(_tokens));
    }

    /**
     * Formats an sql statement from multiple tokens.<br>
     * Leading and trailing whitespace is removed from each token and empty tokens ignored.<br>
     * The tokens are joined using a single blank character to create the sql string.<br>
     * Finally, any trailing semicolons are removed from the resulting sql.
     *
     * @param _tokens collection of tokens
     * @return formatted sql string
     */
    static String format(Iterable<? extends CharSequence> _tokens) {
        String sql = StreamSupport.stream(Objects.requireNonNull(_tokens).spliterator(), false)
            .filter(Objects::nonNull)
            .map(CharSequence::toString)
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.joining(" "));
        while (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql;
    }

    @Override
    public int length() {
        return str.length();
    }

    @Override
    public char charAt(int _index) {
        return str.charAt(_index);
    }

    @Override
    public CharSequence subSequence(int _start, int _end) {
        return str.subSequence(_start, _end);
    }

    @Override
    public int hashCode() {
        return str.hashCode();
    }

    @Override
    public boolean equals(Object _obj) {
        if (_obj == null || getClass() != _obj.getClass()) {
            return false;
        }
        return str.equals(((Sql) _obj).str);
    }

    @Override
    public String toString() {
        return str;
    }

}
