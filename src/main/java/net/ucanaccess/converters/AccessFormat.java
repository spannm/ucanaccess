package net.ucanaccess.converters;

import static net.ucanaccess.converters.RegionalSettings.getRegionalSettings;

import net.ucanaccess.exception.UcanaccessSQLException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.DateFormatSymbols;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Formats values like the Access function Format, using the format language of VBA: named formats like "Standard" or
 * "Long Date", up to four sections separated by semicolons, literals in quotes or after a backslash, the number
 * placeholders 0 and #, the text placeholders @ and &amp; and the date tokens like dd, mmm, hh:nn or AM/PM.
 * <p>
 * Texts that come from the locale, like Yes and No or the names of months, are taken from Java and may differ from the
 * texts Access shows in some languages.
 *
 * @since 5.3.1
 */
final class AccessFormat {

    static final String                GENERAL_DATE = "general date";

    private static final LocalDateTime BASE_DATE    = LocalDateTime.of(1899, 12, 30, 0, 0);
    /** Prefix of literal text in token lists, so a literal like \# differs from the placeholder #. */
    private static final String        LITERAL      = "'";

    private AccessFormat() {
    }

    /**
     * Formats a value like the Access function Format.
     *
     * @param value           number, boolean, date, text or {@code null}
     * @param format          format expression, {@code null} or empty for the general format
     * @param firstDayOfWeek  first day of the week for the tokens w and ww, 1 for Sunday to 7 for Saturday, 0 for the
     *                        default locale
     * @param firstWeekOfYear first week of the year for the token ww, see DatePart
     * @return formatted text, never {@code null}
     * @throws UcanaccessSQLException if a week argument is out of range
     */
    static String format(Object value, String format, int firstDayOfWeek, int firstWeekOfYear) throws UcanaccessSQLException {
        Object val = value instanceof Boolean ? (Boolean) value ? -1d : 0d
            : value instanceof Number ? (Object) ((Number) value).doubleValue() : value;
        if (format == null || format.isEmpty()) {
            return val == null ? "" : general(val);
        }
        String named = formatNamed(val, format);
        if (named != null) {
            return named;
        }
        List<String> sections = splitSections(format);
        if (isTextFormat(sections.get(0))) {
            String text = val == null ? "" : val instanceof String ? (String) val : general(val);
            return text.isEmpty() && sections.size() > 1 ? formatText("", sections.get(1)) : formatText(text, sections.get(0));
        } else if (val == null) {
            return sections.size() > 3 ? formatNumber(0, sections.get(3)) : "";
        }
        boolean dateFormat = !isNumberFormat(sections) && hasDateTokens(sections.get(0));
        if (val instanceof String) {
            val = toNumberOrDate((String) val, dateFormat);
            if (val instanceof String) {
                return (String) val;
            }
        }
        if (val instanceof Timestamp) {
            return dateFormat ? formatDate((Timestamp) val, sections.get(0), firstDayOfWeek, firstWeekOfYear)
                : formatNumber(toSerial((Timestamp) val), sections);
        }
        double d = (Double) val;
        return dateFormat ? formatDate(Functions.cdate(d), sections.get(0), firstDayOfWeek, firstWeekOfYear)
            : formatNumber(d, sections);
    }

    /**
     * Converts a value to text without a format, like Format with one argument.
     *
     * @param value number, date or text
     * @return text
     * @throws UcanaccessSQLException never for valid values
     */
    private static String general(Object value) throws UcanaccessSQLException {
        if (value instanceof Double) {
            return generalNumber((Double) value);
        } else if (value instanceof Timestamp) {
            return formatNamedDate((Timestamp) value, GENERAL_DATE);
        }
        return value.toString();
    }

    /**
     * Converts a number to text like Access with up to 15 significant digits. A number of 1E15 or more that does not
     * fit into 15 digits is shown in scientific notation, as Access does, so 2^53 becomes 9.00719925474099E+15.
     *
     * @param d number
     * @return text with the decimal separator of the default locale
     */
    static String generalNumber(double d) {
        if (!Double.isFinite(d)) {
            return Double.toString(d);
        }
        BigDecimal exact = BigDecimal.valueOf(d);
        BigDecimal rounded = exact.round(new MathContext(15));
        if (Math.abs(d) < 1E15 || rounded.compareTo(exact) == 0) {
            return Functions.cstr(d);
        }
        int exponent = rounded.precision() - rounded.scale() - 1;
        String mantissa = rounded.movePointLeft(exponent).stripTrailingZeros().toPlainString();
        return String.format("%sE+%02d", mantissa.replace('.', decimalSeparator()), exponent);
    }

    /**
     * Formats a value with a named format like "Standard" or "Short Date".
     *
     * @param value  number, date, text or {@code null}
     * @param format format expression
     * @return formatted text, or {@code null} if the format is not a named one
     * @throws UcanaccessSQLException never for valid values
     */
    private static String formatNamed(Object value, String format) throws UcanaccessSQLException {
        String name = format.trim().toLowerCase(Locale.ROOT);
        boolean number = isNamedNumber(name);
        if (!number && !isNamedDate(name)) {
            return null;
        } else if (value == null) {
            return "";
        }
        Object val = value instanceof String ? toNumberOrDate((String) value, !number) : value;
        if (val instanceof String) {
            return (String) val;
        } else if (number) {
            return formatNamedNumber(val instanceof Timestamp ? toSerial((Timestamp) val) : (Double) val, name);
        }
        return formatNamedDate(val instanceof Double ? Functions.cdate((Double) val) : (Timestamp) val, name);
    }

    /**
     * Tells whether a name is a named number format.
     *
     * @param name format name in lower case
     * @return {@code true} for a named number format
     */
    private static boolean isNamedNumber(String name) {
        return List.of("general number", "currency", "fixed", "standard", "percent", "scientific", "yes/no",
            "true/false", "on/off").contains(name);
    }

    /**
     * Tells whether a name is a named date format.
     *
     * @param name format name in lower case
     * @return {@code true} for a named date format
     */
    private static boolean isNamedDate(String name) {
        return List.of(GENERAL_DATE, "long date", "medium date", "short date", "long time", "medium time", "short time")
            .contains(name);
    }

    /**
     * Formats a number with a named number format.
     *
     * @param d    number
     * @param name format name in lower case
     * @return formatted text
     */
    private static String formatNamedNumber(double d, String name) {
        switch (name) {
            case "general number":
                return generalNumber(d);
            case "currency":
                return NumberFormat.getCurrencyInstance().format(d).replace(' ', ' ');
            case "fixed":
                return formatNumber(d, List.of("0.00"));
            case "standard":
                return formatNumber(d, List.of("#,##0.00"));
            case "percent":
                return formatNumber(d, List.of("0.00%"));
            case "scientific":
                return formatNumber(d, List.of("0.00E+00"));
            case "yes/no":
                return d == 0 ? "No" : "Yes";
            case "true/false":
                return d == 0 ? "False" : "True";
            default:
                return d == 0 ? "Off" : "On";
        }
    }

    /**
     * Formats a date with a named date format, taking the patterns from the regional settings.
     *
     * @param t    date
     * @param name format name in lower case
     * @return formatted text
     */
    static String formatNamedDate(Timestamp t, String name) {
        RegionalSettings reg = getRegionalSettings();
        switch (name) {
            case "long date":
                return formatPattern(t, reg.getLongDatePattern());
            case "medium date":
                return formatPattern(t, reg.getMediumDatePattern());
            case "short date":
                return formatPattern(t, reg.getShortDatePattern());
            case "long time":
                return formatPattern(t, reg.getLongTimePattern());
            case "medium time":
                return formatPattern(t, reg.getMediumTimePattern());
            case "short time":
                return formatPattern(t, reg.getShortTimePattern());
            default:
                // like Access, leave out a midnight time and the base date
                LocalDateTime ldt = t.toLocalDateTime();
                if (ldt.toLocalTime().equals(LocalTime.MIDNIGHT)) {
                    return formatPattern(t, reg.getShortDatePattern());
                } else if (ldt.toLocalDate().equals(BASE_DATE.toLocalDate())) {
                    return formatPattern(t, reg.getLongTimePattern());
                }
                return formatPattern(t, reg.getGeneralPattern());
        }
    }

    /**
     * Formats a date with a Java date pattern and the AM and PM texts of the regional settings.
     *
     * @param t       date
     * @param pattern pattern of {@link SimpleDateFormat}
     * @return formatted text
     */
    private static String formatPattern(Timestamp t, String pattern) {
        RegionalSettings reg = getRegionalSettings();
        String ret = Functions.createSimpleDateFormat(pattern).format(t);
        if (!"true".equalsIgnoreCase(reg.getRS())) {
            if (!"AM".equals(reg.getAM())) {
                ret = ret.replace("AM", reg.getAM());
            }
            if (!"PM".equals(reg.getPM())) {
                ret = ret.replace("PM", reg.getPM());
            }
        } else {
            ret = ret.replace(reg.getPM(), "PM").replace(reg.getAM(), "AM");
        }
        return ret;
    }

    /**
     * Converts a text to a number or a date, as Format does before it applies a number or date format. A text like
     * 5.3.2026 can be both in some locales, so a date format tries the date first.
     *
     * @param s          text
     * @param preferDate whether to try the date first
     * @return number, date, or the text itself if it is neither
     */
    private static Object toNumberOrDate(String s, boolean preferDate) {
        if (s.isEmpty()) {
            return s;
        }
        Timestamp t = preferDate ? Functions.dateValue(s, false) : null;
        if (t != null) {
            return t;
        } else if (Functions.isNumeric(s)) {
            Number n = NumberFormat.getInstance().parse(s, new ParsePosition(0));
            if (n != null) {
                return n.doubleValue();
            }
        }
        t = preferDate ? null : Functions.dateValue(s, false);
        return t == null ? s : t;
    }

    /**
     * Converts a date to the Access date value, the days since 1899-12-30 with the time as fraction. As in Access, the
     * fraction of a date before 1899-12-30 counts in the negative direction.
     *
     * @param t date
     * @return date value
     */
    static double toSerial(Timestamp t) {
        LocalDateTime ldt = t.toLocalDateTime();
        long days = ChronoUnit.DAYS.between(BASE_DATE.toLocalDate(), ldt.toLocalDate());
        double time = ldt.toLocalTime().toNanoOfDay() / 86_400_000_000_000d;
        return days < 0 ? days - time : days + time;
    }

    /**
     * Splits a format expression into its sections at semicolons outside literals.
     *
     * @param format format expression
     * @return sections, at least one
     */
    private static List<String> splitSections(String format) {
        List<String> sections = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < format.length(); i++) {
            char c = format.charAt(i);
            if (c == ';') {
                sections.add(sb.toString());
                sb.setLength(0);
                continue;
            }
            sb.append(c);
            if (c == '\\' && i + 1 < format.length()) {
                sb.append(format.charAt(++i));
            } else if (c == '"') {
                int end = format.indexOf('"', i + 1);
                end = end < 0 ? format.length() - 1 : end;
                sb.append(format, i + 1, end + 1);
                i = end;
            }
        }
        sections.add(sb.toString());
        return sections;
    }

    /**
     * Returns the characters of a section that are not part of a literal, so placeholders and tokens can be searched.
     *
     * @param section format section
     * @return section without literals in quotes, escaped characters and color names in brackets
     */
    private static String withoutLiterals(String section) {
        return section.replaceAll("\"[^\"]*\"?|\\\\.?|\\[[^\\]]*]?", "");
    }

    /**
     * Tells whether a section is a text format: it has the placeholders @ or &amp;, or consists only of the flags
     * &lt;, &gt; and !.
     *
     * @param section format section
     * @return {@code true} for a text format
     */
    private static boolean isTextFormat(String section) {
        String s = withoutLiterals(section);
        return s.indexOf('@') >= 0 || s.indexOf('&') >= 0 || (!s.isEmpty() && s.matches("[<>!]+"));
    }

    /**
     * Tells whether a format is a number format, that is whether a section has a number placeholder or a percent sign.
     *
     * @param sections format sections
     * @return {@code true} for a number format
     */
    private static boolean isNumberFormat(List<String> sections) {
        return sections.stream().map(AccessFormat::withoutLiterals).anyMatch(s -> s.matches(".*[0#%].*"));
    }

    /**
     * Tells whether a section has a date token.
     *
     * @param section format section
     * @return {@code true} if the section has a date token
     */
    private static boolean hasDateTokens(String section) {
        return withoutLiterals(section).matches("(?is).*([cdwmqyhns]|ttttt|AM/PM|A/P|AMPM).*");
    }

    /**
     * Formats a number with the sections of a number format: the first for positive numbers, the second for negative
     * numbers and the third for zero; an empty or missing section falls back to the first one. A number that rounds to
     * zero is zero, so it gets the zero section and no minus sign.
     *
     * @param d        number
     * @param sections format sections
     * @return formatted text
     */
    private static String formatNumber(double d, List<String> sections) {
        String negative = sections.size() > 1 && !sections.get(1).isEmpty() ? sections.get(1) : null;
        String zero = sections.size() > 2 && !sections.get(2).isEmpty() ? sections.get(2) : null;
        String section = d < 0 && negative != null ? negative : sections.get(0);
        NumberSection ns = new NumberSection(section);
        if (ns.isZero(Math.abs(d))) {
            return zero == null ? ns.format(0) : formatNumber(0, zero);
        }
        String text = ns.format(Math.abs(d));
        return d < 0 && negative == null ? "-" + text : text;
    }

    /**
     * Formats a non-negative number with one section.
     *
     * @param d       number
     * @param section format section
     * @return formatted text
     */
    private static String formatNumber(double d, String section) {
        return new NumberSection(section).format(d);
    }

    /**
     * Formats a text with a text section: @ stands for a character or a space, &amp; for a character or nothing. The
     * placeholders are filled from the right, with ! from the left; &lt; and &gt; convert to lower or upper case.
     *
     * @param text    text
     * @param section format section
     * @return formatted text
     */
    private static String formatText(String text, String section) {
        List<String> tokens = new ArrayList<>();
        boolean leftToRight = false;
        String s = text;
        for (int i = 0; i < section.length(); i++) {
            char c = section.charAt(i);
            if (c == '@' || c == '&') {
                tokens.add(String.valueOf(c));
            } else if (c == '<') {
                s = text.toLowerCase(Locale.getDefault());
            } else if (c == '>') {
                s = text.toUpperCase(Locale.getDefault());
            } else if (c == '!') {
                leftToRight = true;
            } else {
                i = addLiteral(section, i, tokens);
            }
        }
        int placeholders = (int) tokens.stream().filter(t -> "@".equals(t) || "&".equals(t)).count();
        if (placeholders == 0) {
            return literals(tokens) + s;
        }
        // the characters for each placeholder; without ! the last placeholder takes the excess, with ! the excess is cut
        List<String> chars = new ArrayList<>();
        int len = s.length();
        for (int p = 0; p < placeholders; p++) {
            int index = leftToRight ? p + Math.max(0, len - placeholders) : p - Math.max(0, placeholders - len);
            if (index < 0 || index >= len) {
                chars.add(null);
            } else {
                chars.add(!leftToRight && p == placeholders - 1 ? s.substring(index) : s.substring(index, index + 1));
            }
        }
        StringBuilder sb = new StringBuilder();
        int p = 0;
        for (String t : tokens) {
            if ("@".equals(t) || "&".equals(t)) {
                String ch = chars.get(p++);
                sb.append(ch != null ? ch : "@".equals(t) ? " " : "");
            } else {
                sb.append(t.substring(1));
            }
        }
        return sb.toString();
    }

    /**
     * Adds the literal at a position of a section to a token list: text in quotes, a character after a backslash or
     * the character itself; color names in brackets are skipped.
     *
     * @param section format section
     * @param i       position of the literal
     * @param tokens  token list to add to
     * @return position of the last character of the literal
     */
    private static int addLiteral(String section, int i, List<String> tokens) {
        char c = section.charAt(i);
        if (c == '"') {
            int end = section.indexOf('"', i + 1);
            end = end < 0 ? section.length() : end;
            tokens.add(LITERAL + section.substring(i + 1, end));
            return end;
        } else if (c == '\\') {
            if (i + 1 < section.length()) {
                tokens.add(LITERAL + section.substring(i + 1, i + 2));
            }
            return i + 1;
        } else if (c == '[') {
            int end = section.indexOf(']', i);
            return end < 0 ? section.length() : end;
        }
        tokens.add(LITERAL + c);
        return i;
    }

    /**
     * Joins the literals of a token list and leaves out all other tokens.
     *
     * @param tokens tokens and literals
     * @return literal text
     */
    private static String literals(List<String> tokens) {
        return tokens.stream().filter(t -> t.startsWith(LITERAL)).map(t -> t.substring(1)).collect(Collectors.joining());
    }

    /**
     * Formats a date with a date section.
     *
     * @param t               date
     * @param section         format section
     * @param firstDayOfWeek  first day of the week, see {@link #format(Object, String, int, int)}
     * @param firstWeekOfYear first week of the year, see {@link #format(Object, String, int, int)}
     * @return formatted text
     * @throws UcanaccessSQLException if a week argument is out of range
     */
    private static String formatDate(Timestamp t, String section, int firstDayOfWeek, int firstWeekOfYear)
        throws UcanaccessSQLException {
        List<String> tokens = tokenizeDate(section);
        boolean twelveHours = tokens.stream().anyMatch(tk -> tk.matches("(?i)@(AM/PM|A/P|AMPM)"));
        LocalDateTime ldt = t.toLocalDateTime();
        DateFormatSymbols symbols = DateFormatSymbols.getInstance();
        StringBuilder sb = new StringBuilder();
        String previous = "";
        for (String tk : tokens) {
            if (!tk.startsWith("@")) {
                sb.append(tk.substring(1));
                continue;
            }
            String token = tk.substring(1);
            String lower = token.toLowerCase(Locale.ROOT);
            if (lower.startsWith("m") && lower.length() <= 2 && previous.startsWith("h")) {
                lower = lower.replace('m', 'n');
            }
            sb.append(formatDateToken(ldt, t, token, lower, twelveHours, symbols, firstDayOfWeek, firstWeekOfYear));
            previous = lower;
        }
        return sb.toString();
    }

    /**
     * Formats one date token.
     *
     * @param ldt             date
     * @param t               the same date as timestamp
     * @param token           token as written in the format
     * @param lower           token in lower case, with a minute written as m already turned into n
     * @param twelveHours     whether hours count from 1 to 12, as the format has AM/PM
     * @param symbols         names of months and days
     * @param firstDayOfWeek  first day of the week, see {@link #format(Object, String, int, int)}
     * @param firstWeekOfYear first week of the year, see {@link #format(Object, String, int, int)}
     * @return formatted token
     * @throws UcanaccessSQLException if a week argument is out of range
     */
    private static String formatDateToken(LocalDateTime ldt, Timestamp t, String token, String lower, boolean twelveHours,
        DateFormatSymbols symbols, int firstDayOfWeek, int firstWeekOfYear) throws UcanaccessSQLException {
        int hour = twelveHours ? (ldt.getHour() + 11) % 12 + 1 : ldt.getHour();
        RegionalSettings reg = getRegionalSettings();
        switch (lower) {
            case "c":
                return formatNamedDate(t, GENERAL_DATE);
            case "d":
                return String.valueOf(ldt.getDayOfMonth());
            case "dd":
                return String.format("%02d", ldt.getDayOfMonth());
            case "ddd":
                return symbols.getShortWeekdays()[ldt.getDayOfWeek().getValue() % 7 + 1].replace(".", "");
            case "dddd":
                return symbols.getWeekdays()[ldt.getDayOfWeek().getValue() % 7 + 1];
            case "ddddd":
                return formatPattern(t, reg.getShortDatePattern());
            case "dddddd":
                return formatPattern(t, reg.getLongDatePattern());
            case "w":
                return String.valueOf(weekday(ldt.toLocalDate(), firstDayOfWeek));
            case "ww":
                return String.valueOf(Functions.weekOfYear(ldt.toLocalDate(), firstDayOfWeek, firstWeekOfYear));
            case "m":
                return String.valueOf(ldt.getMonthValue());
            case "mm":
                return String.format("%02d", ldt.getMonthValue());
            case "mmm":
                return symbols.getShortMonths()[ldt.getMonthValue() - 1].replace(".", "");
            case "mmmm":
                return symbols.getMonths()[ldt.getMonthValue() - 1];
            case "q":
                return String.valueOf((ldt.getMonthValue() + 2) / 3);
            case "y":
                return String.valueOf(ldt.getDayOfYear());
            case "yy":
                return String.format("%02d", ldt.getYear() % 100);
            case "yyyy":
                return String.valueOf(ldt.getYear());
            case "h":
                return String.valueOf(hour);
            case "hh":
                return String.format("%02d", hour);
            case "n":
                return String.valueOf(ldt.getMinute());
            case "nn":
                return String.format("%02d", ldt.getMinute());
            case "s":
                return String.valueOf(ldt.getSecond());
            case "ss":
                return String.format("%02d", ldt.getSecond());
            case "ttttt":
                return formatPattern(t, reg.getLongTimePattern());
            case "ampm":
                return ldt.getHour() < 12 ? reg.getAM() : reg.getPM();
            default:
                // AM/PM or A/P in the case written in the format
                String[] parts = token.split("/");
                return ldt.getHour() < 12 ? parts[0] : parts[1];
        }
    }

    /**
     * Returns the day of the week from 1 to 7, counted from the given first day.
     *
     * @param date           date
     * @param firstDayOfWeek 1 for Sunday to 7 for Saturday, 0 for the default locale
     * @return day of the week
     */
    private static int weekday(LocalDate date, int firstDayOfWeek) {
        int first = firstDayOfWeek == 0 ? WeekFields.of(Locale.getDefault()).getFirstDayOfWeek().getValue() % 7 + 1
            : firstDayOfWeek;
        int sundayFirst = date.getDayOfWeek().getValue() % 7 + 1;
        return (sundayFirst - first + 7) % 7 + 1;
    }

    /**
     * Splits a date section into tokens. A token starts with @, followed by the token as written; a run of the same
     * letter longer than the longest token is split, so mmmmm is mmmm and m. Other entries are literal text, with / and
     * : replaced by the date and time separators of the locale.
     *
     * @param section format section
     * @return tokens and literals
     */
    private static List<String> tokenizeDate(String section) {
        List<String> tokens = new ArrayList<>();
        String dateSeparator = separator(getRegionalSettings().getShortDatePattern(), "/");
        String timeSeparator = separator(getRegionalSettings().getLongTimePattern(), ":");
        for (int i = 0; i < section.length(); i++) {
            char c = section.charAt(i);
            char lower = Character.toLowerCase(c);
            int max = maxTokenLength(lower);
            if (section.regionMatches(true, i, "AM/PM", 0, 5)) {
                tokens.add("@" + section.substring(i, i + 5));
                i += 4;
            } else if (section.regionMatches(true, i, "A/P", 0, 3)) {
                tokens.add("@" + section.substring(i, i + 3));
                i += 2;
            } else if (section.regionMatches(true, i, "AMPM", 0, 4)) {
                tokens.add("@" + section.substring(i, i + 4));
                i += 3;
            } else if (lower == 't') {
                int run = runLength(section, i);
                tokens.add(run >= 5 ? "@ttttt" : LITERAL + section.substring(i, i + run));
                i += (run >= 5 ? 5 : run) - 1;
            } else if (max > 0) {
                int run = Math.min(runLength(section, i), max);
                if (lower == 'y' && run == 3) {
                    run = 2;
                }
                tokens.add("@" + section.substring(i, i + run));
                i += run - 1;
            } else if (c == '/') {
                tokens.add(LITERAL + dateSeparator);
            } else if (c == ':') {
                tokens.add(LITERAL + timeSeparator);
            } else {
                i = addLiteral(section, i, tokens);
            }
        }
        return tokens;
    }

    /**
     * Returns the longest token for a letter of a date format.
     *
     * @param lower letter in lower case
     * @return length of the longest token, 0 if the letter is no token
     */
    private static int maxTokenLength(char lower) {
        switch (lower) {
            case 'c':
            case 'q':
                return 1;
            case 'd':
                return 6;
            case 'm':
            case 'y':
                return 4;
            case 'w':
            case 'h':
            case 'n':
            case 's':
                return 2;
            default:
                return 0;
        }
    }

    /**
     * Counts how often the character at a position repeats, ignoring case.
     *
     * @param s text
     * @param i position
     * @return length of the run, at least 1
     */
    private static int runLength(String s, int i) {
        char c = Character.toLowerCase(s.charAt(i));
        int end = i + 1;
        while (end < s.length() && Character.toLowerCase(s.charAt(end)) == c) {
            end++;
        }
        return end - i;
    }

    /**
     * Returns the separator a Java date pattern uses, that is its first character that is no letter.
     *
     * @param pattern      pattern of {@link SimpleDateFormat}
     * @param defaultValue separator if the pattern has none
     * @return separator
     */
    private static String separator(String pattern, String defaultValue) {
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (!Character.isLetter(c) && c != '\'' && c != ' ') {
                return String.valueOf(c);
            }
        }
        return defaultValue;
    }

    /**
     * Returns the decimal separator of the default locale.
     *
     * @return decimal separator
     */
    private static char decimalSeparator() {
        return DecimalFormatSymbols.getInstance().getDecimalSeparator();
    }

    /**
     * One section of a number format, split into integer part, fraction and exponent.
     */
    private static final class NumberSection {
        private final List<String> integerTokens  = new ArrayList<>();
        private final List<String> fractionTokens = new ArrayList<>();
        private final List<String> exponentTokens = new ArrayList<>();

        private boolean            point;
        private boolean            grouping;
        private boolean            percent;
        private int                scale;
        private String             exponent;
        private int                fractionDigits;
        private int                integerDigits;

        /**
         * Parses a number section.
         *
         * @param section format section
         */
        NumberSection(String section) {
            List<String> part = integerTokens;
            for (int i = 0; i < section.length(); i++) {
                char c = section.charAt(i);
                boolean inInteger = !point && exponent == null;
                if (c == '0' || c == '#') {
                    part.add(String.valueOf(c));
                    if (inInteger) {
                        integerDigits++;
                    } else if (exponent == null) {
                        fractionDigits++;
                    }
                } else if (c == '.' && inInteger) {
                    point = true;
                    part = fractionTokens;
                } else if (c == ',') {
                    if (inInteger) {
                        part.add(",");
                    }
                } else if ((c == 'E' || c == 'e') && i + 1 < section.length() && "+-".indexOf(section.charAt(i + 1)) >= 0
                    && exponent == null) {
                    exponent = section.substring(i, i + 2);
                    part = exponentTokens;
                    i++;
                } else if (c == '%') {
                    percent = true;
                    part.add(LITERAL + "%");
                } else {
                    i = addLiteral(section, i, part);
                }
            }
            resolveCommas();
        }

        /**
         * Turns the commas of the integer part into grouping, if a placeholder comes before and after, or into
         * scaling by 1000, if they come after the last placeholder.
         */
        private void resolveCommas() {
            int last = -1;
            for (int i = 0; i < integerTokens.size(); i++) {
                if (isPlaceholder(integerTokens.get(i))) {
                    last = i;
                }
            }
            boolean seenPlaceholder = false;
            for (int i = 0; i < integerTokens.size(); i++) {
                String t = integerTokens.get(i);
                if (isPlaceholder(t)) {
                    seenPlaceholder = true;
                } else if (",".equals(t)) {
                    if (i > last) {
                        scale++;
                    } else if (seenPlaceholder) {
                        grouping = true;
                    }
                    integerTokens.remove(i--);
                    last--;
                }
            }
        }

        /**
         * Tells whether a token is a digit placeholder.
         *
         * @param t token
         * @return {@code true} for 0 or #
         */
        private static boolean isPlaceholder(String t) {
            return "0".equals(t) || "#".equals(t);
        }

        /**
         * Scales a non-negative number by the percent sign and the scaling commas and rounds it to 15 digits.
         *
         * @param d number
         * @return scaled number
         */
        private BigDecimal scaled(double d) {
            BigDecimal bd = BigDecimal.valueOf(d).round(new MathContext(15));
            if (percent) {
                bd = bd.movePointRight(2);
            }
            return bd.movePointLeft(3 * scale);
        }

        /**
         * Tells whether a non-negative number shows as zero in this section.
         *
         * @param d number
         * @return {@code true} if the number rounds to zero
         */
        boolean isZero(double d) {
            return exponent == null ? scaled(d).setScale(fractionDigits, RoundingMode.HALF_UP).signum() == 0 : d == 0;
        }

        /**
         * Formats a non-negative number.
         *
         * @param d number
         * @return formatted text
         */
        String format(double d) {
            if (integerDigits + fractionDigits == 0) {
                return literals(integerTokens) + literals(fractionTokens) + literals(exponentTokens);
            }
            BigDecimal bd = scaled(d);
            int exp = 0;
            if (exponent != null && bd.signum() != 0) {
                int digits = Math.max(1, integerDigits);
                exp = bd.precision() - bd.scale() - digits;
                BigDecimal mantissa = bd.movePointLeft(exp).setScale(fractionDigits, RoundingMode.HALF_UP);
                if (mantissa.precision() - mantissa.scale() > digits) {
                    exp++;
                }
                bd = bd.movePointLeft(exp);
            }
            bd = bd.setScale(fractionDigits, RoundingMode.HALF_UP);
            String plain = bd.toPlainString();
            String intDigits = plain.contains(".") ? plain.substring(0, plain.indexOf('.')) : plain;
            String fraction = plain.contains(".") ? plain.substring(plain.indexOf('.') + 1) : "";
            StringBuilder sb = new StringBuilder(formatInteger("0".equals(intDigits) ? "" : intDigits));
            if (point) {
                sb.append(decimalSeparator());
            }
            sb.append(formatFraction(fraction));
            if (exponent != null) {
                sb.append(formatExponent(exp));
            }
            return sb.toString();
        }

        /**
         * Distributes the integer digits over the placeholders from the right; excess digits go to the first one.
         *
         * @param digits integer digits without leading zeros, empty for zero
         * @return formatted integer part
         */
        private String formatInteger(String digits) {
            List<String> out = new ArrayList<>();
            List<Boolean> isDigit = new ArrayList<>();
            int di = digits.length() - 1;
            int first = -1;
            for (int i = 0; i < integerTokens.size(); i++) {
                if (isPlaceholder(integerTokens.get(i)) && first < 0) {
                    first = i;
                }
            }
            for (int i = integerTokens.size() - 1; i >= 0; i--) {
                String t = integerTokens.get(i);
                if (!isPlaceholder(t)) {
                    out.add(0, t.substring(1));
                    isDigit.add(0, false);
                    continue;
                }
                if (di >= 0) {
                    out.add(0, String.valueOf(digits.charAt(di--)));
                    isDigit.add(0, true);
                } else if ("0".equals(t)) {
                    out.add(0, "0");
                    isDigit.add(0, true);
                }
                if (i == first) {
                    while (di >= 0) {
                        out.add(0, String.valueOf(digits.charAt(di--)));
                        isDigit.add(0, true);
                    }
                }
            }
            if (first < 0) {
                out.add(digits);
                isDigit.add(false);
            }
            StringBuilder sb = new StringBuilder();
            long total = isDigit.stream().filter(b -> b).count();
            long remaining = total;
            char groupingSeparator = DecimalFormatSymbols.getInstance().getGroupingSeparator();
            for (int i = 0; i < out.size(); i++) {
                if (isDigit.get(i)) {
                    if (grouping && remaining < total && remaining % 3 == 0) {
                        sb.append(groupingSeparator);
                    }
                    remaining--;
                }
                sb.append(out.get(i));
            }
            return sb.toString();
        }

        /**
         * Puts the fraction digits into the placeholders; trailing zeros in # placeholders are left out.
         *
         * @param digits fraction digits, as many as placeholders
         * @return formatted fraction
         */
        private String formatFraction(String digits) {
            List<String> placeholders = fractionTokens.stream().filter(NumberSection::isPlaceholder).collect(Collectors.toList());
            int keep = digits.length();
            while (keep > 0 && "#".equals(placeholders.get(keep - 1)) && digits.charAt(keep - 1) == '0') {
                keep--;
            }
            StringBuilder sb = new StringBuilder();
            int di = 0;
            for (String t : fractionTokens) {
                if (isPlaceholder(t)) {
                    if (di < keep) {
                        sb.append(digits.charAt(di));
                    }
                    di++;
                } else {
                    sb.append(t.substring(1));
                }
            }
            return sb.toString();
        }

        /**
         * Formats the exponent: E+ shows the sign always, E- only for a negative exponent.
         *
         * @param exp exponent
         * @return formatted exponent with the literals after it
         */
        private String formatExponent(int exp) {
            int minDigits = (int) exponentTokens.stream().filter("0"::equals).count();
            StringBuilder sb = new StringBuilder().append(exponent.charAt(0));
            if (exp < 0 || exponent.charAt(1) == '+') {
                sb.append(exp < 0 ? '-' : '+');
            }
            String digits = String.valueOf(Math.abs(exp));
            if (exp == 0 && minDigits == 0) {
                digits = "";
            }
            sb.append("0".repeat(Math.max(0, minDigits - digits.length()))).append(digits);
            sb.append(literals(exponentTokens));
            return sb.toString();
        }
    }
}
