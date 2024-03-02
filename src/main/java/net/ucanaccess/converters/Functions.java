package net.ucanaccess.converters;

import static net.ucanaccess.converters.RegionalSettings.getRegionalSettings;

import io.github.spannm.jackcess.DataType;
import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.exception.InvalidFunctionParameterException;
import net.ucanaccess.exception.InvalidIntervalValueException;
import net.ucanaccess.exception.UcanaccessRuntimeException;
import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.ext.FunctionType;
import net.ucanaccess.util.Try;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Functions {
    private static Double       rnd;
    private static Double       lastRnd;
    private static final double APPROX = 0.00000001;

    private Functions() {
    }

    static SimpleDateFormat createSimpleDateFormat(String _pt) {
        SimpleDateFormat sdf = new SimpleDateFormat(_pt);
        ((GregorianCalendar) sdf.getCalendar()).setGregorianChange(new Date(Long.MIN_VALUE));
        return sdf;
    }

    /**
     * Returns an Integer representing the character code corresponding to the first letter in a string.
     *
     * @param _s any valid string expression
     * @return character code
     */
    @FunctionType(functionName = "Asc", argumentTypes = {AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer asc(String _s) {
        return _s == null || _s.isEmpty() ? null : (int) _s.charAt(0);
    }

    @FunctionType(functionName = "Equals", argumentTypes = {AccessType.COMPLEX, AccessType.COMPLEX}, returnType = AccessType.YESNO)
    public static Boolean equals(Object _obj1, Object _obj2) {
        if (_obj1 == null || _obj2 == null || !_obj1.getClass().equals(_obj2.getClass())) {
            return false;
        }
        if (_obj1.getClass().isArray()) {
            return Arrays.equals((Object[]) _obj1, (Object[]) _obj2);
        }
        return _obj1.equals(_obj2);
    }

    @FunctionType(functionName = "EqualsIgnoreOrder", argumentTypes = {AccessType.COMPLEX, AccessType.COMPLEX}, returnType = AccessType.YESNO)
    public static Boolean equalsIgnoreOrder(Object _obj1, Object _obj2) {
        if (_obj1 == null || _obj2 == null || !_obj1.getClass().equals(_obj2.getClass())) {
            return false;
        }
        if (_obj1.getClass().isArray()) {
            List<Object> lo1 = Arrays.asList((Object[]) _obj1);
            List<Object> lo2 = Arrays.asList((Object[]) _obj2);
            return lo1.containsAll(lo2) && lo2.containsAll(lo1);
        }
        return _obj1.equals(_obj2);
    }

    @FunctionType(functionName = "Contains", argumentTypes = {AccessType.COMPLEX, AccessType.COMPLEX}, returnType = AccessType.YESNO)
    public static Boolean contains(Object _obj1, Object _obj2) {
        if (_obj1 == null || _obj2 == null || !_obj1.getClass().isArray()) {
            return false;
        }
        List<Object> arr1 = Arrays.asList((Object[]) _obj1);
        List<Object> arr2 = _obj2.getClass().isArray() ? Arrays.asList((Object[]) _obj2) : Arrays.asList(_obj2);
        return arr1.containsAll(arr2);
    }

    /**
     * Returns a double specifying the arctangent of a number.
     *
     * @param _number a double or any valid numeric expression.
     * @return arctangent
     */
    @FunctionType(functionName = "Atn", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double atn(double _number) {
        return Math.atan(_number);
    }

    /**
     * Returns a double specifying the square root of a number.
     *
     * @param _number a double greater than or equal to zero
     * @return square root
     */
    @FunctionType(functionName = "Sqr", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double sqr(double _number) {
        return Math.sqrt(_number);
    }

    @FunctionType(functionName = "CBool", argumentTypes = {AccessType.NUMERIC}, returnType = AccessType.YESNO)
    public static boolean cbool(BigDecimal _value) {
        return cbool((Object) _value);
    }

    /**
     * Converts a value to a boolean.
     *
     * @param _value boolean input
     * @return boolean
     */
    @FunctionType(functionName = "CBool", argumentTypes = {AccessType.YESNO}, returnType = AccessType.YESNO)
    public static boolean cbool(Boolean _value) {
        return cbool((Object) _value);
    }

    @FunctionType(functionName = "CBool", argumentTypes = {AccessType.MEMO}, returnType = AccessType.YESNO)
    public static boolean cbool(String _value) {
        return cbool((Object) _value);
    }

    private static boolean cbool(Object _obj) {
        if (_obj == null) {
            return false;
        } else if (_obj instanceof Boolean) {
            return (Boolean) _obj;
        } else if (_obj instanceof String) {
            return Boolean.parseBoolean((String) _obj);
        } else {
            return _obj instanceof Number && ((Number) _obj).intValue() != 0;
        }
    }

    /**
     * Converts an expression into a currency value.
     */
    @FunctionType(functionName = "CCur", argumentTypes = {AccessType.CURRENCY}, returnType = AccessType.CURRENCY)
    public static BigDecimal ccur(BigDecimal _value) {
        return _value.setScale(4, RoundingMode.HALF_UP);
    }

    /**
     * Converts an expression into a date value.
     */
    @FunctionType(functionName = "CDate", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp cdate(String _dt) {
        return dateValue(_dt, false);
    }

    /**
     * Converts an expression to a double.
     */
    @FunctionType(functionName = "CDbl", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double cdbl(Double _number) {
        return _number;
    }

    @FunctionType(functionName = "CDec", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double cdec(Double _number) {
        return _number;
    }

    @FunctionType(functionName = "CInt", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.INTEGER)
    public static Short cint(Double _number) {
        return new BigDecimal((long) Math.floor(_number + 0.499999999999999d)).shortValueExact();
    }

    @FunctionType(functionName = "CInt", argumentTypes = {AccessType.YESNO}, returnType = AccessType.INTEGER)
    public static Short cint(boolean _boolean) {
        return (short) (_boolean ? -1 : 0);
    }

    /**
     * Converts an expression to a long integer.
     */
    @FunctionType(functionName = "CLng", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.LONG)
    public static Integer clng(Double _value) {
        return (int) Math.floor(_value + 0.5d);
    }

    @FunctionType(functionName = "CLng", argumentTypes = {AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer clng(String _value) throws UcanaccessSQLException {
        return Try.catching(() -> clng(FormatCache.getNoArgs().parse(_value).doubleValue()))
            .orThrow(UcanaccessSQLException::new);
    }

    @FunctionType(functionName = "CLng", argumentTypes = {AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer clng(Integer _value) {
        return _value;
    }

    @FunctionType(functionName = "CLong", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.LONG)
    public static Integer clong(Double _value) {
        return clng(_value);
    }

    @FunctionType(functionName = "CLong", argumentTypes = {AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer clong(Integer _value) {
        return _value;
    }

    @FunctionType(functionName = "CLong", argumentTypes = {AccessType.YESNO}, returnType = AccessType.LONG)
    public static Integer clong(boolean _value) {
        return _value ? -1 : 0;
    }

    // TODO
    @FunctionType(functionName = "CSign", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.SINGLE)
    public static double csign(double _value) {
        MathContext mc = new MathContext(7);
        return new BigDecimal(Double.toString(_value), mc).doubleValue();
    }

    /**
     * Converts a value to a string.
     */
    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.YESNO}, returnType = AccessType.MEMO)
    public static String cstr(Boolean _value) throws UcanaccessSQLException {
        return cstr((Object) _value);
    }

    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.TEXT}, returnType = AccessType.MEMO)
    public static String cstr(String _value) {
        return _value;
    }

    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String cstr(double _value) throws UcanaccessSQLException {
        return cstr((Object) _value);
    }

    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.LONG}, returnType = AccessType.MEMO)
    public static String cstr(int _value) throws UcanaccessSQLException {
        return cstr((Object) _value);
    }

    @FunctionType(functionName = "CStr", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.MEMO)
    public static String cstr(Timestamp _value) throws UcanaccessSQLException {
        return _value == null ? null : format(_value, "general date");
    }

    private static String cstr(Object _value) throws UcanaccessSQLException {
        return _value == null ? null : format(_value.toString(), "", true);
    }

    @FunctionType(functionName = "CVar", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String cvar(Double _value) throws UcanaccessSQLException {
        return format(_value, "general number");
    }

    /**
     * Returns a date containing a date to which a specified time interval has been added.
     *
     * @param _intv interval of time
     * @param _vl number of intervals to add (to get dates in the future) or dedcut (to get dates in the past)
     * @param _dt date to which the interval is added
     * @return calculated date
     * @throws UcanaccessSQLException on invalid date interval input
     */
    @FunctionType(namingConflict = true, functionName = "DateAdd",
        argumentTypes = {AccessType.MEMO, AccessType.LONG, AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Date dateAdd(String _intv, int _vl, Date _dt) throws UcanaccessSQLException {
        if (_dt == null || _intv == null) {
            return null;
        }
        Calendar cl = Calendar.getInstance();
        cl.setTime(_dt);
        if ("yyyy".equalsIgnoreCase(_intv)) {
            cl.add(Calendar.YEAR, _vl);
        } else if ("q".equalsIgnoreCase(_intv)) { // quarter
            cl.add(Calendar.MONTH, _vl * 3);
        } else if ("m".equalsIgnoreCase(_intv)) { // month
            cl.add(Calendar.MONTH, _vl);
        } else if ("y".equalsIgnoreCase(_intv)) { // day of year
            cl.add(Calendar.DAY_OF_YEAR, _vl);
        } else if ("d".equalsIgnoreCase(_intv)) { // day
            cl.add(Calendar.DAY_OF_YEAR, _vl);
        } else if ("w".equalsIgnoreCase(_intv)) { // weekday
            cl.add(Calendar.DAY_OF_WEEK, _vl);
        } else if ("ww".equalsIgnoreCase(_intv)) { // week
            cl.add(Calendar.WEEK_OF_YEAR, _vl);
        } else if ("h".equalsIgnoreCase(_intv)) { // hour
            cl.add(Calendar.HOUR, _vl);
        } else if ("n".equalsIgnoreCase(_intv)) { // minute
            cl.add(Calendar.MINUTE, _vl);
        } else if ("s".equalsIgnoreCase(_intv)) { // second
            cl.add(Calendar.SECOND, _vl);
        } else {
            throw new InvalidIntervalValueException(_intv);
        }
        return _dt instanceof Timestamp
            ? new Timestamp(cl.getTimeInMillis())
            : new java.sql.Date(cl.getTimeInMillis());
    }

    @FunctionType(namingConflict = true, functionName = "DateAdd", argumentTypes = {AccessType.MEMO, AccessType.LONG, AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp dateAdd(String _intv, int _vl, Timestamp _dt) throws UcanaccessSQLException {
        return (Timestamp) dateAdd(_intv, _vl, (Date) _dt);
    }

    @FunctionType(namingConflict = true, functionName = "DateAdd", argumentTypes = {AccessType.MEMO, AccessType.LONG, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp dateAdd(String _intv, int _vl, String _dt) throws UcanaccessSQLException {
        return (Timestamp) dateAdd(_intv, _vl, (Date) dateValue(_dt, false));
    }

    @FunctionType(namingConflict = true, functionName = "DateDiff", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer dateDiff(String _intv, String _dt1, String _dt2) throws UcanaccessSQLException {
        return dateDiff(_intv, dateValue(_dt1, false), dateValue(_dt2, false));
    }

    @FunctionType(namingConflict = true, functionName = "DateDiff", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.DATETIME}, returnType = AccessType.LONG)
    public static Integer dateDiff(String _intv, String _dt1, Timestamp _dt2) throws UcanaccessSQLException {
        return dateDiff(_intv, dateValue(_dt1, false), _dt2);
    }

    @FunctionType(namingConflict = true, functionName = "DateDiff", argumentTypes = {AccessType.MEMO, AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer dateDiff(String _intv, Timestamp _dt1, String _dt2) throws UcanaccessSQLException {
        return dateDiff(_intv, _dt1, dateValue(_dt2, false));
    }

    @FunctionType(namingConflict = true, functionName = "DateDiff", argumentTypes = {AccessType.MEMO, AccessType.DATETIME, AccessType.DATETIME}, returnType = AccessType.LONG)
    public static Integer dateDiff(String _intv, Timestamp _dt1, Timestamp _dt2) throws UcanaccessSQLException {
        if (_dt1 == null || _intv == null || _dt2 == null) {
            return null;
        }
        Calendar clMin = Calendar.getInstance();
        Calendar clMax = Calendar.getInstance();
        int sign = _dt1.after(_dt2) ? -1 : 1;
        if (sign == 1) {
            clMax.setTime(_dt2);
            clMin.setTime(_dt1);
        } else {
            clMax.setTime(_dt1);
            clMin.setTime(_dt2);
        }
        clMin.set(Calendar.MILLISECOND, 0);
        clMax.set(Calendar.MILLISECOND, 0);
        Integer result;
        if ("yyyy".equalsIgnoreCase(_intv)) {
            result = clMax.get(Calendar.YEAR) - clMin.get(Calendar.YEAR);
        } else if ("q".equalsIgnoreCase(_intv)) {
            result = dateDiff("yyyy", _dt1, _dt2) * 4 + (clMax.get(Calendar.MONTH) - clMin.get(Calendar.MONTH)) / 3;
        } else if ("y".equalsIgnoreCase(_intv) || "d".equalsIgnoreCase(_intv)) {
            result = (int) Math.rint((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / (1000 * 60 * 60 * 24));
        } else if ("m".equalsIgnoreCase(_intv)) {
            result = dateDiff("yyyy", _dt1, _dt2) * 12 + clMax.get(Calendar.MONTH) - clMin.get(Calendar.MONTH);
        } else if ("w".equalsIgnoreCase(_intv) || "ww".equalsIgnoreCase(_intv)) {
            result = (int) Math.floor((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / (1000 * 60 * 60 * 24 * 7));
        } else if ("h".equalsIgnoreCase(_intv)) {
            result = (int) Math.round((clMax.getTime().getTime() - clMin.getTime().getTime()) / (1000d * 60 * 60));
        } else if ("n".equalsIgnoreCase(_intv)) {
            result = (int) Math.rint((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / (1000 * 60));
        } else if ("s".equalsIgnoreCase(_intv)) {
            result = (int) Math.rint((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / 1000);
        } else {
            throw new InvalidIntervalValueException(_intv);
        }
        return result * sign;
    }

    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, String _dt, Integer _firstDayOfWeek) throws UcanaccessSQLException {
        return datePart(_intv, dateValue(_dt, false), _firstDayOfWeek);
    }

    /**
     * Returns an integer containing the specified part of a given date.
     *
     * @param _interval interval of time you want to return
     * @param _date value that you want to evaluate
     * @param _firstDayOfWeek constant that specifies the first day of the week
     * @return date part
     * @throws UcanaccessSQLException on invalid date interval input
     */
    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.DATETIME, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer datePart(String _interval, Timestamp _date, Integer _firstDayOfWeek) throws UcanaccessSQLException {
        Integer ret = "ww".equalsIgnoreCase(_interval)
            ? datePart(_interval, _date, _firstDayOfWeek, 1)
            : datePart(_interval, _date);
        if ("w".equalsIgnoreCase(_interval) && _firstDayOfWeek > 1) {
            Calendar cl = Calendar.getInstance();
            cl.setTime(_date);
            ret = cl.get(Calendar.DAY_OF_WEEK) - _firstDayOfWeek + 1;
            if (ret <= 0) {
                ret = 7 + ret;
            }
        }
        return ret;
    }

    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, String _dt, Integer _firstDayOfWeek, Integer _firstWeekOfYear)
        throws UcanaccessSQLException {
        return datePart(_intv, dateValue(_dt, false), _firstDayOfWeek, _firstWeekOfYear);
    }

    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.DATETIME, AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, Timestamp _dt, Integer _firstDayOfWeek, Integer _firstWeekOfYear) throws UcanaccessSQLException {
        Integer ret = datePart(_intv, _dt);
        if (ret != null && "ww".equalsIgnoreCase(_intv) && (_firstWeekOfYear > 1 || _firstDayOfWeek > 1)) {
            Calendar cl = Calendar.getInstance();
            cl.setTime(_dt);
            cl.set(Calendar.MONTH, Calendar.JANUARY);
            cl.set(Calendar.DAY_OF_MONTH, 1);
            Calendar cl1 = Calendar.getInstance();
            cl1.setTime(_dt);
            if (_firstDayOfWeek == 0) {
                _firstDayOfWeek = 1;
            }
            int dow = cl.get(Calendar.DAY_OF_WEEK) - _firstDayOfWeek + 1;
            if (dow <= 0) {
                dow = 7 + dow;
                if (cl1.get(Calendar.DAY_OF_WEEK) - _firstDayOfWeek >= 0) {
                    ret++;
                }
            }
            if (dow > 4 && _firstWeekOfYear == 2) {
                ret--;
            }
            if (dow > 1 && _firstWeekOfYear == 3) {
                ret--;
            }
        }
        return ret;
    }

    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, String _dt) throws UcanaccessSQLException {
        return datePart(_intv, dateValue(_dt, false));
    }

    @FunctionType(namingConflict = true, functionName = "DatePart", argumentTypes = {AccessType.MEMO, AccessType.DATETIME}, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, Timestamp _dt) throws UcanaccessSQLException {
        if (_intv == null || _dt == null) {
            return null;
        }
        Calendar cl = Calendar.getInstance(Locale.US);
        cl.setTime(_dt);
        if ("yyyy".equalsIgnoreCase(_intv)) {
            return cl.get(Calendar.YEAR);
        } else if ("q".equalsIgnoreCase(_intv)) {
            return (int) Math.ceil((cl.get(Calendar.MONTH) + 1) / 3d);
        } else if ("d".equalsIgnoreCase(_intv)) {
            return cl.get(Calendar.DAY_OF_MONTH);
        } else if ("y".equalsIgnoreCase(_intv)) {
            return cl.get(Calendar.DAY_OF_YEAR);
        } else if ("m".equalsIgnoreCase(_intv)) {
            return cl.get(Calendar.MONTH) + 1;
        } else if ("ww".equalsIgnoreCase(_intv)) {
            return cl.get(Calendar.WEEK_OF_YEAR);
        } else if ("w".equalsIgnoreCase(_intv)) {
            return cl.get(Calendar.DAY_OF_WEEK);
        } else if ("h".equalsIgnoreCase(_intv)) {
            return cl.get(Calendar.HOUR_OF_DAY);
        } else if ("n".equalsIgnoreCase(_intv)) {
            return cl.get(Calendar.MINUTE);
        } else if ("s".equalsIgnoreCase(_intv)) {
            return cl.get(Calendar.SECOND);
        } else {
            throw new InvalidIntervalValueException(_intv);
        }
    }

    /**
     * Returns a date for a specified year, month, and day.
     *
     * @param _year number between 100 and 9999, inclusive, or a numeric expression
     * @param _month any numeric expression
     * @param _day any numeric expression
     * @return date
     */
    @FunctionType(functionName = "DateSerial", argumentTypes = {AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.DATETIME)
    public static Timestamp dateSerial(int _year, int _month, int _day) {
        Calendar cl = Calendar.getInstance();
        cl.setLenient(true);
        cl.set(Calendar.YEAR, _year);
        cl.set(Calendar.MONTH, _month - 1);
        cl.set(Calendar.DAY_OF_MONTH, _day);
        cl.set(Calendar.HOUR_OF_DAY, 0);
        cl.set(Calendar.MINUTE, 0);
        cl.set(Calendar.SECOND, 0);
        cl.set(Calendar.MILLISECOND, 0);
        return new Timestamp(cl.getTime().getTime());
    }

    @FunctionType(functionName = "DateValue", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp dateValue(String _dt) {
        return dateValue(_dt, true);
    }

    @FunctionType(functionName = "Timestamp0", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp timestamp0(String _dt) {
        GregorianCalendar gc = new GregorianCalendar();
        gc.setGregorianChange(new Date(Long.MIN_VALUE));
        Pattern ptdate = Pattern.compile(SQLConverter.DATE_FORMAT + "\\s");
        Pattern pth = Pattern.compile(SQLConverter.HHMMSS_FORMAT);
        Matcher mtc = ptdate.matcher(_dt);
        if (mtc.find()) {
            gc.set(Integer.parseInt(mtc.group(1)), Integer.parseInt(mtc.group(2)) - 1, Integer.parseInt(mtc.group(3)));
        } else {
            UcanaccessRuntimeException.throwNow("internal error in parsing timestamp");
        }
        mtc = pth.matcher(_dt);
        if (mtc.find()) {
            gc.set(Calendar.HOUR_OF_DAY, Integer.parseInt(mtc.group(1)));
            gc.set(Calendar.MINUTE, Integer.parseInt(mtc.group(2)));
            gc.set(Calendar.SECOND, Integer.parseInt(mtc.group(3)));
        } else {
            UcanaccessRuntimeException.throwNow("internal error in parsing timestamp");
        }
        gc.set(Calendar.MILLISECOND, 0);
        return new Timestamp(gc.getTime().getTime());
    }

    private static Timestamp dateValue(String _dt, boolean _onlyDate) {
        RegionalSettings reg = getRegionalSettings();
        if (!"true".equalsIgnoreCase(reg.getRS())
            && (!"PM".equalsIgnoreCase(reg.getPM()) || !"AM".equalsIgnoreCase(reg.getAM()))) {
            _dt = _dt.replaceAll("(?i)" + Pattern.quote(reg.getPM()), "PM")
                .replaceAll("(?i)" + Pattern.quote(reg.getAM()), "AM");
        }

        for (Entry<SimpleDateFormat, Boolean> entry : reg.getDateFormats().entrySet()) {
            SimpleDateFormat sdf = entry.getKey();
            boolean yearOverride = entry.getValue();

            try {
                Timestamp t = new Timestamp(sdf.parse(_dt).getTime());
                if (_onlyDate) {
                    t = dateValue(t);
                }
                if (yearOverride) {
                    Calendar cl = Calendar.getInstance();
                    int y = cl.get(Calendar.YEAR);
                    cl.setTime(t);
                    cl.set(Calendar.YEAR, y);
                    t = new Timestamp(cl.getTime().getTime());
                }
                return t;
            } catch (ParseException _ignored) {}
        }
        return null;
    }

    /**
     * Returns a date based on a string. If the given string does not include a year component, this function will use the current year.
     */
    @FunctionType(functionName = "DateValue", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp dateValue(Timestamp _dt) {
        Calendar cl = Calendar.getInstance();
        cl.setTime(_dt);
        cl.set(Calendar.HOUR_OF_DAY, 0);
        cl.set(Calendar.MINUTE, 0);
        cl.set(Calendar.SECOND, 0);
        cl.set(Calendar.MILLISECOND, 0);
        return new Timestamp(cl.getTime().getTime());
    }

    @FunctionType(functionName = "Format", argumentTypes = {AccessType.DOUBLE, AccessType.TEXT}, returnType = AccessType.TEXT)
    public static String format(Double _d, String _par) throws UcanaccessSQLException {
        if (_d == null) {
            return "";
        } else if ("percent".equalsIgnoreCase(_par)) {
            return FormatCache.getZpzz().format(_d * 100) + "%";
        } else if ("fixed".equalsIgnoreCase(_par)) {
            return FormatCache.getZpzz().format(_d);
        } else if ("standard".equalsIgnoreCase(_par)) {
            return FormatCache.getSharp().format(_d);
        } else if ("general number".equalsIgnoreCase(_par)) {
            return FormatCache.getNoGrouping().format(_d);
        } else if ("currency".equalsIgnoreCase(_par)) {
            return FormatCache.getCurrencyDefault().format(_d);
        } else if ("yes/no".equalsIgnoreCase(_par)) {
            return _d == 0 ? "No" : "Yes";
        } else if ("true/false".equalsIgnoreCase(_par)) {
            return _d == 0 ? "False" : "True";
        } else if ("On/Off".equalsIgnoreCase(_par)) {
            return _d == 0 ? "Off" : "On";
        } else if ("Scientific".equalsIgnoreCase(_par)) {
            return String.format("%6.2E", _d);
        }
        return Try.catching(() -> FormatCache.getDecimalFormat(_par).format(_d))
            .orThrow(UcanaccessSQLException::new);
    }

    @FunctionType(functionName = "Format", argumentTypes = {AccessType.TEXT, AccessType.TEXT}, returnType = AccessType.TEXT)
    public static String format(String _s, String _par) throws UcanaccessSQLException {
        if (_s == null) {
            return "";
        }
        return format(_s, _par, false);
    }

    public static String format(String _s, String _par, boolean _incl) throws UcanaccessSQLException {
        if (isNumeric(_s)) {
            if (_incl) {
                return format(Double.parseDouble(_s), _par);
            }
            return Try.catching(() -> format(FormatCache.getNoArgs().parse(_s).doubleValue(), _par))
                .orThrow(UcanaccessSQLException::new);
        } else if (isDate(_s)) {
            return format(dateValue(_s, false), _par);
        }
        return _s;
    }

    private static String formatDate(Timestamp _t, String _pattern) {
        RegionalSettings reg = getRegionalSettings();
        SimpleDateFormat sdf = createSimpleDateFormat(_pattern);
        String ret = sdf.format(_t);
        if (!reg.getRS().equalsIgnoreCase("true")) {
            if (!reg.getAM().equals("AM")) {
                ret = ret.replace("AM", reg.getAM());
            }
            if (!reg.getPM().equals("PM")) {
                ret = ret.replace("PM", reg.getPM());
            }
        } else {
            ret = ret.replace(reg.getPM(), "PM");
            ret = ret.replace(reg.getAM(), "AM");
        }
        return ret;

    }

    @FunctionType(functionName = "Format", argumentTypes = {AccessType.DATETIME, AccessType.TEXT}, returnType = AccessType.TEXT)
    public static String format(Timestamp _t, String _par) throws UcanaccessSQLException {
        if (_t == null) {
            return "";
        }
        RegionalSettings reg = getRegionalSettings();

        if ("long date".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getLongDatePattern());
        } else if ("medium date".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getMediumDatePattern());
        } else if ("short date".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getShortDatePattern());
        } else if ("general date".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getGeneralPattern());
        } else if ("long time".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getLongTimePattern());
        } else if ("medium time".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getMediumTimePattern());
        } else if ("short time".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getShortTimePattern());
        } else if ("q".equalsIgnoreCase(_par)) {
            return String.valueOf(datePart(_par, _t));
        }
        return createSimpleDateFormat(_par
            .replace("m", "M")
            .replace("n", "m")
            .replace("(?i)AM/PM|A/P|AMPM", "a")
            .replace("dddd", "EEEE")).format(_t);
    }

    /**
     * Returns one of two parts, depending on the evaluation of an expression.
     */
    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String iif(Boolean _b, String _o, String _o1) {
        return (String) iif(_b, _o, (Object) _o1);
    }

    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer iif(Boolean b, Integer o, Integer o1) {
        return (Integer) iif(b, o, (Object) o1);
    }

    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double iif(Boolean b, Double o, Double o1) {
        return (Double) iif(b, o, (Object) o1);
    }

    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.YESNO, AccessType.YESNO}, returnType = AccessType.YESNO)
    public static Boolean iif(Boolean b, Boolean o, Boolean o1) {

        return (Boolean) iif(b, o, (Object) o1);
    }

    @FunctionType(functionName = "IIf", argumentTypes = {AccessType.YESNO, AccessType.DATETIME, AccessType.DATETIME}, returnType = AccessType.DATETIME)
    public static Timestamp iif(Boolean b, Timestamp o, Timestamp o1) {
        return (Timestamp) iif(b, o, (Object) o1);
    }

    private static Object iif(Boolean _b, Object _o1, Object _o2) {
        return Objects.requireNonNullElse(_b, Boolean.FALSE) ? _o1 : _o2;
    }

    /**
     * Returns the position of the first occurrence of a string in another string.
     */
    @FunctionType(namingConflict = true, functionName = "InStr", argumentTypes = {AccessType.LONG, AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer instr(Integer _start, String _text, String _search) {
        return instr(_start, _text, _search, -1);
    }

    @FunctionType(namingConflict = true, functionName = "InStr", argumentTypes = {AccessType.LONG, AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer instr(Integer start, String text, String search, Integer compare) {
        start--;
        if (compare != 0) {
            text = text.toLowerCase();
        }
        if (text.length() <= start) {
            return 0;
        } else {
            text = text.substring(start);
        }
        return text.indexOf(search) + start + 1;
    }

    @FunctionType(namingConflict = true, functionName = "InStr", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer instr(String text, String search) {
        return instr(1, text, search, -1);
    }

    @FunctionType(namingConflict = true, functionName = "InStr", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer instr(String text, String search, Integer compare) {
        return instr(1, text, search, compare);
    }

    /**
     * Returns the position of the first occurrence of a string in another string, starting from the end of the string.
     */
    @FunctionType(functionName = "InStrRev", argumentTypes = {AccessType.TEXT, AccessType.TEXT}, returnType = AccessType.LONG)
    public static Integer instrrev(String text, String search) {
        return instrrev(text, search, -1, -1);
    }

    @FunctionType(functionName = "InStrRev", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer instrrev(String text, String search, Integer start) {
        return instrrev(text, search, start, -1);
    }

    @FunctionType(functionName = "InStrRev", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer instrrev(String text, String search, Integer start, Integer compare) {
        if (compare != 0) {
            text = text.toLowerCase();
        }
        if (text.length() <= start) {
            return 0;
        } else {
            if (start > 0) {
                text = text.substring(0, start);
            }
            return text.lastIndexOf(search) + 1;
        }
    }

    /**
     * Returns {@code true} if the expression is a valid date, otherwise {@code false}.
     */
    @FunctionType(functionName = "IsDate", argumentTypes = {AccessType.MEMO}, returnType = AccessType.YESNO)
    public static boolean isDate(String _value) {
        return dateValue(_value) != null;
    }

    @FunctionType(functionName = "IsDate", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.YESNO)
    public static boolean isDate(Timestamp _value) {
        return true;
    }

    /**
     * Returns {@code true} if the expression is a {@code null} value, otherwise {@code false}.
     */
    @FunctionType(namingConflict = true, functionName = "IsNull", argumentTypes = {AccessType.MEMO}, returnType = AccessType.YESNO)
    public static boolean isNull(String _value) {
        return _value == null;
    }

    @FunctionType(namingConflict = true, functionName = "IsNull", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.YESNO)
    public static boolean isNull(Timestamp _value) {
        return _value == null;
    }

    @FunctionType(namingConflict = true, functionName = "IsNull", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.YESNO)
    public static boolean isNull(Double _value) {
        return _value == null;
    }

    /**
     * Returns {@code true} if the expression is a valid number, otherwise {@code false}.
     */
    @FunctionType(functionName = "IsNumeric", argumentTypes = {AccessType.NUMERIC}, returnType = AccessType.YESNO)
    public static boolean isNumeric(BigDecimal b) {
        return true;
    }

    @FunctionType(functionName = "IsNumeric", argumentTypes = {AccessType.MEMO}, returnType = AccessType.YESNO)
    public static boolean isNumeric(String _s) {
        return Try.catching(() -> {
            Currency cr = Currency.getInstance(Locale.getDefault());
            if (_s.startsWith(cr.getSymbol())) {
                return isNumeric(_s.substring(cr.getSymbol().length()));
            }
            if (_s.startsWith("+") || _s.startsWith("-")) {
                return isNumeric(_s.substring(1));
            }
            DecimalFormatSymbols dfs = DecimalFormatSymbols.getInstance();
            String sep = dfs.getDecimalSeparator() + "";
            String gs = dfs.getGroupingSeparator() + "";
            if (_s.startsWith(gs)) {
                return false;
            }
            if (_s.startsWith(sep)) {
                return isNumeric(_s.substring(1));
            }

            String s;
            if (".".equals(sep)) {
                s = _s.replaceAll(gs, "");
            } else {
                s = _s.replace(".", "")
                    .replace(sep, ".");
            }
            new BigDecimal(s);
            return true;
        }).orElse(false);
    }

    /**
     * Extracts a substring from a string, starting from the left-most character.
     */
    @FunctionType(functionName = "Left", namingConflict = true, argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String left(String _input, int _i) {
        if (_input == null || _i < 0) {
            return null;
        } else if (_i >= _input.length()) {
            return _input;
        } else {
            return _input.substring(0, _i);
        }
    }

    @FunctionType(functionName = "\"LEFT$\"", argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String leftS(String input, int i) {
        return left(input, i);
    }

    /**
     * Returns the length of the specified string.
     */
    @FunctionType(functionName = "Len", argumentTypes = {AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer len(String _value) {
        return _value == null ? null : _value.length();
    }

    /**
     * Extracts a substring from a string (starting at any position).
     */
    @FunctionType(functionName = "Mid", argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String mid(String _value, int start) {
        return mid(_value, start, _value.length());
    }

    @FunctionType(functionName = "Mid", argumentTypes = {AccessType.MEMO, AccessType.LONG, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String mid(String _value, int start, int length) {
        if (_value == null) {
            return null;
        }
        int len = start - 1 + length;
        if (start < 1) {
            UcanaccessRuntimeException.throwNow("Invalid function call");
        }
        if (len > _value.length()) {
            len = _value.length();
        }
        return _value.substring(start - 1, len);
    }

    /**
     * Returns a string representing the month given a number from 1 to 12.
     */
    @FunctionType(namingConflict = true, functionName = "MonthName", argumentTypes = {AccessType.LONG}, returnType = AccessType.TEXT)
    public static String monthName(int _number) throws UcanaccessSQLException {
        return monthName(_number, false);
    }

    @FunctionType(namingConflict = true, functionName = "MonthName", argumentTypes = {AccessType.LONG, AccessType.YESNO}, returnType = AccessType.TEXT)
    public static String monthName(int _number, boolean _abbreviate) throws UcanaccessSQLException {
        if (_number >= 1 && _number <= 12) {
            DateFormatSymbols dfs = new DateFormatSymbols();
            return _abbreviate ? dfs.getShortMonths()[_number - 1] : dfs.getMonths()[_number - 1];
        }
        throw new UcanaccessSQLException("Invalid month number");
    }

    /**
     * Returns the current system date.
     */
    @FunctionType(functionName = "Date", argumentTypes = {}, returnType = AccessType.DATETIME)
    public static Timestamp date() {
        return Timestamp.valueOf(LocalDate.now().atStartOfDay());
    }

    /**
     * Returns the current system date and time.
     */
    @FunctionType(namingConflict = true, functionName = "Now", argumentTypes = {}, returnType = AccessType.DATETIME)
    public static Timestamp now() {
        return new Timestamp(System.currentTimeMillis() / 1000 * 1000);
    }

    /**
     * Returns the second argument if the first argument is {@code null}.
     */
    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String nz(String _value) {
        return _value == null ? "" : _value;
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double nz(Double _value) {
        return _value == null ? 0 : _value;
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer nz(Integer _value) {
        return _value == null ? 0 : _value;
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.NUMERIC}, returnType = AccessType.NUMERIC)
    public static BigDecimal nz(BigDecimal _value) {
        return _value == null ? BigDecimal.ZERO : _value;
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String nz(String _value, String _other) {
        return (String) nz(_value, (Object) _other);
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.NUMERIC, AccessType.NUMERIC}, returnType = AccessType.NUMERIC)
    public static BigDecimal nz(BigDecimal value, BigDecimal _other) {
        return (BigDecimal) nz(value, (Object) _other);
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double nz(Double value, Double _other) {
        return (Double) nz(value, (Object) _other);
    }

    @FunctionType(functionName = "Nz", argumentTypes = {AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer nz(Integer value, Integer _other) {
        return (Integer) nz(value, (Object) _other);
    }

    private static Object nz(Object value, Object _other) {
        return value == null ? _other : value;
    }

    /**
     * Returns the sign of a number. If number &gt; 0, it returns 1. If number = 0, it returns 0.
     */
    @FunctionType(functionName = "Sgn", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.INTEGER)
    public static short sgn(double _n) {
        return (short) (_n == 0 ? 0 : _n > 0 ? 1 : -1);
    }

    @FunctionType(functionName = "Sign", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.INTEGER)
    public static short sign(double _n) {
        return sgn(_n);
    }

    /**
     * Returns a string with a specified number of spaces.
     */
    @FunctionType(functionName = "Space", argumentTypes = {AccessType.LONG}, returnType = AccessType.MEMO)
    public static String space(Integer _nr) {
        return " ".repeat(Math.max(0, Objects.requireNonNullElse(_nr, 0)));
    }

    /**
     * Returns a string representation of a number.
     */
    @FunctionType(functionName = "Str", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.TEXT)
    public static String str(double d) {
        String pre = d > 0 ? " " : "";
        return Math.round(d) == d ? pre + Math.round(d) : pre + d;
    }

    /**
     * Returns the current system time.
     */
    @FunctionType(functionName = "Time", argumentTypes = {}, returnType = AccessType.DATETIME)
    public static Timestamp time() {
        return new Timestamp(LocalDateTime.now()
            .withYear(1899).withMonth(12).withDayOfMonth(30)
            .truncatedTo(ChronoUnit.SECONDS)
            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    /**
     * Accepts a string as input and returns the numbers found in that string.
     */
    @FunctionType(functionName = "Val", argumentTypes = {AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double val(String _val1) {
        return val((Object) _val1);
    }

    @FunctionType(functionName = "Val", argumentTypes = {AccessType.NUMERIC}, returnType = AccessType.DOUBLE)
    public static Double val(BigDecimal _val1) {
        return val((Object) _val1);
    }

    private static Double val(Object _val1) {
        if (_val1 == null) {
            return null;
        }
        String val = _val1.toString().trim();
        int lp = val.lastIndexOf('.');
        char[] ca = val.toCharArray();
        StringBuilder sb = new StringBuilder();
        int minLength = 1;
        for (int i = 0; i < ca.length; i++) {
            char c = ca[i];
            if ((c == '-' || c == '+') && i == 0) {
                minLength++;
                sb.append(c);
            } else if (c == ' ') {
                continue;
            } else if (Character.isDigit(c)) {
                sb.append(c);
            } else if (c == '.' && i == lp) {
                sb.append(c);
                if (i == 0 || i == 1 && minLength == 2) {
                    minLength++;
                }
            } else {
                break;
            }
        }
        if (sb.length() < minLength) {
            return 0.0d;
        } else {
            return Double.parseDouble(sb.toString());
        }
    }

    /**
     * Returns a string representing the day of the week given a number from 1 to 7.
     */
    @FunctionType(functionName = "WeekdayName", argumentTypes = {AccessType.LONG}, returnType = AccessType.TEXT)
    public static String weekdayName(int _number) {
        return weekdayName(_number, false);
    }

    @FunctionType(functionName = "WeekdayName", argumentTypes = {AccessType.LONG, AccessType.YESNO}, returnType = AccessType.TEXT)
    public static String weekdayName(int _number, boolean _abbreviate) {
        int vbSunday = 1;
        return weekdayName(_number, _abbreviate, vbSunday);
    }

    @FunctionType(functionName = "WeekdayName", argumentTypes = {AccessType.LONG, AccessType.YESNO, AccessType.LONG}, returnType = AccessType.TEXT)
    public static String weekdayName(int _number, boolean _abbreviate, int _firstDayOfWeek) {
        // DayOfWeek starts with Monday, WeekdayName with Sunday (default)
        int firstDayOfWeek = Math.min(Math.max(1, _firstDayOfWeek), 7);
        int offset = firstDayOfWeek == 1 ? -1
            : firstDayOfWeek == 1 ? -1
                : firstDayOfWeek - 2;
        int number = _number;
        while (number > 7) {
            number -= 7;
        }
        return DayOfWeek.of(number).plus(offset)
            .getDisplayName(_abbreviate ? TextStyle.SHORT : TextStyle.FULL, Locale.getDefault());
    }

    public static void main(String[] args) {
        System.out.println("WeekDayName(3): " + weekdayName(3));
    }

    /**
     * Returns a number representing the day of the week (a number from 1 to 7) given a date value.
     */
    @FunctionType(functionName = "WeekDay", argumentTypes = {AccessType.DATETIME}, returnType = AccessType.LONG)
    public static Integer weekDay(Timestamp _date) throws UcanaccessSQLException {
        return datePart("w", _date);
    }

    @FunctionType(functionName = "WeekDay", argumentTypes = {AccessType.DATETIME, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer weekDay(Timestamp _date, Integer _firstDayOfWeek) throws UcanaccessSQLException {
        return datePart("w", _date, _firstDayOfWeek);
    }

    @FunctionType(functionName = "String", argumentTypes = {AccessType.LONG, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String string(Integer _nr, String _str) {
        if (_str == null) {
            return null;
        }
        String ret = "";
        for (int i = 0; i < _nr; i++) {
            ret += _str.charAt(0);
        }
        return ret;
    }

    /**
     * Returns a time given an hour, minute, and second value.
     */
    @FunctionType(functionName = "TimeSerial", argumentTypes = {AccessType.LONG, AccessType.LONG, AccessType.LONG}, returnType = AccessType.DATETIME)
    public static Timestamp timeSerial(Integer _h, Integer _m, Integer _s) {
        return new Timestamp(LocalDateTime.now()
            .withYear(1899).withMonth(12).withDayOfMonth(30)
            .truncatedTo(ChronoUnit.SECONDS)
            .withHour(_h).withMinute(_m).withSecond(_s)
            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    /**
     *  Generates a random number (integer value).
     */
    @FunctionType(functionName = "Rnd", argumentTypes = {}, returnType = AccessType.DOUBLE)
    public static Double rnd() {
        return rnd(null);
    }

    @FunctionType(functionName = "Rnd", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double rnd(Double _d) {
        if (_d == null || _d > 0) {
            lastRnd = Math.random();
            return lastRnd;
        }
        if (_d < 0) {
            if (rnd == null) {
                rnd = _d;
            }
            return rnd;
        }
        if (_d == 0) {
            if (lastRnd == null) {
                lastRnd = Math.random();
            }
            return lastRnd;
        }
        return null;
    }

    /**
     * Returns a string whose characters are in reverse order.
     */
    @FunctionType(functionName = "StrReverse", argumentTypes = {AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String strReverse(String _value) {
        return _value == null ? null : new StringBuilder(_value).reverse().toString();
    }

    /**
     * Returns a string converted as specified.
     */
    @FunctionType(functionName = "StrConv", argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String strConv(String _value, int _conversion) {
        if (_value == null) {
            return null;
        } else if (_conversion == 1) { // vbUpperCase
            return _value.toUpperCase();
        } else if (_conversion == 2) { // vbLowerCase
            return _value.toLowerCase();
        } else if (_conversion == 3) { // vbProperCase: not implemented
            return _value;
        }
        return _value;
    }

    /**
     * Returns an integer value representing the result of a string comparison.
     */
    @FunctionType(functionName = "StrComp", argumentTypes = {AccessType.MEMO, AccessType.MEMO, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer strComp(String _value1, String _value2, Integer _type) throws UcanaccessSQLException {
        switch (_type) {
            case 0:
            case -1:
            case 2:
                return _value1.compareTo(_value2);
            case 1:
                return _value1.toUpperCase().compareTo(_value2.toUpperCase());
            default:
                throw new InvalidFunctionParameterException("StrComp", _type);
        }
    }

    @FunctionType(functionName = "StrComp", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.LONG)
    public static Integer strComp(String _value1, String _value2) throws UcanaccessSQLException {
        return strComp(_value1, _value2, 0);
    }

    /**
     * Returns the integer portion of a number.
     */
    @FunctionType(functionName = "Int", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.LONG)
    public static Integer mint(Double _value) {
        return new BigDecimal((long) Math.floor(_value)).intValueExact();
    }

    @FunctionType(functionName = "Int", argumentTypes = {AccessType.YESNO}, returnType = AccessType.INTEGER)
    public static Short mint(boolean _value) {
        return (short) (_value ? -1 : 0);
    }

    @FunctionType(functionName = "DDB", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ddb(double cost, double salvage, double life, double period) {
        return ddb(cost, salvage, life, period, 2d);
    }

    @FunctionType(functionName = "DDB", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ddb(double cost, double salvage, double life, double period, double factor) {
        if (cost < 0 || life == 2d && period > 1d) {
            return 0;
        }
        if (life < 2d || life == 2d && period <= 1d) {
            return cost - salvage;
        }
        if (period <= 1d) {
            return Math.min(cost * factor / life, cost - salvage);
        }
        double retk = Math.max(salvage - cost * Math.pow((life - factor) / life, period), 0);

        return Math.max(factor * cost / life * Math.pow((life - factor) / life, period - 1d) - retk, 0);
    }

    @FunctionType(functionName = "FV", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double fv(double rate, int periods, double payment) {
        return fv(rate, periods, payment, 0, 0);
    }

    @FunctionType(functionName = "FV", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double fv(double rate, int periods, double payment, double pv) {
        return fv(rate, periods, payment, pv, 0);
    }

    @FunctionType(functionName = "FV", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double fv(double rate, int periods, double payment, double pv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;
        double fv = pv * Math.pow(1 + rate, periods);
        for (int i = 0; i < periods; i++) {
            fv += payment * Math.pow(1 + rate, i + type);
        }
        return -fv;
    }

    @FunctionType(functionName = "PMT", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pmt(double rate, double periods, double pv) {
        return pmt(rate, periods, pv, 0, 0);
    }

    @FunctionType(functionName = "PMT", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pmt(double rate, double periods, double pv, double fv) {
        return pmt(rate, periods, pv, 0, 0);
    }

    @FunctionType(functionName = "PMT", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pmt(double rate, double periods, double pv, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;

        if (rate == 0) {
            return -1 * (fv + pv) / periods;
        } else {
            return (fv + pv * Math.pow(1 + rate, periods)) * rate
                / ((type == 1 ? 1 + rate : 1) * (1 - Math.pow(1 + rate, periods)));
        }

    }

    @FunctionType(functionName = "NPER", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double nper(double rate, double pmt, double pv) {
        return nper(rate, pmt, pv, 0, 0);
    }

    @FunctionType(functionName = "NPER", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double nper(double rate, double pmt, double pv, double fv) {

        return nper(rate, pmt, pv, fv, 0);
    }

    @FunctionType(functionName = "NPER", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double nper(double rate, double pmt, double pv, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;
        double nper = 0;
        if (rate == 0) {
            nper = -1 * (fv + pv) / pmt;
        } else {

            double cr = (type == 1 ? 1 + rate : 1) * pmt / rate;
            double val1 = cr - fv < 0 ? Math.log(fv - cr) : Math.log(cr - fv);
            double val2 = cr - fv < 0 ? Math.log(-pv - cr) : Math.log(pv + cr);
            double val3 = Math.log(1 + rate);
            nper = (val1 - val2) / val3;
        }
        return nper;
    }

    @FunctionType(functionName = "IPMT", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ipmt(double rate, double per, double nper, double pv) {
        return ipmt(rate, per, nper, pv, 0, 0);
    }

    @FunctionType(functionName = "IPMT", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ipmt(double rate, double per, double nper, double pv, double fv) {
        return ipmt(rate, per, nper, pv, fv, 0);
    }

    @FunctionType(functionName = "IPMT",
        argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ipmt(double rate, double per, double nper, double pv, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;
        double ipmt = fv(rate, (int) per - 1, pmt(rate, nper, pv, fv, type), pv, type) * rate;
        if (type == 1) {
            ipmt = ipmt / (1 + rate);
        }
        return ipmt;
    }

    @FunctionType(functionName = "PV", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pv(double rate, double nper, double pmt) {
        return pv(rate, nper, pmt, 0, 0);

    }

    @FunctionType(functionName = "PV", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pv(double rate, double nper, double pmt, double fv) {
        return pv(rate, nper, pmt, fv, 0);

    }

    @FunctionType(functionName = "PV", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double pv(double rate, double nper, double pmt, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;

        if (rate == 0) {
            return -1 * (nper * pmt + fv);
        } else {

            return ((1 - Math.pow(1 + rate, nper)) / rate * (type == 1 ? 1 + rate : 1) * pmt - fv)
                / Math.pow(1 + rate, nper);
        }

    }

    @FunctionType(functionName = "PPMT", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ppmt(double rate, int per, int nper, double pv) {
        return ppmt(rate, per, nper, pv, 0, 0);
    }

    @FunctionType(functionName = "PPMT", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ppmt(double rate, int per, int nper, double pv, double fv) {
        return ppmt(rate, per, nper, pv, fv, 0);
    }

    @FunctionType(functionName = "PPMT", argumentTypes = {AccessType.DOUBLE, AccessType.LONG, AccessType.LONG, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double ppmt(double rate, int per, int nper, double pv, double fv, double type) {
        return pmt(rate, nper, pv, fv, type) - ipmt(rate, per, nper, pv, fv, type);
    }

    @FunctionType(functionName = "SLN", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double sln(double cost, double salvage, double life) {
        return (cost - salvage) / life;
    }

    @FunctionType(functionName = "SYD", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double syd(double cost, double salvage, double life, double per) {
        return (cost - salvage) * (life - per + 1) * 2 / (life * (life + 1));
    }

    @FunctionType(functionName = "RATE", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv) {
        return rate(nper, pmt, pv, 0, 0, 0.1);
    }

    @FunctionType(functionName = "RATE", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv, double fv) {
        return rate(nper, pmt, pv, fv, 0, 0.1);
    }

    @FunctionType(functionName = "RATE", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv, double fv, double type) {
        return rate(nper, pmt, pv, fv, type, 0.1);
    }

    @FunctionType(functionName = "RATE",
        argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv, double fv, double type, double guess) {
        // FROM MS http://office.microsoft.com/en-us/excel-help/rate-HP005209232.aspx

        type = Math.abs(type) >= 1 ? 1 : 0; // the only change to the implementation Apache POI
        final int financialMaxIterations = 20; // Bet accuracy with 128
        final double financialPrecision = 0.0000001; // 1.0e-8

        double y = 0;
        double y0 = 0;
        double y1 = 0;
        double x0 = 0;
        double f = 0;
        double i = 0;
        double rate = guess;
        if (Math.abs(rate) < financialPrecision) {
            y = pv * (1 + nper * rate) + pmt * (1 + rate * type) * nper + fv;
        } else {
            f = Math.exp(nper * Math.log(1 + rate));
            y = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;
        }
        y0 = pv + pmt * nper + fv;
        y1 = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;

        // find root by Newton secant method
        i = 0.0;
        x0 = 0.0;
        double x1 = rate;
        while (Math.abs(y0 - y1) > financialPrecision && i < financialMaxIterations) {
            rate = (y1 * x0 - y0 * x1) / (y1 - y0);
            x0 = x1;
            x1 = rate;

            if (Math.abs(rate) < financialPrecision) {
                y = pv * (1 + nper * rate) + pmt * (1 + rate * type) * nper + fv;
            } else {
                f = Math.exp(nper * Math.log(1 + rate));
                y = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;
            }

            y0 = y1;
            y1 = y;
            ++i;
        }

        return rate;
    }

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = {AccessType.DOUBLE, AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(Double res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = {AccessType.YESNO, AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(Boolean res, String datatype) {
        if (res == null) {
            return null;
        }
        return res ? -1d : 0d;
    }

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(String _res, String _datatype) {
        if (_res == null) {
            return null;
        }
        return Try.catching(() -> {
            DecimalFormatSymbols dfs = DecimalFormatSymbols.getInstance();
            String sep = dfs.getDecimalSeparator() + "";
            String gs = dfs.getGroupingSeparator() + "";
            String res = _res.replaceAll(Pattern.quote(gs), "");
            if (!".".equalsIgnoreCase(sep)) {
                res = res.replaceAll(Pattern.quote(sep), ".");
            }
            double d = val(res);
            DataType dt = DataType.valueOf(_datatype);
            if (dt.equals(DataType.BYTE) || dt.equals(DataType.INT) || dt.equals(DataType.LONG)) {
                d = Math.rint(d + APPROX);
            }
            return d;
        }).orIgnore();
    }

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = {AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(Timestamp res, String datatype) throws UcanaccessSQLException {
        if (res == null) {
            return null;
        }
        Calendar clbb = Calendar.getInstance();
        clbb.set(1899, 11, 30, 0, 0, 0);
        return (double) dateDiff("y", new Timestamp(clbb.getTimeInMillis()), res);
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.YESNO, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Boolean res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.DOUBLE, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Double res, String datatype) {
        if (res == null) {
            return null;
        }
        return res != 0d;
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Timestamp res, String datatype) {
        return null;

    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(String res, String datatype) {
        if (res == null) {
            return null;
        } else if ("-1".equals(res)) {
            return true;
        } else if ("0".equals(res)) {
            return false;
        }
        return null;

    }

    @FunctionType(functionName = "formulaToText", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String formulaToText(String res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToText", argumentTypes = {AccessType.DOUBLE, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String formulaToText(Double res, String datatype) {
        if (res == null) {
            return null;
        }
        DecimalFormatSymbols dfs = DecimalFormatSymbols.getInstance();
        DecimalFormat df = new DecimalFormat("#", dfs);
        df.setGroupingUsed(false);
        df.setMaximumFractionDigits(100);
        return df.format(res);
    }

    @FunctionType(functionName = "formulaToText", argumentTypes = {AccessType.YESNO, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String formulaToText(Boolean res, String datatype) {
        if (res == null) {
            return null;
        }
        return res ? "-1" : "0";
    }

    @FunctionType(functionName = "formulaToText", argumentTypes = {AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String formulaToText(Timestamp res, String datatype) throws UcanaccessSQLException {
        Calendar cl = Calendar.getInstance();
        cl.setTimeInMillis(res.getTime());
        if (cl.get(Calendar.HOUR) == 0 && cl.get(Calendar.MINUTE) == 0 && cl.get(Calendar.SECOND) == 0) {
            return format(res, "short date");
        } else {
            return format(res, "general date");
        }
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = {AccessType.DATETIME, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(Timestamp res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = {AccessType.MEMO, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(String res, String datatype) {
        if (res == null) {
            return null;
        }
        return Try.catching(() -> dateValue(res, false)).orIgnore();
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = {AccessType.YESNO, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(Boolean res, String datatype) throws UcanaccessSQLException {
        if (res == null) {
            return null;
        }
        Calendar clbb = Calendar.getInstance();
        clbb.set(1899, 11, 30, 0, 0, 0);
        clbb.set(Calendar.MILLISECOND, 0);
        return dateAdd("y", res ? -1 : 0, new Timestamp(clbb.getTimeInMillis()));
    }

    @FunctionType(functionName = "orderJet", argumentTypes = {AccessType.MEMO}, returnType = AccessType.MEMO)
    public static String orderJet(String s) {
        return s.replaceAll("([a-zA-Z0-9])[\\-–—]([a-zA-Z0-9])", "$1$2");
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = {AccessType.DOUBLE, AccessType.MEMO}, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(Double res, String datatype) throws UcanaccessSQLException {
        if (res == null) {
            return null;
        }
        Calendar clbb = Calendar.getInstance();
        clbb.set(1899, 11, 30, 0, 0, 0);
        clbb.set(Calendar.MILLISECOND, 0);
        Double d = Math.floor(res);
        Timestamp tr = dateAdd("y", d.intValue(), new Timestamp(clbb.getTimeInMillis()));
        d = (res - res.intValue()) * 24;
        tr = dateAdd("H", d.intValue(), tr);
        d = (d - d.intValue()) * 60;
        tr = dateAdd("N", d.intValue(), tr);
        d = (d - d.intValue()) * 60;
        tr = dateAdd("S", (int) Math.rint(d + APPROX), tr);
        return tr;
    }

    /**
     * Extracts a substring from a string starting from the right-most character.
     */
    @FunctionType(functionName = "Right", namingConflict = true, argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String right(String input, int i) {
        if (input == null || i < 0) {
            return null;
        }
        int ln = input.length();
        if (i >= ln) {
            return input;
        } else {
            return input.substring(ln - i, ln);
        }
    }

    @FunctionType(functionName = "\"RIGHT$\"", argumentTypes = {AccessType.MEMO, AccessType.LONG}, returnType = AccessType.MEMO)
    public static String rightS(String input, int i) {
        return right(input, i);
    }

    /**
     * Returns a number rounded to a specified number of decimal places.
     */
    @FunctionType(namingConflict = true, functionName = "Round", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static double round(double d, double p) {
        double f = Math.pow(10d, p);
        return Math.round(d * f) / f;
    }

    /**
     * Returns the integer portion of a number.
     *
     * @param _number a double or any valid numeric expression, if {@code null}, {@code null} is returned
     * @return integer portion
     */
    @FunctionType(namingConflict = true, functionName = "Fix", argumentTypes = {AccessType.DOUBLE}, returnType = AccessType.DOUBLE)
    public static Double fix(Double _number) {
        return _number == null
            ? null
            : sign(_number) * (double) mint(Math.abs(_number));
    }

    @FunctionType(functionName = "PARTITION", argumentTypes = {AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE}, returnType = AccessType.MEMO)
    public static String partition(Double _number, double start, double stop, double interval) {
        if (_number == null) {
            return null;
        }
        _number = Math.rint(_number);
        interval = Math.rint(interval);

        String ul = String.valueOf(lrint(stop) + 1);
        stop = lrint(stop);
        start = lrint(start);
        int h = ul.length();
        if (_number < start) {
            return padLeft(-1, h) + ":" + padLeft(lrint(start) - 1, h);
        }
        if (_number > stop) {
            return ul + ":" + padLeft(-1, h);
        }

        for (double d = start; d <= stop; d += interval) {
            if (_number >= d && _number < d + interval) {
                return padLeft(lceil(d), h) + ":"
                    + padLeft(d + interval <= stop ? lfloor(d + interval) : lrint(stop), h);

            }
        }
        return "";
    }

    private static int lfloor(double _d) {
        return (int) Math.floor(_d - APPROX);
    }

    private static int lceil(double _d) {
        return (int) Math.ceil(_d - APPROX);
    }

    private static int lrint(double _d) {
        return (int) Math.rint(_d - APPROX);
    }

    private static String padLeft(int ext, int n) {
        String tp = ext > 0 ? String.valueOf(ext) : "";
        return String.format("%1$" + n + "s", tp);
    }

}
