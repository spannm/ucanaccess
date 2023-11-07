package net.ucanaccess.converters;

import static net.ucanaccess.converters.RegionalSettings.getRegionalSettings;

import com.healthmarketscience.jackcess.DataType;
import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.ext.FunctionType;
import net.ucanaccess.jdbc.UcanaccessSQLException;
import net.ucanaccess.jdbc.UcanaccessSQLException.ExceptionMessages;
import net.ucanaccess.util.Try;
import net.ucanaccess.util.UcanaccessRuntimeException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.*;
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

    @FunctionType(functionName = "ASC", argumentTypes = { AccessType.MEMO }, returnType = AccessType.LONG)
    public static Integer asc(String _s) {
        if (_s == null || _s.length() == 0) {
            return null;
        }
        return (int) _s.charAt(0);
    }

    @FunctionType(functionName = "EQUALS", argumentTypes = { AccessType.COMPLEX,
            AccessType.COMPLEX }, returnType = AccessType.YESNO)
    public static Boolean equals(Object _obj1, Object _obj2) {
        if (_obj1 == null || _obj2 == null || !_obj1.getClass().equals(_obj2.getClass())) {
            return false;
        }
        if (_obj1.getClass().isArray()) {
            return Arrays.equals((Object[]) _obj1, (Object[]) _obj2);
        }
        return _obj1.equals(_obj2);
    }

    @FunctionType(functionName = "EQUALSIGNOREORDER", argumentTypes = { AccessType.COMPLEX,
            AccessType.COMPLEX }, returnType = AccessType.YESNO)
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

    @FunctionType(functionName = "CONTAINS", argumentTypes = { AccessType.COMPLEX,
            AccessType.COMPLEX }, returnType = AccessType.YESNO)
    public static Boolean contains(Object _obj1, Object _obj2) {
        if (_obj1 == null || _obj2 == null || !_obj1.getClass().isArray()) {
            return false;
        }
        List<Object> lo = Arrays.asList((Object[]) _obj1);
        List<Object> arg =
                _obj2.getClass().isArray() ? Arrays.asList((Object[]) _obj2) : Arrays.asList(_obj2);
        return lo.containsAll(arg);
    }

    @FunctionType(functionName = "ATN", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double atn(double _v) {
        return Math.atan(_v);
    }

    @FunctionType(functionName = "SQR", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double sqr(double _v) {
        return Math.sqrt(_v);
    }

    @FunctionType(functionName = "CBOOL", argumentTypes = { AccessType.NUMERIC }, returnType = AccessType.YESNO)
    public static boolean cbool(BigDecimal _value) {
        return cbool((Object) _value);
    }

    @FunctionType(functionName = "CBOOL", argumentTypes = { AccessType.YESNO }, returnType = AccessType.YESNO)
    public static boolean cbool(Boolean _value) {
        return cbool((Object) _value);
    }

    private static boolean cbool(Object _obj) {
        if (_obj instanceof Boolean) {
            return (Boolean) _obj;
        } else if (_obj instanceof String) {
            return Boolean.parseBoolean((String) _obj);
        } else {
            return _obj instanceof Number && ((Number) _obj).doubleValue() != 0;
        }
    }

    @FunctionType(functionName = "CBOOL", argumentTypes = { AccessType.MEMO }, returnType = AccessType.YESNO)
    public static boolean cbool(String _value) {
        return cbool((Object) _value);
    }

    @FunctionType(functionName = "CCUR", argumentTypes = { AccessType.CURRENCY }, returnType = AccessType.CURRENCY)
    public static BigDecimal ccur(BigDecimal value) {
        return value.setScale(4, RoundingMode.HALF_UP);
    }

    @FunctionType(functionName = "CDATE", argumentTypes = { AccessType.MEMO }, returnType = AccessType.DATETIME)
    public static Timestamp cdate(String _dt) {
        return dateValue(_dt, false);
    }

    @FunctionType(functionName = "CDBL", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static Double cdbl(Double _value) {
        return _value;
    }

    @FunctionType(functionName = "CDEC", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static Double cdec(Double _value) {
        return _value;
    }

    @FunctionType(functionName = "CINT", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.INTEGER)
    public static Short cint(Double _value) {
        return new BigDecimal((long) Math.floor(_value + 0.499999999999999d)).shortValueExact();
    }

    @FunctionType(functionName = "CINT", argumentTypes = { AccessType.YESNO }, returnType = AccessType.INTEGER)
    public static Short cint(boolean _value) {
        return (short) (_value ? -1 : 0);
    }

    @FunctionType(functionName = "CLONG", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.LONG)
    public static Integer clong(Double _value) {
        return clng(_value);
    }

    @FunctionType(functionName = "CLONG", argumentTypes = { AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer clong(Integer _value) {
        return _value;
    }

    @FunctionType(functionName = "CLNG", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.LONG)
    public static Integer clng(Double _value) {
        return (int) Math.floor(_value + 0.5d);
    }

    @FunctionType(functionName = "CLNG", argumentTypes = { AccessType.MEMO }, returnType = AccessType.LONG)
    public static Integer clng(String _value) throws UcanaccessSQLException {
        return Try.catching(() -> clng(FormatCache.getNoArgs().parse(_value).doubleValue()))
            .orThrow(UcanaccessSQLException::new);
    }

    @FunctionType(functionName = "CLNG", argumentTypes = { AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer clng(Integer _value) {
        return _value;
    }

    @FunctionType(functionName = "CLONG", argumentTypes = { AccessType.YESNO }, returnType = AccessType.LONG)
    public static Integer clong(boolean _value) {
        return _value ? -1 : 0;
    }

    @FunctionType(functionName = "CSIGN", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.SINGLE)
    public static double csign(double _value) {
        MathContext mc = new MathContext(7);
        return new BigDecimal(_value, mc).doubleValue();
    }

    @FunctionType(functionName = "CSTR", argumentTypes = { AccessType.YESNO }, returnType = AccessType.MEMO)
    public static String cstr(Boolean _value) throws UcanaccessSQLException {
        return cstr((Object) _value);
    }

    @FunctionType(functionName = "CSTR", argumentTypes = { AccessType.TEXT }, returnType = AccessType.MEMO)
    public static String cstr(String _value) {
        return _value;
    }

    @FunctionType(functionName = "CSTR", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.MEMO)
    public static String cstr(double _value) throws UcanaccessSQLException {
        return cstr((Object) _value);
    }

    @FunctionType(functionName = "CSTR", argumentTypes = { AccessType.LONG }, returnType = AccessType.MEMO)
    public static String cstr(int _value) throws UcanaccessSQLException {
        return cstr((Object) _value);
    }

    public static String cstr(Object _value) throws UcanaccessSQLException {
        return _value == null ? null : format(_value.toString(), "", true);
    }

    @FunctionType(functionName = "CSTR", argumentTypes = { AccessType.DATETIME }, returnType = AccessType.MEMO)
    public static String cstr(Timestamp _value) throws UcanaccessSQLException {
        return _value == null ? null : format(_value, "general date");
    }

    @FunctionType(functionName = "CVAR", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.MEMO)
    public static String cvar(Double _value) throws UcanaccessSQLException {
        return format(_value, "general number");
    }

    @FunctionType(namingConflict = true, functionName = "DATEADD", argumentTypes = { AccessType.MEMO, AccessType.LONG,
            AccessType.DATETIME }, returnType = AccessType.DATETIME)
    public static Date dateAdd(String _intv, int _vl, Date _dt) throws UcanaccessSQLException {
        if (_dt == null || _intv == null) {
            return null;
        }
        Calendar cl = Calendar.getInstance();
        cl.setTime(_dt);
        if ("yyyy".equalsIgnoreCase(_intv)) {
            cl.add(Calendar.YEAR, _vl);
        } else if ("q".equalsIgnoreCase(_intv)) {
            cl.add(Calendar.MONTH, _vl * 3);
        } else if ("y".equalsIgnoreCase(_intv) || "d".equalsIgnoreCase(_intv)) {
            cl.add(Calendar.DAY_OF_YEAR, _vl);
        } else if ("m".equalsIgnoreCase(_intv)) {
            cl.add(Calendar.MONTH, _vl);
        } else if ("w".equalsIgnoreCase(_intv)) {
            cl.add(Calendar.DAY_OF_WEEK, _vl);
        } else if ("ww".equalsIgnoreCase(_intv)) {
            cl.add(Calendar.WEEK_OF_YEAR, _vl);
        } else if ("h".equalsIgnoreCase(_intv)) {
            cl.add(Calendar.HOUR, _vl);
        } else if ("n".equalsIgnoreCase(_intv)) {
            cl.add(Calendar.MINUTE, _vl);
        } else if ("s".equalsIgnoreCase(_intv)) {
            cl.add(Calendar.SECOND, _vl);
        } else {
            throw new UcanaccessSQLException(ExceptionMessages.INVALID_INTERVAL_VALUE);
        }
        return _dt instanceof Timestamp ? new Timestamp(cl.getTimeInMillis())
                : new java.sql.Date(cl.getTimeInMillis());
    }

    @FunctionType(namingConflict = true, functionName = "DATEADD", argumentTypes = { AccessType.MEMO, AccessType.LONG,
            AccessType.DATETIME }, returnType = AccessType.DATETIME)
    public static Timestamp dateAdd(String _intv, int _vl, Timestamp _dt) throws UcanaccessSQLException {
        return (Timestamp) dateAdd(_intv, _vl, (Date) _dt);
    }

    @FunctionType(namingConflict = true, functionName = "DATEADD", argumentTypes = { AccessType.MEMO, AccessType.LONG,
            AccessType.MEMO }, returnType = AccessType.DATETIME)
    public static Timestamp dateAdd(String _intv, int _vl, String _dt) throws UcanaccessSQLException {
        return (Timestamp) dateAdd(_intv, _vl, (Date) dateValue(_dt, false));
    }

    @FunctionType(namingConflict = true, functionName = "DATEDIFF", argumentTypes = { AccessType.MEMO, AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.LONG)
    public static Integer dateDiff(String _intv, String _dt1, String _dt2) throws UcanaccessSQLException {
        return dateDiff(_intv, dateValue(_dt1, false), dateValue(_dt2, false));
    }

    @FunctionType(namingConflict = true, functionName = "DATEDIFF", argumentTypes = { AccessType.MEMO, AccessType.MEMO,
            AccessType.DATETIME }, returnType = AccessType.LONG)
    public static Integer dateDiff(String _intv, String _dt1, Timestamp _dt2) throws UcanaccessSQLException {
        return dateDiff(_intv, dateValue(_dt1, false), _dt2);
    }

    @FunctionType(namingConflict = true, functionName = "DATEDIFF", argumentTypes = { AccessType.MEMO,
            AccessType.DATETIME, AccessType.MEMO }, returnType = AccessType.LONG)
    public static Integer dateDiff(String _intv, Timestamp _dt1, String _dt2) throws UcanaccessSQLException {
        return dateDiff(_intv, _dt1, dateValue(_dt2, false));
    }

    @FunctionType(namingConflict = true, functionName = "DATEDIFF", argumentTypes = { AccessType.MEMO,
            AccessType.DATETIME, AccessType.DATETIME }, returnType = AccessType.LONG)
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
        if (_intv.equalsIgnoreCase("yyyy")) {
            result = clMax.get(Calendar.YEAR) - clMin.get(Calendar.YEAR);
        } else if (_intv.equalsIgnoreCase("q")) {
            result = dateDiff("yyyy", _dt1, _dt2) * 4 + (clMax.get(Calendar.MONTH) - clMin.get(Calendar.MONTH)) / 3;
        } else if ("y".equalsIgnoreCase(_intv) || "d".equalsIgnoreCase(_intv)) {
            result = (int) Math
                    .rint((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / (1000 * 60 * 60 * 24));
        } else if (_intv.equalsIgnoreCase("m")) {
            result = dateDiff("yyyy", _dt1, _dt2) * 12 + clMax.get(Calendar.MONTH) - clMin.get(Calendar.MONTH);
        } else if (_intv.equalsIgnoreCase("w") || _intv.equalsIgnoreCase("ww")) {
            result = (int) Math
                    .floor((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / (1000 * 60 * 60 * 24 * 7));
        } else if (_intv.equalsIgnoreCase("h")) {
            result = (int) Math.round((clMax.getTime().getTime() - clMin.getTime().getTime()) / (1000d * 60 * 60));
        } else if (_intv.equalsIgnoreCase("n")) {
            result = (int) Math.rint((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / (1000 * 60));
        } else if (_intv.equalsIgnoreCase("s")) {
            result = (int) Math.rint((double) (clMax.getTimeInMillis() - clMin.getTimeInMillis()) / 1000);
        } else {
            throw new UcanaccessSQLException(ExceptionMessages.INVALID_INTERVAL_VALUE);
        }
        return result * sign;
    }

    @FunctionType(namingConflict = true, functionName = "DATEPART", argumentTypes = { AccessType.MEMO, AccessType.MEMO,
            AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, String _dt, Integer _firstDayOfWeek) throws UcanaccessSQLException {
        return datePart(_intv, dateValue(_dt, false), _firstDayOfWeek);
    }

    @FunctionType(namingConflict = true, functionName = "DATEPART", argumentTypes = { AccessType.MEMO,
            AccessType.DATETIME, AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, Timestamp _dt, Integer _firstDayOfWeek) throws UcanaccessSQLException {
        Integer ret = _intv.equalsIgnoreCase("ww") ? datePart(_intv, _dt, _firstDayOfWeek, 1) : datePart(_intv, _dt);
        if (_intv.equalsIgnoreCase("w") && _firstDayOfWeek > 1) {
            Calendar cl = Calendar.getInstance();
            cl.setTime(_dt);
            ret = cl.get(Calendar.DAY_OF_WEEK) - _firstDayOfWeek + 1;
            if (ret <= 0) {
                ret = 7 + ret;
            }
        }
        return ret;
    }

    @FunctionType(namingConflict = true, functionName = "DATEPART", argumentTypes = { AccessType.MEMO, AccessType.MEMO,
            AccessType.LONG, AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, String _dt, Integer _firstDayOfWeek, Integer _firstWeekOfYear)
            throws UcanaccessSQLException {
        return datePart(_intv, dateValue(_dt, false), _firstDayOfWeek, _firstWeekOfYear);
    }

    @FunctionType(namingConflict = true, functionName = "DATEPART", argumentTypes = {AccessType.MEMO, AccessType.DATETIME, AccessType.LONG, AccessType.LONG}, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, Timestamp _dt, Integer _firstDayOfWeek, Integer _firstWeekOfYear) throws UcanaccessSQLException {
        Integer ret = datePart(_intv, _dt);
        if (ret != null && _intv.equalsIgnoreCase("ww") && (_firstWeekOfYear > 1 || _firstDayOfWeek > 1)) {
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

    @FunctionType(namingConflict = true, functionName = "DATEPART", argumentTypes = { AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, String _dt) throws UcanaccessSQLException {
        return datePart(_intv, dateValue(_dt, false));
    }

    @FunctionType(namingConflict = true, functionName = "DATEPART", argumentTypes = { AccessType.MEMO,
            AccessType.DATETIME }, returnType = AccessType.LONG)
    public static Integer datePart(String _intv, Timestamp _dt) throws UcanaccessSQLException {
        if (_intv == null || _dt == null) {
            return null;
        }
        Calendar cl = Calendar.getInstance(Locale.US);
        cl.setTime(_dt);
        if (_intv.equalsIgnoreCase("yyyy")) {
            return cl.get(Calendar.YEAR);
        } else if (_intv.equalsIgnoreCase("q")) {
            return (int) Math.ceil((cl.get(Calendar.MONTH) + 1) / 3d);
        } else if (_intv.equalsIgnoreCase("d")) {
            return cl.get(Calendar.DAY_OF_MONTH);
        } else if (_intv.equalsIgnoreCase("y")) {
            return cl.get(Calendar.DAY_OF_YEAR);
        } else if (_intv.equalsIgnoreCase("m")) {
            return cl.get(Calendar.MONTH) + 1;
        } else if (_intv.equalsIgnoreCase("ww")) {
            return cl.get(Calendar.WEEK_OF_YEAR);
        } else if (_intv.equalsIgnoreCase("w")) {
            return cl.get(Calendar.DAY_OF_WEEK);
        } else if (_intv.equalsIgnoreCase("h")) {
            return cl.get(Calendar.HOUR_OF_DAY);
        } else if (_intv.equalsIgnoreCase("n")) {
            return cl.get(Calendar.MINUTE);
        } else if (_intv.equalsIgnoreCase("s")) {
            return cl.get(Calendar.SECOND);
        } else {
            throw new UcanaccessSQLException(ExceptionMessages.INVALID_INTERVAL_VALUE);
        }
    }

    @FunctionType(functionName = "DATESERIAL", argumentTypes = { AccessType.LONG, AccessType.LONG,
            AccessType.LONG }, returnType = AccessType.DATETIME)
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

    @FunctionType(functionName = "DATEVALUE", argumentTypes = { AccessType.MEMO }, returnType = AccessType.DATETIME)
    public static Timestamp dateValue(String _dt) {
        return dateValue(_dt, true);
    }

    @FunctionType(functionName = "TIMESTAMP0", argumentTypes = { AccessType.MEMO }, returnType = AccessType.DATETIME)
    public static Timestamp timestamp0(String _dt) {
        GregorianCalendar gc = new GregorianCalendar();
        gc.setGregorianChange(new Date(Long.MIN_VALUE));
        Pattern ptdate = Pattern.compile(SQLConverter.DATE_FORMAT + "\\s");
        Pattern pth = Pattern.compile(SQLConverter.HHMMSS_FORMAT);
        Matcher mtc = ptdate.matcher(_dt);
        if (mtc.find()) {
            gc.set(Integer.parseInt(mtc.group(1)), Integer.parseInt(mtc.group(2)) - 1, Integer.parseInt(mtc.group(3)));
        } else {
            throw new UcanaccessRuntimeException("internal error in parsing timestamp");
        }
        mtc = pth.matcher(_dt);
        if (mtc.find()) {
            gc.set(Calendar.HOUR_OF_DAY, Integer.parseInt(mtc.group(1)));
            gc.set(Calendar.MINUTE, Integer.parseInt(mtc.group(2)));
            gc.set(Calendar.SECOND, Integer.parseInt(mtc.group(3)));
        } else {
            throw new UcanaccessRuntimeException("internal error in parsing timestamp");
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
            } catch (ParseException _ignored) {
            }
        }
        return null;
    }

    @FunctionType(functionName = "DATEVALUE", argumentTypes = { AccessType.DATETIME }, returnType = AccessType.DATETIME)
    public static Timestamp dateValue(Timestamp _dt) {
        Calendar cl = Calendar.getInstance();
        cl.setTime(_dt);
        cl.set(Calendar.HOUR_OF_DAY, 0);
        cl.set(Calendar.MINUTE, 0);
        cl.set(Calendar.SECOND, 0);
        cl.set(Calendar.MILLISECOND, 0);
        return new Timestamp(cl.getTime().getTime());
    }

    @FunctionType(functionName = "FORMAT", argumentTypes = { AccessType.DOUBLE,
            AccessType.TEXT }, returnType = AccessType.TEXT)
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

    @FunctionType(functionName = "FORMAT", argumentTypes = { AccessType.TEXT,
            AccessType.TEXT }, returnType = AccessType.TEXT)
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

    @FunctionType(functionName = "FORMAT", argumentTypes = { AccessType.DATETIME, AccessType.TEXT }, returnType = AccessType.TEXT)
    public static String format(Timestamp _t, String _par) throws UcanaccessSQLException {
        if (_t == null) {
            return "";
        }
        RegionalSettings reg = getRegionalSettings();

        if ("long date".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getLongDatePattern());
        }
        if ("medium date".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getMediumDatePattern());
        }
        if ("short date".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getShortDatePattern());
        }
        if ("general date".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getGeneralPattern());
        }
        if ("long time".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getLongTimePattern());
        }
        if ("medium time".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getMediumTimePattern());
        }
        if ("short time".equalsIgnoreCase(_par)) {
            return formatDate(_t, reg.getShortTimePattern());
        }
        if ("q".equalsIgnoreCase(_par)) {
            return String.valueOf(datePart(_par, _t));
        }
        return createSimpleDateFormat(_par
            .replace("m", "M")
            .replace("n", "m")
            .replace("(?i)AM/PM|A/P|AMPM", "a")
            .replace("dddd", "EEEE")).format(_t);
    }

    @FunctionType(functionName = "IIF", argumentTypes = { AccessType.YESNO, AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.MEMO)
    public static String iif(Boolean _b, String _o, String _o1) {
        return (String) iif(_b, _o, (Object) _o1);
    }

    @FunctionType(functionName = "IIF", argumentTypes = { AccessType.YESNO, AccessType.LONG,
            AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer iif(Boolean b, Integer o, Integer o1) {
        return (Integer) iif(b, o, (Object) o1);
    }

    @FunctionType(functionName = "IIF", argumentTypes = { AccessType.YESNO, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static Double iif(Boolean b, Double o, Double o1) {
        return (Double) iif(b, o, (Object) o1);
    }

    @FunctionType(functionName = "IIF", argumentTypes = { AccessType.YESNO, AccessType.YESNO,
            AccessType.YESNO }, returnType = AccessType.YESNO)
    public static Boolean iif(Boolean b, Boolean o, Boolean o1) {

        return (Boolean) iif(b, o, (Object) o1);
    }

    @FunctionType(functionName = "IIF", argumentTypes = { AccessType.YESNO, AccessType.DATETIME,
            AccessType.DATETIME }, returnType = AccessType.DATETIME)
    public static Timestamp iif(Boolean b, Timestamp o, Timestamp o1) {
        return (Timestamp) iif(b, o, (Object) o1);
    }

    private static Object iif(Boolean _b, Object _o1, Object _o2) {
        if (_b == null) {
            _b = Boolean.FALSE;
        }
        return _b ? _o1 : _o2;
    }

    @FunctionType(namingConflict = true, functionName = "INSTR", argumentTypes = { AccessType.LONG, AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.LONG)
    public static Integer instr(Integer _start, String _text, String _search) {
        return instr(_start, _text, _search, -1);
    }

    @FunctionType(namingConflict = true, functionName = "INSTR", argumentTypes = { AccessType.LONG, AccessType.MEMO,
            AccessType.MEMO, AccessType.LONG }, returnType = AccessType.LONG)
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

    @FunctionType(namingConflict = true, functionName = "INSTR", argumentTypes = { AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.LONG)
    public static Integer instr(String text, String search) {
        return instr(1, text, search, -1);
    }

    @FunctionType(namingConflict = true, functionName = "INSTR", argumentTypes = { AccessType.MEMO, AccessType.MEMO,
            AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer instr(String text, String search, Integer compare) {
        return instr(1, text, search, compare);
    }

    @FunctionType(functionName = "INSTRREV", argumentTypes = { AccessType.TEXT,
            AccessType.TEXT }, returnType = AccessType.LONG)
    public static Integer instrrev(String text, String search) {
        return instrrev(text, search, -1, -1);
    }

    @FunctionType(functionName = "INSTRREV", argumentTypes = { AccessType.MEMO, AccessType.MEMO,
            AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer instrrev(String text, String search, Integer start) {
        return instrrev(text, search, start, -1);
    }

    @FunctionType(functionName = "INSTRREV", argumentTypes = { AccessType.MEMO, AccessType.MEMO, AccessType.LONG,
            AccessType.LONG }, returnType = AccessType.LONG)
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

    @FunctionType(functionName = "ISDATE", argumentTypes = { AccessType.MEMO }, returnType = AccessType.YESNO)
    public static boolean isDate(String dt) {
        return dateValue(dt) != null;
    }

    @FunctionType(functionName = "ISDATE", argumentTypes = { AccessType.DATETIME }, returnType = AccessType.YESNO)
    public static boolean isDate(Timestamp dt) {
        return true;
    }

    @FunctionType(namingConflict = true, functionName = "IsNull", argumentTypes = {
            AccessType.MEMO }, returnType = AccessType.YESNO)
    public static boolean isNull(String o) {
        return o == null;
    }

    @FunctionType(namingConflict = true, functionName = "IsNull", argumentTypes = {
            AccessType.DATETIME }, returnType = AccessType.YESNO)
    public static boolean isNull(Timestamp o) {
        return o == null;
    }

    @FunctionType(namingConflict = true, functionName = "IsNull", argumentTypes = {
            AccessType.DOUBLE }, returnType = AccessType.YESNO)
    public static boolean isNull(Double o) {
        return o == null;
    }

    @FunctionType(functionName = "ISNUMERIC", argumentTypes = { AccessType.NUMERIC }, returnType = AccessType.YESNO)
    public static boolean isNumeric(BigDecimal b) {
        return true;
    }

    @FunctionType(functionName = "ISNUMERIC", argumentTypes = { AccessType.MEMO }, returnType = AccessType.YESNO)
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

    @FunctionType(functionName = "LEFT", namingConflict = true, argumentTypes = { AccessType.MEMO,
            AccessType.LONG }, returnType = AccessType.MEMO)
    public static String left(String input, int i) {
        if (input == null || i < 0) {
            return null;
        }
        if (i >= input.length()) {
            return input;
        } else {
            return input.substring(0, i);
        }
    }

    @FunctionType(functionName = "\"LEFT$\"", argumentTypes = { AccessType.MEMO,
            AccessType.LONG }, returnType = AccessType.MEMO)
    public static String leftS(String input, int i) {
        return left(input, i);
    }

    @FunctionType(functionName = "LEN", argumentTypes = { AccessType.MEMO }, returnType = AccessType.LONG)
    public static Integer len(String o) {
        if (o == null) {
            return null;
        }
        return o.length();
    }

    @FunctionType(functionName = "MID", argumentTypes = { AccessType.MEMO,
            AccessType.LONG }, returnType = AccessType.MEMO)
    public static String mid(String value, int start) {
        return mid(value, start, value.length());
    }

    @FunctionType(functionName = "MID", argumentTypes = { AccessType.MEMO, AccessType.LONG,
            AccessType.LONG }, returnType = AccessType.MEMO)
    public static String mid(String value, int start, int length) {
        if (value == null) {
            return null;
        }
        int len = start - 1 + length;
        if (start < 1) {
            throw new UcanaccessRuntimeException("Invalid function call");
        }
        if (len > value.length()) {
            len = value.length();
        }
        return value.substring(start - 1, len);
    }

    @FunctionType(namingConflict = true, functionName = "MONTHNAME", argumentTypes = {
            AccessType.LONG }, returnType = AccessType.TEXT)
    public static String monthName(int i) throws UcanaccessSQLException {
        return monthName(i, false);
    }

    @FunctionType(namingConflict = true, functionName = "MONTHNAME", argumentTypes = { AccessType.LONG,
            AccessType.YESNO }, returnType = AccessType.TEXT)
    public static String monthName(int i, boolean abbr) throws UcanaccessSQLException {
        i--;
        if (i >= 0 && i <= 11) {
            DateFormatSymbols dfs = new DateFormatSymbols();
            return abbr ? dfs.getShortMonths()[i] : dfs.getMonths()[i];
        }
        throw new UcanaccessSQLException(ExceptionMessages.INVALID_MONTH_NUMBER);
    }

    @FunctionType(functionName = "DATE", argumentTypes = {}, returnType = AccessType.DATETIME)
    public static Timestamp date() {
        Calendar cl = Calendar.getInstance();
        cl.set(Calendar.MILLISECOND, 0);
        cl.set(Calendar.SECOND, 0);
        cl.set(Calendar.MINUTE, 0);
        cl.set(Calendar.HOUR_OF_DAY, 0);
        return new Timestamp(cl.getTime().getTime());
    }

    @FunctionType(namingConflict = true, functionName = "NOW", argumentTypes = {}, returnType = AccessType.DATETIME)
    public static Timestamp now() {
        return new Timestamp(System.currentTimeMillis() / 1000 * 1000);
    }

    private static Object nz(Object value, Object outher) {
        return value == null ? outher : value;
    }

    @FunctionType(functionName = "NZ", argumentTypes = { AccessType.MEMO }, returnType = AccessType.MEMO)
    public static String nz(String value) {
        return value == null ? "" : value;
    }

    @FunctionType(functionName = "NZ", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static Double nz(Double value) {
        return value == null ? 0 : value;
    }

    @FunctionType(functionName = "NZ", argumentTypes = { AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer nz(Integer value) {
        return value == null ? 0 : value;
    }

    @FunctionType(functionName = "NZ", argumentTypes = { AccessType.NUMERIC }, returnType = AccessType.NUMERIC)
    public static BigDecimal nz(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    @FunctionType(functionName = "NZ", argumentTypes = { AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.MEMO)
    public static String nz(String value, String outher) {
        return (String) nz(value, (Object) outher);
    }

    @FunctionType(functionName = "SIGN", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.INTEGER)
    public static short sign(double n) {
        return (short) (n == 0 ? 0 : n > 0 ? 1 : -1);
    }

    @FunctionType(functionName = "SPACE", argumentTypes = { AccessType.LONG }, returnType = AccessType.MEMO)
    public static String space(Integer nr) {
        return " ".repeat(Math.max(0, nr));
    }

    @FunctionType(functionName = "STR", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.TEXT)
    public static String str(double d) {
        String pre = d > 0 ? " " : "";
        return Math.round(d) == d ? pre + Math.round(d) : pre + d;
    }

    @FunctionType(functionName = "TIME", argumentTypes = {}, returnType = AccessType.DATETIME)
    public static Timestamp time() {
        Calendar cl = Calendar.getInstance();
        cl.setTime(now());
        cl.set(1899, 11, 30);
        return new Timestamp(cl.getTimeInMillis());
    }

    @FunctionType(functionName = "VAL", argumentTypes = { AccessType.NUMERIC }, returnType = AccessType.DOUBLE)
    public static Double val(BigDecimal val1) {
        return val((Object) val1);
    }

    private static Double val(Object val1) {
        if (val1 == null) {
            return null;
        }
        String val = val1.toString().trim();
        int lp = val.lastIndexOf('.');
        char[] ca = val.toCharArray();
        StringBuilder sb = new StringBuilder();
        int minLength = 1;
        for (int i = 0; i < ca.length; i++) {
            char c = ca[i];
            if ((c == '-' || c == '+') && i == 0) {
                ++minLength;
                sb.append(c);
            } else if (c == ' ') {
                continue;
            } else if (Character.isDigit(c)) {
                sb.append(c);
            } else if (c == '.' && i == lp) {
                sb.append(c);
                if (i == 0 || i == 1 && minLength == 2) {
                    ++minLength;
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

    @FunctionType(functionName = "VAL", argumentTypes = { AccessType.MEMO }, returnType = AccessType.DOUBLE)
    public static Double val(String val1) {
        return val((Object) val1);
    }

    @FunctionType(functionName = "WEEKDAYNAME", argumentTypes = { AccessType.LONG }, returnType = AccessType.TEXT)
    public static String weekDayName(int i) {
        return weekDayName(i, false);
    }

    @FunctionType(functionName = "WEEKDAYNAME", argumentTypes = { AccessType.LONG,
            AccessType.YESNO }, returnType = AccessType.TEXT)
    public static String weekDayName(int i, boolean abbr) {
        return weekDayName(i, abbr, 1);
    }

    @FunctionType(functionName = "WEEKDAYNAME", argumentTypes = { AccessType.LONG, AccessType.YESNO,
            AccessType.LONG }, returnType = AccessType.TEXT)
    public static String weekDayName(int i, boolean abbr, int s) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DAY_OF_WEEK, i + s - 1);
        String pattern = abbr ? "%ta" : "%tA";
        return String.format(pattern, cal, cal);
    }

    @FunctionType(functionName = "WEEKDAY", argumentTypes = { AccessType.DATETIME }, returnType = AccessType.LONG)
    public static Integer weekDay(Timestamp dt) throws UcanaccessSQLException {
        return datePart("w", dt);
    }

    @FunctionType(functionName = "WEEKDAY", argumentTypes = { AccessType.DATETIME,
            AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer weekDay(Timestamp dt, Integer firstDayOfWeek) throws UcanaccessSQLException {
        return datePart("w", dt, firstDayOfWeek);
    }

    @FunctionType(functionName = "STRING", argumentTypes = { AccessType.LONG,
            AccessType.MEMO }, returnType = AccessType.MEMO)
    public static String string(Integer nr, String str) {
        if (str == null) {
            return null;
        }
        String ret = "";
        for (int i = 0; i < nr; ++i) {
            ret += str.charAt(0);
        }
        return ret;
    }

    @FunctionType(functionName = "TIMESERIAL", argumentTypes = { AccessType.LONG, AccessType.LONG,
            AccessType.LONG }, returnType = AccessType.DATETIME)
    public static Timestamp timeserial(Integer h, Integer m, Integer s) {
        Calendar cl = Calendar.getInstance();
        cl.setTime(now());
        cl.set(1899, 11, 30, h, m, s);
        return new Timestamp(cl.getTimeInMillis());
    }

    @FunctionType(functionName = "RND", argumentTypes = {}, returnType = AccessType.DOUBLE)
    public static Double rnd() {
        return rnd(null);
    }

    @FunctionType(functionName = "RND", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static Double rnd(Double d) {
        if (d == null || d > 0) {
            lastRnd = Math.random();
            return lastRnd;
        }
        if (d < 0) {
            if (rnd == null) {
                rnd = d;
            }
            return rnd;
        }
        if (d == 0) {
            if (lastRnd == null) {
                lastRnd = Math.random();
            }
            return lastRnd;
        }
        return null;
    }

    @FunctionType(functionName = "NZ", argumentTypes = { AccessType.NUMERIC,
            AccessType.NUMERIC }, returnType = AccessType.NUMERIC)
    public static BigDecimal nz(BigDecimal value, BigDecimal outher) {
        return (BigDecimal) nz(value, (Object) outher);
    }

    @FunctionType(functionName = "NZ", argumentTypes = { AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static Double nz(Double value, Double outher) {
        return (Double) nz(value, (Object) outher);
    }

    @FunctionType(functionName = "NZ", argumentTypes = { AccessType.LONG,
            AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer nz(Integer value, Integer outher) {
        return (Integer) nz(value, (Object) outher);
    }

    @FunctionType(functionName = "STRREVERSE", argumentTypes = { AccessType.MEMO }, returnType = AccessType.MEMO)
    public static String strReverse(String value) {
        if (value == null) {
            return null;
        }
        return new StringBuffer(value).reverse().toString();
    }

    @FunctionType(functionName = "STRCONV", argumentTypes = { AccessType.MEMO,
            AccessType.LONG }, returnType = AccessType.MEMO)
    public static String strConv(String value, int ul) {
        if (value == null) {
            return null;
        }
        if (ul == 1) {
            value = value.toUpperCase();
        }
        if (ul == 2) {
            value = value.toLowerCase();
        }
        return value;
    }

    @FunctionType(functionName = "STRCOMP", argumentTypes = { AccessType.MEMO, AccessType.MEMO,
            AccessType.LONG }, returnType = AccessType.LONG)
    public static Integer strComp(String value1, String value2, Integer type) throws UcanaccessSQLException {
        switch (type) {
        case 0:
        case -1:
        case 2:
            return value1.compareTo(value2);
        case 1:
            return value1.toUpperCase().compareTo(value2.toUpperCase());
        default:
            throw new UcanaccessSQLException(ExceptionMessages.INVALID_PARAMETER);
        }
    }

    @FunctionType(functionName = "STRCOMP", argumentTypes = { AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.LONG)
    public static Integer strComp(String value1, String value2) throws UcanaccessSQLException {
        return strComp(value1, value2, 0);
    }

    @FunctionType(functionName = "INT", argumentTypes = { AccessType.DOUBLE }, returnType = AccessType.LONG)
    public static Integer mint(Double value) {
        return new BigDecimal((long) Math.floor(value)).intValueExact();
    }

    @FunctionType(functionName = "INT", argumentTypes = { AccessType.YESNO }, returnType = AccessType.INTEGER)
    public static Short mint(boolean value) {
        return (short) (value ? -1 : 0);
    }

    @FunctionType(functionName = "DDB", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double ddb(double cost, double salvage, double life, double period) {
        return ddb(cost, salvage, life, period, 2d);
    }

    @FunctionType(functionName = "DDB", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
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

    @FunctionType(functionName = "FV", argumentTypes = { AccessType.DOUBLE, AccessType.LONG,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double fv(double rate, int periods, double payment) {
        return fv(rate, periods, payment, 0, 0);
    }

    @FunctionType(functionName = "FV", argumentTypes = { AccessType.DOUBLE, AccessType.LONG, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double fv(double rate, int periods, double payment, double pv) {
        return fv(rate, periods, payment, pv, 0);
    }

    @FunctionType(functionName = "FV", argumentTypes = { AccessType.DOUBLE, AccessType.LONG, AccessType.DOUBLE,
            AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double fv(double rate, int periods, double payment, double pv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;
        double fv = pv * Math.pow(1 + rate, periods);
        for (int i = 0; i < periods; i++) {
            fv += payment * Math.pow(1 + rate, i + type);
        }
        return -fv;
    }

    @FunctionType(functionName = "PMT", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double pmt(double rate, double periods, double pv) {
        return pmt(rate, periods, pv, 0, 0);
    }

    @FunctionType(functionName = "PMT", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double pmt(double rate, double periods, double pv, double fv) {
        return pmt(rate, periods, pv, 0, 0);
    }

    @FunctionType(functionName = "PMT", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double pmt(double rate, double periods, double pv, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;

        if (rate == 0) {
            return -1 * (fv + pv) / periods;
        } else {
            return (fv + pv * Math.pow(1 + rate, periods)) * rate
                    / ((type == 1 ? 1 + rate : 1) * (1 - Math.pow(1 + rate, periods)));
        }

    }

    @FunctionType(functionName = "NPER", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double nper(double rate, double pmt, double pv) {
        return nper(rate, pmt, pv, 0, 0);
    }

    @FunctionType(functionName = "NPER", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double nper(double rate, double pmt, double pv, double fv) {

        return nper(rate, pmt, pv, fv, 0);
    }

    @FunctionType(functionName = "NPER", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
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

    @FunctionType(functionName = "IPMT", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double ipmt(double rate, double per, double nper, double pv) {
        return ipmt(rate, per, nper, pv, 0, 0);
    }

    @FunctionType(functionName = "IPMT", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double ipmt(double rate, double per, double nper, double pv, double fv) {
        return ipmt(rate, per, nper, pv, fv, 0);
    }

    @FunctionType(functionName = "IPMT", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double ipmt(double rate, double per, double nper, double pv, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;
        double ipmt = fv(rate, (int) per - 1, pmt(rate, nper, pv, fv, type), pv, type) * rate;
        if (type == 1) {
            ipmt = ipmt / (1 + rate);
        }
        return ipmt;
    }

    @FunctionType(functionName = "PV", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double pv(double rate, double nper, double pmt) {
        return pv(rate, nper, pmt, 0, 0);

    }

    @FunctionType(functionName = "PV", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double pv(double rate, double nper, double pmt, double fv) {
        return pv(rate, nper, pmt, fv, 0);

    }

    @FunctionType(functionName = "PV", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double pv(double rate, double nper, double pmt, double fv, double type) {
        type = Math.abs(type) >= 1 ? 1 : 0;

        if (rate == 0) {
            return -1 * (nper * pmt + fv);
        } else {

            return ((1 - Math.pow(1 + rate, nper)) / rate * (type == 1 ? 1 + rate : 1) * pmt - fv)
                    / Math.pow(1 + rate, nper);
        }

    }

    @FunctionType(functionName = "PPMT", argumentTypes = { AccessType.DOUBLE, AccessType.LONG, AccessType.LONG,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double ppmt(double rate, int per, int nper, double pv) {
        return ppmt(rate, per, nper, pv, 0, 0);
    }

    @FunctionType(functionName = "PPMT", argumentTypes = { AccessType.DOUBLE, AccessType.LONG, AccessType.LONG,
            AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double ppmt(double rate, int per, int nper, double pv, double fv) {
        return ppmt(rate, per, nper, pv, fv, 0);
    }

    @FunctionType(functionName = "PPMT", argumentTypes = { AccessType.DOUBLE, AccessType.LONG, AccessType.LONG,
            AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double ppmt(double rate, int per, int nper, double pv, double fv, double type) {
        return pmt(rate, nper, pv, fv, type) - ipmt(rate, per, nper, pv, fv, type);
    }

    @FunctionType(functionName = "SLN", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double sln(double cost, double salvage, double life) {
        return (cost - salvage) / life;
    }

    @FunctionType(functionName = "SYD", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double syd(double cost, double salvage, double life, double per) {
        return (cost - salvage) * (life - per + 1) * 2 / (life * (life + 1));
    }

    @FunctionType(functionName = "RATE", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv) {
        return rate(nper, pmt, pv, 0, 0, 0.1);
    }

    @FunctionType(functionName = "RATE", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv, double fv) {
        return rate(nper, pmt, pv, fv, 0, 0.1);
    }

    @FunctionType(functionName = "RATE", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double rate(double nper, double pmt, double pv, double fv, double type) {
        return rate(nper, pmt, pv, fv, type, 0.1);
    }

    @FunctionType(functionName = "RATE", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
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

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = { AccessType.DOUBLE,
            AccessType.MEMO }, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(Double res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = { AccessType.YESNO,
            AccessType.MEMO }, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(Boolean res, String datatype) {
        if (res == null) {
            return null;
        }
        return res ? -1d : 0d;
    }

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = { AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(String _res, String _datatype) {
        if (_res == null) {
            return null;
        }
        return Try.catching(() -> {
            DecimalFormatSymbols dfs = DecimalFormatSymbols.getInstance();
            String sep = dfs.getDecimalSeparator() + "";
            String gs = dfs.getGroupingSeparator() + "";
            String res = _res.replaceAll(Pattern.quote(gs), "");
            if (!sep.equalsIgnoreCase(".")) {
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

    @FunctionType(functionName = "formulaToNumeric", argumentTypes = { AccessType.DATETIME,
            AccessType.MEMO }, returnType = AccessType.DOUBLE)
    public static Double formulaToNumeric(Timestamp res, String datatype) throws UcanaccessSQLException {
        if (res == null) {
            return null;
        }
        Calendar clbb = Calendar.getInstance();
        clbb.set(1899, 11, 30, 0, 0, 0);
        return (double) dateDiff("y", new Timestamp(clbb.getTimeInMillis()), res);
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = { AccessType.YESNO,
            AccessType.MEMO }, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Boolean res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = { AccessType.DOUBLE,
            AccessType.MEMO }, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Double res, String datatype) {
        if (res == null) {
            return null;
        }
        return res != 0d;
    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = { AccessType.DATETIME,
            AccessType.MEMO }, returnType = AccessType.YESNO)
    public static Boolean formulaToBoolean(Timestamp res, String datatype) {
        return null;

    }

    @FunctionType(functionName = "formulaToBoolean", argumentTypes = { AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.YESNO)
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

    @FunctionType(functionName = "formulaToText", argumentTypes = { AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.MEMO)
    public static String formulaToText(String res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToText", argumentTypes = { AccessType.DOUBLE,
            AccessType.MEMO }, returnType = AccessType.MEMO)
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

    @FunctionType(functionName = "formulaToText", argumentTypes = { AccessType.YESNO,
            AccessType.MEMO }, returnType = AccessType.MEMO)
    public static String formulaToText(Boolean res, String datatype) {
        if (res == null) {
            return null;
        }
        return res ? "-1" : "0";
    }

    @FunctionType(functionName = "formulaToText", argumentTypes = { AccessType.DATETIME,
            AccessType.MEMO }, returnType = AccessType.MEMO)
    public static String formulaToText(Timestamp res, String datatype) throws UcanaccessSQLException {
        Calendar cl = Calendar.getInstance();
        cl.setTimeInMillis(res.getTime());
        if (cl.get(Calendar.HOUR) == 0 && cl.get(Calendar.MINUTE) == 0 && cl.get(Calendar.SECOND) == 0) {
            return format(res, "short date");
        } else {
            return format(res, "general date");
        }
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = { AccessType.DATETIME,
            AccessType.MEMO }, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(Timestamp res, String datatype) {
        return res;
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = { AccessType.MEMO,
            AccessType.MEMO }, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(String res, String datatype) {
        if (res == null) {
            return null;
        }
        return Try.catching(() -> dateValue(res, false)).orIgnore();
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = { AccessType.YESNO,
            AccessType.MEMO }, returnType = AccessType.DATETIME)
    public static Timestamp formulaToDate(Boolean res, String datatype) throws UcanaccessSQLException {
        if (res == null) {
            return null;
        }
        Calendar clbb = Calendar.getInstance();
        clbb.set(1899, 11, 30, 0, 0, 0);
        clbb.set(Calendar.MILLISECOND, 0);
        return dateAdd("y", res ? -1 : 0, new Timestamp(clbb.getTimeInMillis()));
    }

    @FunctionType(functionName = "orderJet", argumentTypes = { AccessType.MEMO }, returnType = AccessType.MEMO)
    public static String orderJet(String s) {
        return s.replaceAll("([a-zA-Z0-9])[\\-–—]([a-zA-Z0-9])", "$1$2");
    }

    @FunctionType(functionName = "formulaToDate", argumentTypes = { AccessType.DOUBLE, AccessType.MEMO }, returnType = AccessType.DATETIME)
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

    @FunctionType(functionName = "RIGHT", namingConflict = true, argumentTypes = { AccessType.MEMO,
            AccessType.LONG }, returnType = AccessType.MEMO)
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

    @FunctionType(functionName = "\"RIGHT$\"", argumentTypes = { AccessType.MEMO,
            AccessType.LONG }, returnType = AccessType.MEMO)
    public static String rightS(String input, int i) {
        return right(input, i);
    }

    @FunctionType(namingConflict = true, functionName = "ROUND", argumentTypes = { AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double round(double d, double p) {
        double f = Math.pow(10d, p);
        return Math.round(d * f) / f;
    }

    @FunctionType(namingConflict = true, functionName = "FIX", argumentTypes = {
            AccessType.DOUBLE }, returnType = AccessType.DOUBLE)
    public static double fix(double d) {
        return sign(d) * (double) mint(Math.abs(d));
    }

    @FunctionType(functionName = "PARTITION", argumentTypes = { AccessType.DOUBLE, AccessType.DOUBLE, AccessType.DOUBLE,
            AccessType.DOUBLE }, returnType = AccessType.MEMO)
    public static String partition(Double number, double start, double stop, double interval) {
        if (number == null) {
            return null;
        }
        number = Math.rint(number);
        interval = Math.rint(interval);

        String ul = String.valueOf(lrint(stop) + 1);
        stop = lrint(stop);
        start = lrint(start);
        int h = ul.length();
        if (number < start) {
            return padLeft(-1, h) + ":" + padLeft(lrint(start) - 1, h);
        }
        if (number > stop) {
            return ul + ":" + padLeft(-1, h);
        }

        for (double d = start; d <= stop; d += interval) {
            if (number >= d && number < d + interval) {
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
