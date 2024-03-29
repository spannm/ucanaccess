package net.ucanaccess.converters;

import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public final class RegionalSettings {

    static final Map<Locale, RegionalSettings> REG_MAP = new HashMap<>();

    private final ResourceBundle                 dateBundle;
    private final Locale                         locale;
    private boolean                              pointDateSeparator;
    private final Map<SimpleDateFormat, Boolean> dateFormats = new LinkedHashMap<>();
    private final List<String>                   dateFormatPatterns = new ArrayList<>();

    RegionalSettings() {
        this(Locale.getDefault());
    }

    RegionalSettings(Locale _locale) {
        dateBundle = ResourceBundle.getBundle("net.ucanaccess.util.format.dateFormat", _locale);
        locale = _locale;
        String[] dfsp = new String[] {getGeneralPattern(), getLongDatePattern(), getMediumDatePattern(), getShortDatePattern()};
        for (String pattern : dfsp) {
            if (pattern.indexOf('.') > 0 && !pattern.contains("h.") && !pattern.contains("H.")) {
                pointDateSeparator = true;
                break;
            }
        }

        addDateP("yyyy-MM-dd h:m:s a", false, false);
        addDateP("yyyy-MM-dd H:m:s", false, false);
        addDateP("yyyy-MM-dd", false, false);
        addDateP("yyyy/MM/dd h:m:s a", false, false);
        addDateP("yyyy/MM/dd H:m:s", false, false);
        addDateP("yyyy/MM/dd", false, false);

        addDateP(getGeneralPattern(), true, false);
        addDateP(getLongDatePattern(), true, false);
        addDateP(getMediumDatePattern(), true, false);

        addDateP(getShortDatePattern(), true, false);

        if (!locale.equals(Locale.US)) {
            RegionalSettings regUs = new RegionalSettings(Locale.US);
            addDateP(regUs.getGeneralPattern(), false, false);
            addDateP(regUs.getLongDatePattern(), true, false);
            addDateP(regUs.getMediumDatePattern(), true, false);
            addDateP(regUs.getShortDatePattern(), true, false);
        }

        addDateP("MMM dd,yyyy", false, false);
        addDateP("MM dd,yyyy", false, false);
        addDateP("MMM dd hh:mm:ss", false, true);
        addDateP("MM dd hh:mm:ss", false, true);
        addDateP("MMM yy hh:mm:ss", false, false);
        addDateP("MM yy hh:mm:ss", false, false);
        // locale is MM/dd/yyyy like in US but user is trying to parse something like 22/11/2003
        addDateP("dd/MM/yyyy h:m:s a", true, false);
        addDateP("dd/MM/yyyy H:m:s", true, false);
        addDateP("dd/MM/yyyy", true, false);
        addDateP("ddd MMM dd yyyy", false, false);

        for (SimpleDateFormat sdf : dateFormats.keySet()) {
            dateFormatPatterns.add(sdf.toPattern());
        }
    }

    public String getAM() {
        return dateBundle.getString("AM");
    }

    public String getPM() {
        return dateBundle.getString("PM");
    }

    public String getRS() {
        return dateBundle.getString("RS");
    }

    public String getLongDatePattern() {
        return dateBundle.getString("longDate");
    }

    public String getMediumDatePattern() {
        return dateBundle.getString("mediumDate");
    }

    public String getShortDatePattern() {
        return dateBundle.getString("shortDate");
    }

    public String getLongTimePattern() {
        return dateBundle.getString("longTime");
    }

    public String getMediumTimePattern() {
        return dateBundle.getString("mediumTime");
    }

    public String getShortTimePattern() {
        return dateBundle.getString("shortTime");
    }

    public String getGeneralPattern() {
        return dateBundle.getString("generalDate");
    }

    public boolean isPointDateSeparator() {
        return pointDateSeparator;
    }

    public Map<SimpleDateFormat, Boolean> getDateFormats() {
        return Collections.unmodifiableMap(dateFormats);
    }

    void addDateP(String _pattern, boolean _heuristic, boolean _yearOverride) {
        if (_heuristic && !_pattern.contains("a") && _pattern.indexOf('H') > 0) {
            String chg = _pattern.replace('H', 'h') + " a";
            addDateP(chg, false, false);
            addTogglePattern(chg);
        }

        SimpleDateFormat sdf = new SimpleDateFormat(_pattern, locale);
        ((GregorianCalendar) sdf.getCalendar()).setGregorianChange(new Date(Long.MIN_VALUE));
        sdf.setLenient(false);

        if ("true".equalsIgnoreCase(getRS())) {

            DateFormatSymbols df = new DateFormatSymbols();
            df.setAmPmStrings(new String[] {"AM", "PM"});
            sdf.setDateFormatSymbols(df);
        }

        dateFormats.put(sdf, _yearOverride);

        if (_heuristic) {
            addTogglePattern(_pattern);
            if (_pattern.endsWith(" a") && _pattern.indexOf('h') > 0) {
                String chg = _pattern.substring(0, _pattern.length() - 2).trim().replace('h', 'H');
                addDateP(chg, false, false);
                addTogglePattern(chg);
            }
        }

    }

    void addTogglePattern(String _p) {

        if (_p.indexOf('/') > 0) {
            addDateP(_p.replace('/', '-'), false, false);
            if (isPointDateSeparator()) {
                addDateP(_p.replace('/', '.'), false, false);
            }
        } else if (_p.indexOf('-') > 0) {
            addDateP(_p.replaceAll(Pattern.quote("-"), "/"), false, false);
            if (isPointDateSeparator()) {
                addDateP(_p.replaceAll(Pattern.quote("-"), "."), false, false);
            }

        } else if (_p.indexOf('.') > 0 && !_p.contains("h.") && !_p.contains("H.")) {
            addDateP(_p.replaceAll(Pattern.quote("."), "/"), false, false);
        }
    }

    public static RegionalSettings getRegionalSettings() {
        return getRegionalSettings(Locale.getDefault());
    }

    public static RegionalSettings getRegionalSettings(Locale _locale) {
        Locale locale = Objects.requireNonNullElseGet(_locale, Locale::getDefault);
        return REG_MAP.computeIfAbsent(locale, RegionalSettings::new);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[locale=" + locale + ", dateBundle=" + dateBundle + ", dateFormats=" + dateFormatPatterns + "]";
    }

}
