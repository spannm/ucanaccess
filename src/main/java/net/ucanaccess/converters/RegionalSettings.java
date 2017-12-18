/*
Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package net.ucanaccess.converters;

import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.regex.Pattern;

public final class RegionalSettings {

    static final Map<Locale, RegionalSettings> REG_MAP = new HashMap<Locale, RegionalSettings>();

    private final ResourceBundle                 dateBundle;
    private final Locale                         locale;
    private boolean                              pointDateSeparator;
    private final Map<SimpleDateFormat, Boolean> dateFormats = new LinkedHashMap<SimpleDateFormat, Boolean>();
    private final List<String>                   dateFormatPatterns = new ArrayList<String>();

    RegionalSettings() {
        this(Locale.getDefault());
    }

    RegionalSettings(Locale _locale) {
        dateBundle = ResourceBundle.getBundle("net.ucanaccess.util.format.dateFormat", _locale);
        locale = _locale;
        String[] dfsp = new String[] { getGeneralPattern(), getLongDatePattern(), getMediumDatePattern(),
                getShortDatePattern() };
        for (String pattern : dfsp) {
            if (pattern.indexOf(".") > 0 && pattern.indexOf("h.") < 0 && pattern.indexOf("H.") < 0) {
                pointDateSeparator = true;
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
        if (_heuristic) {
            if (_pattern.indexOf("a") < 0 && _pattern.indexOf("H") > 0) {
                String chg = _pattern.replaceAll("H", "h") + " a";
                addDateP(chg, false, false);
                addTogglePattern(chg);
            }
        }

        SimpleDateFormat sdf = new SimpleDateFormat(_pattern, locale);
        ((GregorianCalendar) sdf.getCalendar()).setGregorianChange(new java.util.Date(Long.MIN_VALUE));
        sdf.setLenient(false);

        if ("true".equalsIgnoreCase(getRS())) {

            DateFormatSymbols df = new DateFormatSymbols();
            df.setAmPmStrings(new String[] { "AM", "PM" });
            sdf.setDateFormatSymbols(df);
        }

        dateFormats.put(sdf, _yearOverride);

        if (_heuristic) {
            addTogglePattern(_pattern);
            if (_pattern.endsWith(" a") && _pattern.indexOf("h") > 0) {
                String chg = _pattern.substring(0, _pattern.length() - 2).trim().replaceAll("h", "H");
                addDateP(chg, false, false);
                addTogglePattern(chg);
            }
        }

    }

    void addTogglePattern(String _p) {

        if (_p.indexOf("/") > 0) {
            addDateP(_p.replaceAll("/", "-"), false, false);
            if (isPointDateSeparator()) {
                addDateP(_p.replaceAll("/", "."), false, false);
            }
        } else if (_p.indexOf("-") > 0) {
            addDateP(_p.replaceAll(Pattern.quote("-"), "/"), false, false);
            if (isPointDateSeparator()) {
                addDateP(_p.replaceAll(Pattern.quote("-"), "."), false, false);
            }

        } else if (_p.indexOf(".") > 0 && _p.indexOf("h.") < 0 && _p.indexOf("H.") < 0) {
            addDateP(_p.replaceAll(Pattern.quote("."), "/"), false, false);
        }
    }

    public static RegionalSettings getRegionalSettings() {
        return getRegionalSettings(Locale.getDefault());
    }

    public static RegionalSettings getRegionalSettings(Locale _locale) {
        if (_locale == null) {
            _locale = Locale.getDefault();
        }
        RegionalSettings rs = REG_MAP.get(_locale);
        if (rs == null) {
            rs = new RegionalSettings(_locale);
            REG_MAP.put(_locale, rs);
        }
        return rs;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[locale=" + locale + ", dateBundle=" + dateBundle + ", dateFormats=" + dateFormatPatterns + "]";
    }

}
