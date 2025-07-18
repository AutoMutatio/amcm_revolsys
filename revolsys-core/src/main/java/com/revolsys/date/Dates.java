package com.revolsys.date;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.INSTANT_SECONDS;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.revolsys.data.type.DataTypes;
import com.revolsys.logging.Logs;

public interface Dates {
  public static class Timer {
    private long startTime = System.currentTimeMillis();

    private long stepStartTime = this.startTime;

    private Timer() {

    }

    public void clear() {
      this.startTime = System.currentTimeMillis();
      this.stepStartTime = this.startTime;
    }

    public Duration duration() {
      return Duration.between(Instant.now(), Instant.ofEpochMilli(this.startTime));
    }

    public void printStep(final String message) {
      this.stepStartTime = printEllapsedTime(message, this.stepStartTime);

    }

    public void printTotal(final String message) {
      printEllapsedTime(message, this.startTime);
    }
  }

  DateTimeFormatter RFC_1123_DATE_TIME = newRfc1123();

  DateTimeFormatter yyyyMMdd = DateTimeFormatter.ofPattern("yyyyMMdd");

  DateTimeFormatter HHmmssSS = DateTimeFormatter.ofPattern("HHmmssSS");

  ZoneId UTC = ZoneId.of("UTC");

  Pattern DATE_TIME_NANOS_PATTERN = Pattern.compile(
    "\\s*(\\d{4})-(\\d{2})-(\\d{2})(?:[\\sT]+(\\d{2})\\:(\\d{2})\\:(\\d{2})(?:\\.(\\d{1,9}))?)?\\s*");

  DateTimeFormatter INSTANT_PARSER = new DateTimeFormatterBuilder()
    .appendOptional(DateTimeFormatter.ISO_INSTANT)
    .appendOptional(new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss")
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .parseLenient()
      .appendOffset("+HH:MM", "Z")
      .toFormatter())
    .toFormatter();

  DateTimeFormatter INSTANT_PARSER_UTC = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    .parseLenient()
    .toFormatter()
    .withZone(UTC);

  DateTimeFormatter ISO_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    .withZone(UTC);

  DateTimeFormatter ISO_TIME = DateTimeFormatter.ofPattern("HH:mm:ss")
    .withZone(UTC);

  static Set<DayOfWeek> days(final int... days) {
    final Set<DayOfWeek> daysOfWeek = new TreeSet<>();
    for (final int day : days) {
      final DayOfWeek dayOfWeek = DayOfWeek.of(day);
      daysOfWeek.add(dayOfWeek);
    }
    return daysOfWeek;
  }

  static long debugEllapsedTime(final Class<?> clazz, final String message, final long startTime) {
    final long endTime = System.currentTimeMillis();
    final String timeString = toEllapsedTime(startTime, endTime);
    Logs.debug(clazz, message + "\t" + timeString);
    return endTime;
  }

  static long debugEllapsedTime(final Object object, final String message, final long startTime) {
    final long endTime = System.currentTimeMillis();
    final String timeString = toEllapsedTime(startTime, endTime);
    Logs.debug(object, message + "\t" + timeString);
    return endTime;
  }

  public static Duration durationSince(final Instant start) {
    return Duration.between(start, Instant.now());
  }

  static boolean equalsNotNull(final Object date1, final Object date2) {
    return ((Date)date1).compareTo((Date)date2) == 0;
  }

  static String format(final DateFormat format, final Date date) {
    return format.format(date);
  }

  static String format(final int dateStyle, final int timeStyle, final Timestamp timestamp) {
    final DateFormat format = DateFormat.getDateTimeInstance(dateStyle, timeStyle);
    return format(format, timestamp);
  }

  static String format(final String pattern) {
    return format(pattern, new Date(System.currentTimeMillis()));
  }

  static String format(final String pattern, final Calendar calendar) {
    if (calendar == null) {
      return null;
    } else {
      final Date date = calendar.getTime();
      return format(pattern, date);
    }
  }

  static String format(final String pattern, final Date date) {
    if (date == null) {
      return null;
    } else {
      final DateFormat format = new SimpleDateFormat(pattern);
      return format(format, date);
    }
  }

  static String format(final String pattern, final TemporalAccessor date) {
    if (date == null) {
      return null;
    } else {
      return DateTimeFormatter.ofPattern(pattern)
        .format(date);
    }
  }

  static String formatIsoDate(final Instant timestamp) {
    // TODO Auto-generated method stub
    return null;
  }

  static Calendar getCalendar(final String dateString) {
    if (dateString != null) {
      final Matcher matcher = DATE_TIME_NANOS_PATTERN.matcher(dateString);
      if (matcher.find()) {
        final int year = getInteger(matcher, 1, 0);
        final int month = getInteger(matcher, 2, 0) - 1;
        final int day = getInteger(matcher, 3, 0);
        final int hour = getInteger(matcher, 4, 0);
        final int minute = getInteger(matcher, 5, 0);
        final int second = getInteger(matcher, 6, 0);
        int millisecond = getInteger(matcher, 7, 0);
        final Calendar calendar = new GregorianCalendar(year, month, day, hour, minute, second);
        if (millisecond != 0) {
          BigDecimal number = new BigDecimal("0." + millisecond);
          number = number.multiply(BigDecimal.valueOf(100))
            .setScale(0, RoundingMode.HALF_DOWN);
          millisecond = number.intValue();
          calendar.set(Calendar.MILLISECOND, millisecond);
        }
        return calendar;
      }
      return null;
    } else {
      return null;
    }
  }

  static Date getDate() {
    return new Date(System.currentTimeMillis());
  }

  static Date getDate(final DateFormat format, final String dateString) {
    if (dateString == null) {
      return null;
    } else {
      try {
        return format.parse(dateString);
      } catch (final ParseException e) {
        if (format instanceof SimpleDateFormat) {
          final SimpleDateFormat simpleFormat = (SimpleDateFormat)format;
          throw new IllegalArgumentException("Invalid date '" + dateString
            + "'. Must match pattern '" + simpleFormat.toPattern() + "'.", e);
        } else {
          throw new IllegalArgumentException("Invalid date  '" + dateString + "'.", e);
        }
      }
    }
  }

  static java.util.Date getDate(final Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof java.util.Date) {
      final java.util.Date date = (java.util.Date)value;
      return date;
    } else if (value instanceof Instant) {
      final Instant instant = (Instant)value;
      return java.util.Date.from(instant);
    } else if (value instanceof Calendar) {
      final Calendar calendar = (Calendar)value;
      final long timeInMillis = calendar.getTimeInMillis();
      return new java.util.Date(timeInMillis);
    } else {
      final String string = DataTypes.toString(value);
      return getDate(string);
    }
  }

  static Date getDate(final String dateString) {
    if (dateString != null) {
      final Matcher matcher = DATE_TIME_NANOS_PATTERN.matcher(dateString);
      if (matcher.find()) {
        final int year = getInteger(matcher, 1, 0);
        final int month = getInteger(matcher, 2, 0) - 1;
        final int day = getInteger(matcher, 3, 0);
        final int hour = getInteger(matcher, 4, 0);
        final int minute = getInteger(matcher, 5, 0);
        final int second = getInteger(matcher, 6, 0);
        int millisecond = getInteger(matcher, 7, 0);
        final Calendar calendar = new GregorianCalendar(year, month, day, hour, minute, second);
        if (millisecond != 0) {
          BigDecimal number = new BigDecimal("0." + millisecond);
          number = number.multiply(BigDecimal.valueOf(1000))
            .setScale(0, RoundingMode.HALF_DOWN);
          millisecond = number.intValue();
          calendar.set(Calendar.MILLISECOND, millisecond);
        }
        return calendar.getTime();
      }
      throw new IllegalArgumentException("Value '" + dateString
        + "' is not a valid date-time, expecting 'yyyy-MM-dd HH:mm:ss.SSS'.");
    } else {
      return null;
    }
  }

  static Date getDate(final String pattern, final String dateString) {
    final DateFormat format = new SimpleDateFormat(pattern);
    return getDate(format, dateString);
  }

  static Instant getInstant(final Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Instant) {
      final Instant date = (Instant)value;
      return date;
    } else if (value instanceof OffsetDateTime) {
      final OffsetDateTime date = (OffsetDateTime)value;
      return date.toInstant();
    } else if (value instanceof java.sql.Date) {
      final java.sql.Date date = (java.sql.Date)value;
      final LocalDate localDate = date.toLocalDate();
      return getInstant(localDate);
    } else if (value instanceof Date) {
      final Date date = (Date)value;
      return date.toInstant();
    } else if (value instanceof Calendar) {
      final Calendar calendar = (Calendar)value;
      return calendar.toInstant();
    } else if (value instanceof LocalDate) {
      final LocalDate date = (LocalDate)value;
      final ZoneId zoneId = ZoneId.systemDefault();
      return date.atStartOfDay(zoneId)
        .toInstant();
    } else if (value instanceof TemporalAccessor) {
      final TemporalAccessor temporal = (TemporalAccessor)value;
      return Instant.from(temporal);
    } else {
      final String string = value.toString();
      if (string.charAt(4) == '-') {
        try {
          return INSTANT_PARSER.parse(string, Instant::from);
        } catch (final Exception e2) {
          try {
            return INSTANT_PARSER_UTC.parse(string, Instant::from);
          } catch (final Exception e3) {
            final var localDate = getLocalDate(string);
            if (localDate != null) {
              return getInstant(localDate);
            } else {
              throw e3;
            }
          }
        }
      } else {
        return DateTimeFormatter.RFC_1123_DATE_TIME.parse(string, Instant::from);
      }
    }
  }

  static int getInteger(final Matcher matcher, final int groupIndex, final int defaultValue) {
    final String group = matcher.group(groupIndex);
    if (group != null) {
      return Integer.parseInt(group);
    } else {
      return defaultValue;
    }
  }

  static Calendar getIsoCalendar(String dateString) {
    if (dateString != null) {
      dateString = dateString.strip();
      final int length = dateString.length();

      if (length < 4) {
        throw new IllegalArgumentException(dateString + " is not a valid ISO 8601 date");
      } else {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");

        final int year = Integer.valueOf(dateString.substring(0, 4));
        int month = 0;
        int day = 1;
        int hour = 1;
        int minute = 1;
        int second = 0;
        int millis = 0;
        if (length >= 7) {
          month = Integer.valueOf(dateString.substring(5, 7)) - 1;
          if (length >= 10) {
            day = Integer.valueOf(dateString.substring(8, 10));
            if (length >= 13) {
              hour = Integer.valueOf(dateString.substring(11, 13));
              if (length >= 16) {
                minute = Integer.valueOf(dateString.substring(14, 16));
                if (length >= 19) {
                  second = Integer.valueOf(dateString.substring(17, 19));
                }
              }

              if (length > 19) {
                int tzIndex = 19;
                if (dateString.charAt(tzIndex) == '.') {
                  final int millisIndex = 20;
                  tzIndex = 20;
                  while (tzIndex < length && Character.isDigit(dateString.charAt(tzIndex))) {
                    tzIndex++;
                  }
                  if (millisIndex != tzIndex) {
                    final String millisString = dateString.substring(millisIndex, tzIndex);
                    millis = Integer.valueOf(millisString);
                    if (millisString.length() == 1) {
                      millis = millis * 100;
                    } else if (millisString.length() == 2) {
                      millis = millis * 10;
                    }
                  }

                }
                if (tzIndex < length) {
                  final char tzChar = dateString.charAt(tzIndex);
                  if (tzChar == 'Z') {
                  } else if (tzChar == '+' || tzChar == '-') {
                    if (tzIndex + 5 < length) {
                      final String tzString = dateString.substring(tzIndex, tzIndex + 6);
                      timeZone = TimeZone.getTimeZone("GMT" + tzString);
                    }
                  }
                }
              }

            }
          }

        }
        final Calendar calendar = new GregorianCalendar(timeZone);
        calendar.clear();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, day);
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minute);
        calendar.set(Calendar.SECOND, second);
        calendar.set(Calendar.MILLISECOND, millis);

        return calendar;
      }
    } else {
      return null;
    }
  }

  static Date getIsoDate(final String dateString) {
    final Calendar calendar = getIsoCalendar(dateString);
    return calendar.getTime();
  }

  static java.sql.Date getIsoSqlDate(final String dateString) {
    final Calendar calendar = getIsoCalendar(dateString);
    final long time = calendar.getTimeInMillis();
    return new java.sql.Date(time);
  }

  static Timestamp getIsoTimestamp(final String dateString) {
    final Calendar calendar = getIsoCalendar(dateString);
    final long time = calendar.getTimeInMillis();
    return new Timestamp(time);
  }

  static LocalDate getLocalDate(final Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof LocalDate) {
      return (LocalDate)value;
    } else if (value instanceof final Instant instant) {
      return instant.atZone(UTC)
        .toLocalDate();
    } else if (value instanceof final java.sql.Date date) {
      return date.toLocalDate();
    } else if (value instanceof final Date date) {
      return getLocalDate(date.toInstant());
    } else if (value instanceof final Calendar calendar) {
      return getLocalDate(calendar.toInstant());
    } else if (value instanceof final TemporalAccessor temporal) {
      return LocalDate.from(temporal);
    } else {
      var s = value.toString();
      if (s.length() > 10) {
        s = s.substring(0, 10);
      }
      return LocalDate.parse(s);
    }
  }

  static java.sql.Date getSqlDate() {
    return new java.sql.Date(System.currentTimeMillis());
  }

  static java.sql.Date getSqlDate(final Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof java.sql.Date) {
      final java.sql.Date date = (java.sql.Date)value;
      return date;
    } else if (value instanceof Instant) {
      final Instant instant = (Instant)value;
      final LocalDate date = instant.atZone(UTC)
        .toLocalDate();
      return getSqlDate(date);
    } else if (value instanceof LocalDate) {
      final LocalDate date = (LocalDate)value;
      return java.sql.Date.valueOf(date);
    } else if (value instanceof Date) {
      final Date date = (Date)value;
      return new java.sql.Date(date.getTime());
    } else if (value instanceof Calendar) {
      final Calendar calendar = (Calendar)value;
      final long timeInMillis = calendar.getTimeInMillis();
      return new java.sql.Date(timeInMillis);
    } else {
      return getSqlDate(value.toString());
    }
  }

  static java.sql.Date getSqlDate(final String dateString) {
    if (dateString != null) {
      final Matcher matcher = DATE_TIME_NANOS_PATTERN.matcher(dateString);
      if (matcher.find()) {
        final int year = getInteger(matcher, 1, 0);
        final int month = getInteger(matcher, 2, 0) - 1;
        final int day = getInteger(matcher, 3, 0);
        int millisecond = getInteger(matcher, 7, 0);
        final Calendar calendar = new GregorianCalendar(year, month, day);
        if (millisecond != 0) {
          BigDecimal number = new BigDecimal("0." + millisecond);
          number = number.multiply(BigDecimal.valueOf(1000))
            .setScale(0, RoundingMode.HALF_DOWN);
          millisecond = number.intValue();
          calendar.set(Calendar.MILLISECOND, millisecond);
        }
        final long timeInMillis = calendar.getTimeInMillis();
        return new java.sql.Date(timeInMillis);
      }
      throw new IllegalArgumentException("Value '" + dateString
        + "' is not a valid date-time, expecting 'yyyy-MM-dd HH:mm:ss.SSS'.");
    } else {
      return null;
    }
  }

  static java.sql.Date getSqlDate(final String pattern, final String dateString) {
    final Date date = getDate(pattern, dateString);
    if (date == null) {
      return null;
    } else {
      final long time = date.getTime();
      return new java.sql.Date(time);
    }
  }

  static Timestamp getTimestamp() {
    return new Timestamp(System.currentTimeMillis());
  }

  static Timestamp getTimestamp(final Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Timestamp) {
      final Timestamp date = (Timestamp)value;
      return date;
    } else if (value instanceof Instant) {
      final Instant instant = (Instant)value;
      return Timestamp.from(instant);
    } else if (value instanceof Date) {
      final Date date = (Date)value;
      final long time = date.getTime();
      return new Timestamp(time);
    } else if (value instanceof Calendar) {
      final Calendar calendar = (Calendar)value;
      final long timeInMillis = calendar.getTimeInMillis();
      return new Timestamp(timeInMillis);
    } else if (value instanceof TemporalAccessor) {
      final TemporalAccessor temporal = (TemporalAccessor)value;
      final long instantSecs = temporal.getLong(INSTANT_SECONDS);
      final int nanoOfSecond = temporal.get(NANO_OF_SECOND);
      final long timeInMillis = instantSecs * 1000 + nanoOfSecond / 1000;
      return new Timestamp(timeInMillis);
    } else {
      return getTimestamp(value.toString());
    }
  }

  static Timestamp getTimestamp(final String dateString) {
    if (dateString != null) {
      final Instant instant = Instant.parse(dateString);
      return Timestamp.from(instant);
    } else {
      return null;
    }
  }

  static Timestamp getTimestamp(final String pattern, final String dateString) {

    final Date date = getDate(pattern, dateString);
    if (date == null) {
      return null;
    } else {
      final long time = date.getTime();
      return new Timestamp(time);
    }
  }

  static int getYear() {
    final Calendar calendar = Calendar.getInstance();
    return calendar.get(Calendar.YEAR);
  }

  static long infoEllapsedTime(final Object object, final String message, final long startTime) {
    final long endTime = System.currentTimeMillis();
    final String timeString = toEllapsedTime(startTime, endTime);
    Logs.info(object, message + " " + timeString);
    return endTime;
  }

  private static DateTimeFormatter newRfc1123() {
    // manually code maps to ensure correct data always used
    // (locale data can be changed by application code)
    final Map<Long, String> dow = new HashMap<>();
    dow.put(1L, "Mon");
    dow.put(2L, "Tue");
    dow.put(3L, "Wed");
    dow.put(4L, "Thu");
    dow.put(5L, "Fri");
    dow.put(6L, "Sat");
    dow.put(7L, "Sun");
    final Map<Long, String> moy = new HashMap<>();
    moy.put(1L, "Jan");
    moy.put(2L, "Feb");
    moy.put(3L, "Mar");
    moy.put(4L, "Apr");
    moy.put(5L, "May");
    moy.put(6L, "Jun");
    moy.put(7L, "Jul");
    moy.put(8L, "Aug");
    moy.put(9L, "Sep");
    moy.put(10L, "Oct");
    moy.put(11L, "Nov");
    moy.put(12L, "Dec");
    return new DateTimeFormatterBuilder().parseCaseInsensitive()
      .parseLenient()
      .optionalStart()
      .appendText(DAY_OF_WEEK, dow)
      .appendLiteral(", ")
      .optionalEnd()
      .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
      .appendLiteral(' ')
      .appendText(MONTH_OF_YEAR, moy)
      .appendLiteral(' ')
      .appendValue(YEAR, 4) // 2 digit year not handled
      .appendLiteral(' ')
      .appendValue(HOUR_OF_DAY, 2)
      .appendLiteral(':')
      .appendValue(MINUTE_OF_HOUR, 2)
      .optionalStart()
      .appendLiteral(':')
      .appendValue(SECOND_OF_MINUTE, 2)
      .optionalEnd()
      .appendLiteral(' ')
      .appendOffset("+HHMM", "GMT") // should handle
                                    // UT/Z/EST/EDT/CST/CDT/MST/MDT/PST/MDT
      .toFormatter(Locale.getDefault(Locale.Category.FORMAT));
  }

  static long printEllapsedTime(final long startTime) {
    final long endTime = System.currentTimeMillis();
    System.out.println(toEllapsedTime(startTime, endTime));
    return endTime;
  }

  static long printEllapsedTime(final String message, final long startTime) {
    final long endTime = System.currentTimeMillis();
    System.out.println(message + "\t" + toEllapsedTime(startTime, endTime));
    return endTime;
  }

  public static Timer timer() {
    return new Timer();
  }

  @SuppressWarnings("deprecation")
  static String toDateTimeIsoString(final Date date) {
    if (date == null) {
      return null;
    } else {
      final StringBuilder string = new StringBuilder(23);
      int year = date.getYear() + 1900;
      if (year < 0) {
        string.append('-');
        year = -year;
      }
      if (year < 1000) {
        string.append('0');
        if (year < 100) {
          string.append('0');
        }
        if (year < 10) {
          string.append('0');
        }
      }
      string.append(year);

      string.append('-');

      final int month = date.getMonth() + 1;
      if (month < 10) {
        string.append('0');
      }
      string.append(month);

      string.append('-');

      final int day = date.getDate();
      if (day < 10) {
        string.append('0');
      }
      string.append(day);

      if (date instanceof java.sql.Date) {
        string.append("T00:00:00");
      } else {

        string.append('T');

        final int hour = date.getHours();

        if (hour < 10) {
          string.append('0');
        }
        string.append(hour);

        string.append(':');

        final int minutes = date.getMinutes();
        if (minutes < 10) {
          string.append('0');
        }
        string.append(minutes);

        string.append(':');

        final int seconds = date.getSeconds();
        if (seconds < 10) {
          string.append('0');
        }
        string.append(seconds);

        string.append('.');
        final int milliseconds = (int)(date.getTime() % 1000);
        if (milliseconds == 0) {
          string.append('0');
        } else {
          String millisecondsString = Integer.toString(milliseconds);

          // Add leading zeros
          millisecondsString = "000".substring(0, 3 - millisecondsString.length())
            + millisecondsString;

          // Truncate trailing zeros
          final char[] nanosChar = new char[millisecondsString.length()];
          millisecondsString.getChars(0, millisecondsString.length(), nanosChar, 0);
          int truncIndex = 2;
          while (nanosChar[truncIndex] == '0') {
            truncIndex--;
          }
          string.append(millisecondsString, 0, truncIndex + 1);
        }

      }
      return string.toString();
    }
  }

  static String toDateTimeIsoString(final Object value) {
    if (value == null) {
      return null;
    } else {
      final Date date = getDate(value);
      return toDateTimeIsoString(date);
    }
  }

  static String toEllapsedTime(final long time) {
    final StringBuilder string = new StringBuilder();
    final long totalSeconds = Math.floorDiv(time, 1000);
    final long days = Math.floorDiv(totalSeconds, 24 * 60 * 60);
    if (days > 0) {
      string.append(days);
      string.append(' ');
    }
    final long hours = Math.floorDiv(totalSeconds, 60 * 60);
    if (hours > 0) {
      if (hours < 10 && string.length() > 0) {
        string.append('0');
      }
      string.append(hours);
      string.append(':');
    }
    final long minutes = Math.floorDiv(totalSeconds, 60) % 60;
    if (minutes > 0) {
      if (minutes < 10 && string.length() > 0) {
        string.append('0');
      }
      string.append(minutes);
      string.append(':');
    }
    final long seconds = totalSeconds % 60;
    if (seconds < 10 && string.length() > 0) {
      string.append('0');
    }
    string.append(seconds);
    final long milliSeconds = time % 1000;
    if (milliSeconds > 0) {
      string.append('.');
      if (milliSeconds < 10) {
        string.append('0');
      }
      if (milliSeconds < 100) {
        string.append('0');
      }
      string.append(milliSeconds);
    }
    return string.toString();
  }

  static String toEllapsedTime(final long startTime, final long endTime) {
    return toEllapsedTime(endTime - startTime);
  }

  static String toInstantIsoString(final Object value) {
    if (value == null) {
      return null;
    } else {
      final Instant instant = getInstant(value);
      return instant.toString();
    }
  }

  static String toLocalDateIsoString(final Object value) {
    if (value == null) {
      return null;
    } else {
      final LocalDate date = getLocalDate(value);
      return date.toString();
    }
  }

  @SuppressWarnings("deprecation")
  static String toSqlDateString(final Date date) {
    if (date == null) {
      return null;
    } else {
      final StringBuilder string = new StringBuilder(10);
      int year = date.getYear() + 1900;
      if (year < 0) {
        string.append('-');
        year = -year;
      }
      if (year < 1000) {
        string.append('0');
        if (year < 100) {
          string.append('0');
        }
        if (year < 10) {
          string.append('0');
        }
      }
      string.append(year);

      string.append('-');

      final int month = date.getMonth() + 1;
      if (month < 10) {
        string.append('0');
      }
      string.append(month);

      string.append('-');

      final int day = date.getDate();
      if (day < 10) {
        string.append('0');
      }
      string.append(day);

      return string.toString();
    }
  }

  static String toSqlDateString(final Object value) {
    if (value == null) {
      return null;
    } else {
      final java.sql.Date date = getSqlDate(value);
      return toSqlDateString(date);
    }
  }

  static String toTimestampIsoString(final Object value) {
    if (value == null) {
      return null;
    } else {
      final Timestamp timestamp = getTimestamp(value);
      return toTimestampIsoString(timestamp);
    }
  }

  static String toTimestampIsoString(final Timestamp date) {
    if (date == null) {
      return null;
    } else {
      return DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
    }
  }
}
