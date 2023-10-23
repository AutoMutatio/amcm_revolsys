package com.revolsys.util;

import com.revolsys.data.type.DataTypes;

public interface StringBuilders {

  static void append(final StringBuilder string, final Iterable<? extends Object> values) {
    append(string, values, ",");
  }

  static void append(final StringBuilder buffer, final Iterable<? extends Object> values,
    final boolean skipNulls, final String separator) {
    boolean first = true;
    for (final Object value : values) {
      final String string = DataTypes.toString(value);
      if (!skipNulls || Property.hasValue(string)) {
        if (first) {
          first = false;
        } else {
          buffer.append(separator);
        }
        if (string != null) {
          buffer.append(string);
        }
      }
    }
  }

  static void append(final StringBuilder buffer, final Iterable<? extends Object> values,
    final String separator) {
    boolean first = true;
    for (final Object value : values) {
      if (value != null) {
        final String string = DataTypes.toString(value);
        if (Property.hasValue(string)) {
          if (first) {
            first = false;
          } else {
            buffer.append(separator);
          }
          buffer.append(string);
        }
      }
    }
  }

}
