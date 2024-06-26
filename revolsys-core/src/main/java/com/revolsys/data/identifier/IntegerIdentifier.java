package com.revolsys.data.identifier;

import java.util.Collections;
import java.util.List;

import com.revolsys.number.Integers;
import com.revolsys.number.Longs;

public class IntegerIdentifier extends Number implements Identifier, Comparable<Object> {
  private static final long serialVersionUID = 1L;

  private final int value;

  IntegerIdentifier(final int value) {
    this.value = value;
  }

  @Override
  public int compareTo(final Identifier identifier2) {
    if (identifier2 == this) {
      return 0;
    } else if (identifier2 == null) {
      return -1;
    } else if (identifier2 instanceof Number) {
      final Number number2 = (Number)identifier2;
      return Long.compare(this.value, number2.longValue());
    } else if (identifier2.isSingle()) {
      final Object value2 = identifier2.getValue(0);
      return compareTo(value2);
    } else {
      return -1;
    }
  }

  @Override
  public int compareTo(final Object object) {
    if (object == null) {
      return -1;
    } else if (object instanceof Number) {
      final Number number = (Number)object;
      final long longValue = number.intValue();
      return Long.compare(this.value, longValue);
    } else if (object instanceof Identifier) {
      final Identifier identifier = (Identifier)object;
      return compareTo(identifier);
    } else {
      return compareToObject(object);
    }
  }

  private int compareToObject(final Object object) {
    try {
      final Long longValue = Longs.toValid(object);
      if (longValue == null) {
        return -1;
      } else {
        return Long.compare(this.value, longValue);
      }
    } catch (final Exception e) {
      final String string = Integer.toString(this.value);
      final String string2 = object.toString();
      return string.compareTo(string2);
    }
  }

  @Override
  public double doubleValue() {
    return this.value;
  }

  @Override
  public boolean equals(final Identifier identifier) {
    if (identifier != null && identifier.isSingle()) {
      final Object otherValue = identifier.getValue(0);
      return Integers.equals(this.value, otherValue);
    } else {
      return false;
    }
  }

  @Override
  public boolean equals(final Object other) {
    if (other instanceof Identifier) {
      final Identifier identifier = (Identifier)other;
      return equals(identifier);
    } else if (other instanceof Number) {
      final Number number = (Number)other;
      return this.value == number.intValue();
    } else {
      return Integers.equals(this.value, other);
    }
  }

  @Override
  public float floatValue() {
    return this.value;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V getValue(final int index) {
    if (index == 0) {
      return (V)Integer.valueOf(this.value);
    } else {
      return null;
    }
  }

  @Override
  public List<Object> getValues() {
    return Collections.singletonList((Object)this.value);
  }

  @Override
  public int hashCode() {
    return this.value;
  }

  @Override
  public int intValue() {
    return this.value;
  }

  @Override
  public long longValue() {
    return this.value;
  }

  @Override
  public String toString() {
    return Integer.toString(this.value);
  }
}
