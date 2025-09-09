package com.revolsys.data.type;

import com.revolsys.number.Doubles;

public class DoubleDataType extends AbstractDataType {

  public DoubleDataType() {
    super("double", Double.class, false);
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2) {
    final double number1 = (double)value1;
    final double number2 = (double)value2;
    if (Double.isNaN(number1)) {
      return Double.isNaN(number2);
    } else if (Double.isInfinite(number1)) {
      return Double.isInfinite(number2);
    } else {
      if (Double.compare(number1, number2) == 0) {
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getMaxValue() {
    final Double max = Double.MAX_VALUE;
    return (V)max;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getMinValue() {
    final Double min = -Double.MAX_VALUE;
    return (V)min;
  }

  @Override
  protected Object toObjectDo(final Object value) {
    if (value instanceof Number) {
      final Number number = (Number)value;
      return number.doubleValue();
    } else {
      final String string = DataTypes.toString(value);
      if (string == null || string.length() == 0) {
        return null;
      } else {
        return Double.valueOf(string);
      }
    }
  }

  @Override
  protected String toStringDo(final Object value) {
    return Doubles.toString((double)value);
  }
}
