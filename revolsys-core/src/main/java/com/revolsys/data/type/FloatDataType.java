package com.revolsys.data.type;

import com.revolsys.number.Floats;

public class FloatDataType extends AbstractDataType {

  public FloatDataType() {
    super("float", Float.class, false);
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2) {
    final float number1 = (float)value1;
    final float number2 = (float)value2;
    if (Float.isNaN(number1)) {
      return Float.isNaN(number2);
    } else if (Float.isInfinite(number1)) {
      return Float.isInfinite(number2);
    } else {
      if (Float.compare(number1, number2) == 0) {
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getMaxValue() {
    final Float max = Float.MAX_VALUE;
    return (V)max;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getMinValue() {
    final Float min = -Float.MAX_VALUE;
    return (V)min;
  }

  @Override
  public boolean isMathSupported() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V plus(final Object value1, final Number value2) {
    final float number1 = toObject(value1);
    return (V)(Object)(number1 + value2.floatValue());
  }

  @Override
  protected Object toObjectDo(final Object value) {
    if (value instanceof Number) {
      final Number number = (Number)value;
      return number.floatValue();
    } else {
      final String string = DataTypes.toString(value);
      if (string == null || string.length() == 0) {
        return null;
      } else {
        return Float.valueOf(string);
      }
    }
  }

  @Override
  protected String toStringDo(final Object value) {
    return Floats.toString((float)value);
  }
}
