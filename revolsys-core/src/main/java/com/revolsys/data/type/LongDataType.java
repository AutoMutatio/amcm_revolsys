package com.revolsys.data.type;

public class LongDataType extends AbstractDataType {

  public LongDataType() {
    super("long", Long.class, false);
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2) {
    return (long)value1 == (long)value2;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getMaxValue() {
    final Long max = Long.MAX_VALUE;
    return (V)max;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getMinValue() {
    final Long min = Long.MIN_VALUE;
    return (V)min;
  }

  @Override
  public boolean isMathSupported() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V plus(final Object value1, final Number value2) {
    final long number1 = toObject(value1);
    return (V)(Object)(number1 + value2.longValue());
  }

  @Override
  protected Long toObjectDo(final Object value) {
    if (value instanceof Number) {
      final Number number = (Number)value;
      return number.longValue();
    } else {
      final String string = DataTypes.toString(value);
      return Long.valueOf(string);
    }
  }

  @Override
  protected String toStringDo(final Object value) {
    return String.valueOf((long)value);
  }

}
