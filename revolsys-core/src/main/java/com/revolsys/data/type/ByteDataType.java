package com.revolsys.data.type;

public class ByteDataType extends AbstractDataType {

  public ByteDataType() {
    super("ubyte", Short.class, false);
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2) {
    return (short)value1 == (short)value2;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getMaxValue() {
    final Short max = 255;
    return (V)max;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getMinValue() {
    final Short min = 0;
    return (V)min;
  }

  @Override
  protected Object toObjectDo(final Object value) {
    final String string = DataTypes.toString(value);
    return Short.valueOf(string);
  }

  @Override
  protected String toStringDo(final Object value) {
    return String.valueOf((short)value);
  }
}
