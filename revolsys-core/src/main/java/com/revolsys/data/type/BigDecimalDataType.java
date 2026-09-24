package com.revolsys.data.type;

import java.math.BigDecimal;

public class BigDecimalDataType extends AbstractDataType {

  public BigDecimalDataType() {
    super("decimal", BigDecimal.class, false);
  }

  @Override
  public boolean isMathSupported() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V plus(final Object value1, final Number value2) {
    final BigDecimal number1 = toObject(value1);
    final BigDecimal number2 = toObject(value2);
    return (V)number1.add(number2);
  }

  @Override
  protected Object toObjectDo(final Object value) {
    final String string = DataTypes.toString(value)
      .replaceAll(",", "");
    if ("-".equals(string) || "+".equals(string)) {
      return BigDecimal.ZERO;
    }
    final BigDecimal decimal = new BigDecimal(string).stripTrailingZeros();
    if (decimal.scale() < 0) {
      return new BigDecimal(decimal.toPlainString());
    } else {
      return decimal;
    }
  }

  @Override
  protected String toStringDo(final Object value) {
    return ((BigDecimal)value).toPlainString();
  }
}
