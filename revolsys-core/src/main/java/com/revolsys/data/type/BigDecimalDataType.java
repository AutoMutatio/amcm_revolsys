package com.revolsys.data.type;

import java.math.BigDecimal;

public class BigDecimalDataType extends AbstractDataType {

  public BigDecimalDataType() {
    super("decimal", BigDecimal.class, false);
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2) {
    return super.equalsNotNull(value1, value2);
  }

  @Override
  protected Object toObjectDo(final Object value) {
    final String string = DataTypes.toString(value).replaceAll(",", "");
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
