package com.revolsys.comparator;

import java.math.BigDecimal;
import java.util.Comparator;

import com.revolsys.number.BigDecimals;
import com.revolsys.number.Numbers;

public class StringNumberComparator implements Comparator<String> {
  @Override
  public int compare(final String string1, final String string2) {
    if (Numbers.isNumber(string1) && Numbers.isNumber(string2)) {
      final BigDecimal number1 = BigDecimals.toValid(string1);
      final BigDecimal number2 = BigDecimals.toValid(string2);
      return number1.compareTo(number2);
    } else {
      return string1.compareTo(string2);
    }
  }
}
