package com.revolsys.collection.json;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Collection;

import com.revolsys.exception.Exceptions;

public class JsonBigDecimal extends BigDecimal implements JsonType {

  public JsonBigDecimal(final BigInteger val) {
    super(val);
  }

  public JsonBigDecimal(final BigInteger unscaledVal, final int scale) {
    super(unscaledVal, scale);
  }

  public JsonBigDecimal(final BigInteger unscaledVal, final int scale, final MathContext mc) {
    super(unscaledVal, scale, mc);
  }

  public JsonBigDecimal(final BigInteger val, final MathContext mc) {
    super(val, mc);
  }

  public JsonBigDecimal(final char[] in) {
    super(in);
  }

  public JsonBigDecimal(final char[] in, final int offset, final int len) {
    super(in, offset, len);
  }

  public JsonBigDecimal(final char[] in, final int offset, final int len, final MathContext mc) {
    super(in, offset, len, mc);
  }

  public JsonBigDecimal(final char[] in, final MathContext mc) {
    super(in, mc);
  }

  public JsonBigDecimal(final double val) {
    super(val);
  }

  public JsonBigDecimal(final double val, final MathContext mc) {
    super(val, mc);
  }

  public JsonBigDecimal(final int val) {
    super(val);
  }

  public JsonBigDecimal(final int val, final MathContext mc) {
    super(val, mc);
  }

  public JsonBigDecimal(final long val) {
    super(val);
  }

  public JsonBigDecimal(final long val, final MathContext mc) {
    super(val, mc);
  }

  public JsonBigDecimal(final String val) {
    super(val);
  }

  public JsonBigDecimal(final String val, final MathContext mc) {
    super(val, mc);
  }

  @Override
  public Appendable appendJson(final Appendable appendable) {
    try {
      appendable.append(toPlainString());
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return appendable;
  }

  @Override
  public JsonBigDecimal clone() {
    try {
      return (JsonBigDecimal)super.clone();
    } catch (final CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean equals(final Object object,
    final Collection<? extends CharSequence> excludeFieldNames) {
    return super.equals(object);
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean removeEmptyProperties() {
    return false;
  }

}
