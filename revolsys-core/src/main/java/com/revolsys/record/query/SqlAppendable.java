package com.revolsys.record.query;

import java.io.IOException;

import com.revolsys.exception.Exceptions;
import com.revolsys.jdbc.io.AbstractJdbcRecordStore;
import com.revolsys.record.schema.RecordStore;

public abstract class SqlAppendable implements Appendable {

  public static StringBuilderSqlAppendable stringBuilder() {
    return new StringBuilderSqlAppendable();

  }

  public static StringBuilderSqlAppendable stringBuilder(final CharSequence chars) {
    final StringBuilderSqlAppendable appendable = new StringBuilderSqlAppendable();
    appendable.append(chars);
    return appendable;

  }

  private final Appendable appendable;

  private RecordStore recordStore;

  private boolean usePlaceholders = true;

  private boolean quoteNames = true;

  protected SqlAppendable(final Appendable appendable) {
    this.appendable = appendable;
  }

  @Override
  public SqlAppendable append(final char c) {
    try {
      this.appendable.append(c);
      return this;
    } catch (final IOException e) {
      throw Exceptions.wrap(e);
    }
  }

  @Override
  public SqlAppendable append(final CharSequence chars) {
    try {
      this.appendable.append(chars);
      return this;
    } catch (final IOException e) {
      throw Exceptions.wrap(e);
    }
  }

  @Override
  public SqlAppendable append(final CharSequence chars, final int start, final int end) {
    try {
      this.appendable.append(chars, start, end);
      return this;
    } catch (final IOException e) {
      throw Exceptions.wrap(e);
    }
  }

  public SqlAppendable append(final Object obj) {
    final String string = String.valueOf(obj);
    return append(string);
  }

  public RecordStore getRecordStore() {
    return this.recordStore;
  }

  public boolean isQuoteNames() {
    return this.quoteNames;
  }

  public boolean isUsePlaceholders() {
    return this.usePlaceholders;
  }

  public int length() {
    throw new UnsupportedOperationException("length not supported");
  }

  public SqlAppendable setRecordStore(final RecordStore recordStore) {
    this.recordStore = recordStore;
    if (recordStore instanceof final AbstractJdbcRecordStore jrs) {
      this.quoteNames = jrs.isQuoteNames();
    }
    return this;
  }

  public SqlAppendable setUsePlaceholders(final boolean usePlaceholders) {
    this.usePlaceholders = usePlaceholders;
    return this;
  }

  @Override
  public String toString() {
    return this.appendable.toString();
  }

}
