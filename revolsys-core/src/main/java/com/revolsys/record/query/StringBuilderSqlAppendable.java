package com.revolsys.record.query;

public class StringBuilderSqlAppendable extends SqlAppendable implements CharSequence {
  private final StringBuilder stringBuilder;

  public StringBuilderSqlAppendable() {
    this(new StringBuilder());
  }

  StringBuilderSqlAppendable(final StringBuilder stringBuilder) {
    super(stringBuilder);
    this.stringBuilder = stringBuilder;
  }

  @Override
  public StringBuilderSqlAppendable append(final CharSequence chars) {
    this.stringBuilder.append(chars);
    return this;
  }

  @Override
  public char charAt(final int index) {
    return this.stringBuilder.charAt(index);
  }

  @Override
  public int length() {
    return this.stringBuilder.length();
  }

  @Override
  public CharSequence subSequence(final int start, final int end) {
    return this.stringBuilder.subSequence(start, end);
  }

  public String toSqlString() {
    return this.stringBuilder.toString();
  }

  @Override
  public String toString() {
    return this.stringBuilder.toString();
  }
}
