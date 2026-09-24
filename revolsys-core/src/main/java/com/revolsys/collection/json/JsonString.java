package com.revolsys.collection.json;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import com.revolsys.exception.Exceptions;

public class JsonString implements CharSequence, JsonType {

  private final String string;

  public JsonString(final String string) {
    this.string = Objects.requireNonNull(string);
  }

  @Override
  public Appendable appendJson(final Appendable appendable) {
    try {
      appendable.append('"');
      JsonWriterUtil.charSequence(appendable, this.string);
      appendable.append('"');
      return appendable;
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @Override
  public char charAt(final int index) {
    return this.string.charAt(index);
  }

  @Override
  public JsonString clone() {
    return this;
  }

  @Override
  public boolean equals(final Object object) {
    if (object instanceof final CharSequence cs) {
      return this.string.equals(cs.toString());
    }
    return false;
  }

  @Override
  public boolean equals(final Object object,
    final Collection<? extends CharSequence> excludeFieldNames) {
    return equals(object);
  }

  @Override
  public int hashCode() {
    return this.string.hashCode();
  }

  @Override
  public boolean isEmpty() {
    return this.string.isEmpty();
  }

  @Override
  public int length() {
    return this.string.length();
  }

  @Override
  public boolean removeEmptyProperties() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public CharSequence subSequence(final int start, final int end) {
    return this.string.subSequence(start, end);
  }

  @Override
  public String toString() {
    return this.string;
  }
}
