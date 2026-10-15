package com.revolsys.data.type;

import java.util.Collection;
import java.util.function.Supplier;

public record NamedDataType(String name, DataType dataType) implements DataType {

  @Override
  public int compareNullFirst(final Object object1, final Object object2) {
    return dataType().compareNullFirst(object1, object2);
  }

  @Override
  public boolean equals(final Object object1, final Object object2) {
    return dataType().equals(object1, object2);
  }

  @Override
  public boolean equals(final Object object1, final Object object2,
    final Collection<? extends CharSequence> excludeFieldNames) {
    return dataType().equals(object1, object2, excludeFieldNames);
  }

  @Override
  public boolean equalsExact(final Object value1, final Object value2) {
    return DataType.equalExact(value1, value2);
  }

  @Override
  public Class<?> getJavaClass() {
    return dataType().getJavaClass();
  }

  @Override
  public <V> V getMaxValue() {
    return dataType().getMaxValue();
  }

  @Override
  public <V> V getMinValue() {
    return dataType().getMinValue();
  }

  @Override
  public String getName() {
    return dataType().getName();
  }

  @Override
  public String getValidationName() {
    return dataType().getValidationName();
  }

  @Override
  public boolean isAssignableFrom(final Class<?> valueClass) {
    return dataType().isAssignableFrom(valueClass);
  }

  @Override
  public boolean isAssignableTo(final Class<?> valueClass) {
    return dataType().isAssignableTo(valueClass);
  }

  @Override
  public boolean isInstance(final Object value) {
    return dataType().isInstance(value);
  }

  @Override
  public boolean isMathSupported() {
    return dataType().isMathSupported();
  }

  @Override
  public boolean isNumeric() {
    return dataType().isNumeric();
  }

  @Override
  public boolean isRequiresQuotes() {
    return dataType().isRequiresQuotes();
  }

  @Override
  public boolean isValid(final Object value) {
    return dataType().isValid(value);
  }

  @Override
  public <V> DataTypeValueFactory<V> newFactory(final Supplier<V> supplier) {
    return dataType().newFactory(supplier);
  }

  @Override
  public <V> V plus(final Object value, final Number number) {
    return dataType().plus(value, number);
  }

  @Override
  public <V> V toObject(final Object value) {
    return dataType().toObject(value);
  }

  @Override
  public <V> V toObject(final String value) {
    return dataType().toObject(value);
  }

  @Override
  public String toString(final Object value) {
    return dataType().toString(value);
  }

}
