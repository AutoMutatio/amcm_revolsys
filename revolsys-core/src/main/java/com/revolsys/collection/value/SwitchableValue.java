package com.revolsys.collection.value;

import java.util.function.Consumer;

public class SwitchableValue<T> implements ValueHolder<T> {

  private T value;

  private T previousValue;

  public SwitchableValue(final ValueHolder<T> valueHolder, final T previousValue, final T value) {
    this.value = value;
    this.previousValue = previousValue;
  }

  public T getPreviousValue() {
    return this.previousValue;
  }

  @Override
  public T getValue() {
    return this.value;
  }

  @Override
  public T setValue(final T value) {
    throw new UnsupportedOperationException("Cannot change the values, just swap");
  }

  public SwitchableValue<T> swapValue() {
    final T t = this.value;
    this.value = this.previousValue;
    this.previousValue = t;
    return this;
  }

  public SwitchableValue<T> withValue(final Consumer<T> action) {
    action.accept(this.value);
    return this;
  }
}
