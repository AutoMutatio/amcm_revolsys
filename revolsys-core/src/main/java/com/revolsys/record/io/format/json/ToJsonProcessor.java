package com.revolsys.record.io.format.json;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Deque;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.reactive.chars.ValueProcessor;

public class ToJsonProcessor<V> implements JsonProcessor {
  private final Deque<Object> values = new ArrayDeque<>(64);

  private final ValueProcessor<V> processor;

  public ToJsonProcessor(final ValueProcessor<V> processor) {
    this.processor = processor;
  }

  @Override
  public void after() {
    if (!this.values.isEmpty()) {
      @SuppressWarnings("unchecked")
      final V value = (V)this.values.pop();
      this.processor.process(value);
    }
  }

  @SuppressWarnings("unchecked")
  private void applyValue(final JsonStatus status, final Object value) {
    if (this.values.isEmpty()) {
      this.values.push(value);
    } else {
      final Object parent = this.values.peek();
      if (parent instanceof JsonObject) {
        final JsonObject json = (JsonObject)parent;
        final String label = status.getLabel();
        if (label == null) {
          throw new IllegalStateException("Object requires a label " + value);
        } else {
          json.addValue(label, value);
        }
      } else if (parent instanceof final ListEx<?> list) {
        ((ListEx<Object>)list).add(value);
      } else {
        throw new IllegalStateException("Parent not an object or array: " + parent);
      }
    }
  }

  @Override
  public void before() {
    this.values.clear();
  }

  public void emitValue() {
    if (!this.values.isEmpty()) {
      @SuppressWarnings("unchecked")
      final V value = (V)this.values.pop();
      this.processor.process(value);
    }
  }

  @Override
  public void endArray(final JsonStatus status) {
    @SuppressWarnings("unchecked")
    final ListEx<Object> array = (ListEx<Object>)this.values.pop();
    applyValue(status, array);
  }

  @Override
  public void endDocument(final JsonStatus status) {
    emitValue();
    this.processor.onComplete();
  }

  @Override
  public void endObject(final JsonStatus status) {
    final JsonObject json = (JsonObject)this.values.pop();
    applyValue(status, json);
  }

  @Override
  public void nullValue(final JsonStatus status) {
    applyValue(status, null);
  }

  @Override
  public void onCancel() {
    this.values.clear();
  }

  @Override
  public void onComplete() {
    endDocument(null);
  }

  @Override
  public void startArray(final JsonStatus status) {
    this.values.push(Lists.newArray());
  }

  @Override
  public void startDocument(final JsonStatus status) {
  }

  @Override
  public void startObject(final JsonStatus status) {
    this.values.push(JsonObject.hash());
  }

  @Override
  public void value(final JsonStatus status, final BigDecimal value) {
    applyValue(status, value);
  }

  @Override
  public void value(final JsonStatus status, final boolean value) {
    applyValue(status, value);
  }

  @Override
  public void value(final JsonStatus status, final String value) {
    applyValue(status, value);
  }

}
