package com.revolsys.record.io.format.json;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Function;

public class MultiToJsonProcessor<V> implements JsonProcessor {

  private JsonProcessor currentProcessor;

  private int currentProcessorDepth = -1;

  private final Deque<Integer> processorDepths = new ArrayDeque<>(64);

  private final Deque<JsonProcessor> processors = new ArrayDeque<>(64);

  public MultiToJsonProcessor(final Function<MultiToJsonProcessor<V>, JsonProcessor> constructor) {
    final JsonProcessor processor = constructor.apply(this);
    processorPush(-1, processor);
  }

  @Override
  public void beforeArrayValue(final JsonStatus status) {
    this.currentProcessor.beforeArrayValue(status);
  }

  @Override
  public void endArray(final JsonStatus status) {
    this.currentProcessor.endArray(status);
    endDo(status);
  }

  protected void endDo(final JsonStatus status) {
    if (status.getDepth() <= this.currentProcessorDepth) {
      this.currentProcessor.after();
      this.processors.removeLast();
      this.processorDepths.removeLast();
      this.currentProcessor = this.processors.peekLast();
      this.currentProcessorDepth = this.processorDepths.peekLast();
    }
  }

  @Override
  public void endDocument(final JsonStatus status) {
    this.currentProcessor.endDocument(status);
    endDo(status);
  }

  @Override
  public void endObject(final JsonStatus status) {
    this.currentProcessor.endObject(status);
    endDo(status);
  }

  @Override
  public void label(final JsonStatus status, final String label) {
    this.currentProcessor.label(status, label);
  }

  @Override
  public void nullValue(final JsonStatus status) {
    this.currentProcessor.nullValue(status);
  }

  @Override
  public void onCancel() {
    this.currentProcessor.onCancel();
  }

  @Override
  public void onComplete() {
    this.currentProcessor.onComplete();
  }

  private void processorPush(final int depth, final JsonProcessor processor) {
    processor.before();
    this.currentProcessor = processor;
    this.currentProcessorDepth = depth;
    this.processors.addLast(processor);
    this.processorDepths.addLast(depth);
  }

  public void processorPush(final JsonStatus status, final JsonProcessor processor) {
    final int depth = status.getDepth();
    processorPush(depth, processor);
  }

  @Override
  public void startArray(final JsonStatus status) {
    this.currentProcessor.startArray(status);
  }

  @Override
  public void startDocument(final JsonStatus status) {
    this.currentProcessor.startDocument(status);
  }

  @Override
  public void startObject(final JsonStatus status) {
    this.currentProcessor.startObject(status);
  }

  @Override
  public void value(final JsonStatus status, final BigDecimal value) {
    this.currentProcessor.value(status, value);
  }

  @Override
  public void value(final JsonStatus status, final boolean value) {
    this.currentProcessor.value(status, value);
  }

  @Override
  public void value(final JsonStatus status, final String value) {
    this.currentProcessor.value(status, value);
  }
}
