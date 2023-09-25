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

  public MultiToJsonProcessor(Function<MultiToJsonProcessor<V>, JsonProcessor> constructor) {
    final JsonProcessor processor = constructor.apply(this);
    processorPush(-1, processor);
  }

  @Override
  public void beforeArrayValue(JsonStatus status) {
    this.currentProcessor.beforeArrayValue(status);
  }

  @Override
  public void endArray(JsonStatus status) {
    this.currentProcessor.endArray(status);
    endDo(status);
  }

  protected void endDo(JsonStatus status) {
    if (status.getDepth() <= this.currentProcessorDepth) {
      this.currentProcessor.after();
      this.processors.removeLast();
      this.processorDepths.removeLast();
      this.currentProcessor = this.processors.peekLast();
      this.currentProcessorDepth = this.processorDepths.peekLast();
    }
  }

  @Override
  public void endDocument(JsonStatus status) {
    this.currentProcessor.endDocument(status);
    endDo(status);
  }

  @Override
  public void endObject(JsonStatus status) {
    this.currentProcessor.endObject(status);
    endDo(status);
  }

  @Override
  public void label(JsonStatus status, String label) {
    this.currentProcessor.label(status, label);
  }

  @Override
  public void nullValue(JsonStatus status) {
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

  @Override
  public void onError(Throwable e) {
    this.currentProcessor.onError(e);
  }

  private void processorPush(int depth, JsonProcessor processor) {
    processor.before();
    this.currentProcessor = processor;
    this.currentProcessorDepth = depth;
    this.processors.addLast(processor);
    this.processorDepths.addLast(depth);
  }

  public void processorPush(JsonStatus status, JsonProcessor processor) {
    final int depth = status.getDepth();
    processorPush(depth, processor);
  }

  @Override
  public void startArray(JsonStatus status) {
    this.currentProcessor.startArray(status);
  }

  @Override
  public void startDocument(JsonStatus status) {
    this.currentProcessor.startDocument(status);
  }

  @Override
  public void startObject(JsonStatus status) {
    this.currentProcessor.startObject(status);
  }

  @Override
  public void value(JsonStatus status, BigDecimal value) {
    this.currentProcessor.value(status, value);
  }

  @Override
  public void value(JsonStatus status, boolean value) {
    this.currentProcessor.value(status, value);
  }

  @Override
  public void value(JsonStatus status, String value) {
    this.currentProcessor.value(status, value);
  }
}
