package com.revolsys.record.io.format.json;

import com.revolsys.reactive.chars.ValueProcessor;

import reactor.core.publisher.FluxSink;

public class FluxSinkValueProcessor<V> implements ValueProcessor<V> {

  private final FluxSink<V> sink;

  public FluxSinkValueProcessor(final FluxSink<V> sink) {
    this.sink = sink;
  }

  @Override
  public void onCancel() {
    this.sink.complete();
  }

  @Override
  public void onComplete() {
    this.sink.complete();
  }

  @Override
  public void onError(final Throwable e) {
    this.sink.error(e);
  }

  @Override
  public boolean process(final V value) {
    this.sink.next(value);
    return true;
  }

}
