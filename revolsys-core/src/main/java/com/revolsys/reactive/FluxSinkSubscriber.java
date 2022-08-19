package com.revolsys.reactive;

import org.reactivestreams.Subscription;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.FluxSink;
import reactor.util.context.Context;

public class FluxSinkSubscriber<IN, OUT> extends BaseSubscriber<IN> {

  protected final FluxSink<OUT> sink;

  public FluxSinkSubscriber(final FluxSink<OUT> sink) {
    this.sink = sink;
  }

  @Override
  public Context currentContext() {
    return Context.of(this.sink.contextView());
  }

  @Override
  protected void hookOnComplete() {
    this.sink.complete();
  }

  @Override
  protected void hookOnError(final Throwable e) {
    this.sink.error(e);
  }

  @Override
  protected void hookOnSubscribe(final Subscription subscription) {
    request(1);
  }

}
