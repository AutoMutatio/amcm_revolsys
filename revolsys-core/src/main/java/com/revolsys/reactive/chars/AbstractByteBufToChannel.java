package com.revolsys.reactive.chars;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;

import org.jeometry.common.exception.Exceptions;
import org.reactivestreams.Subscription;

import io.netty.buffer.ByteBuf;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Schedulers;

public abstract class AbstractByteBufToChannel<C extends Channel>
  implements CoreSubscriber<ByteBuffer> {

  private long offset = 0;

  protected C channel;

  protected MonoSink<Long> sink;

  private Subscription subscription;

  private boolean complete;

  private ByteBuf inBuffer;

  private ByteBuffer outBuffer;

  protected long writeOutstanding = 0;

  private final Flux<ByteBuffer> source;

  private boolean started = false;

  public AbstractByteBufToChannel(final Flux<ByteBuffer> source, final C channel,
    final MonoSink<Long> sink) {
    this.source = source;
    this.channel = channel;
    this.sink = sink;
    this.source.limitRate(1).subscribeOn(Schedulers.boundedElastic()).subscribe(this);
    this.sink.onCancel(this::cancel).onDispose(this::dispose).onRequest(this::onRequest);
  }

  private void cancel() {
    final Subscription subscription = this.subscription;
    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void dispose() {
    final Subscription subscription = this.subscription;
    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void doRequest() {
    if (this.started) {
      this.subscription.request(1);
    }
  }

  protected void doWrite() {
    try {
      if (this.writeOutstanding != 0) {
      } else {
        if (this.outBuffer.hasRemaining()) {
          final int size = this.outBuffer.remaining();
          this.writeOutstanding += doWrite(this.outBuffer, this.offset);
          this.offset += size;
          doWrite();
        } else if (this.complete) {
          try {
            this.sink.success(this.offset);
            if (this.subscription != null) {
              this.subscription.cancel();
            }
          } finally {
            this.subscription = null;
            this.sink = null;
            this.channel = null;
          }
          return;
        } else {
          doRequest();
        }
      }
    } catch (final IOException e) {
      throw Exceptions.wrap(e);
    }
  }

  protected abstract int doWrite(ByteBuffer bytes, long offset) throws IOException;

  @Override
  public void onComplete() {
    this.complete = true;
    doWrite();
  }

  @Override
  public void onError(final Throwable t) {
    this.sink.error(t);
  }

  @Override
  public void onNext(final ByteBuffer bytes) {
    try {
      this.outBuffer = bytes;
      doWrite();
    } catch (final Exception e) {
      if (!this.complete) {
        this.sink.error(e);
      }
    }
  }

  private void onRequest(final long request) {
    if (!this.complete) {
      this.started = true;

      doRequest();
    }
  }

  @Override
  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;
  }
}
