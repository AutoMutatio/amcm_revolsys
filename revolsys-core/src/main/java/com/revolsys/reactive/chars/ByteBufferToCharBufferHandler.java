package com.revolsys.reactive.chars;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jeometry.common.exception.Exceptions;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.publisher.FluxSink;

public class ByteBufferToCharBufferHandler implements Subscriber<ByteBuffer> {

  private enum SourceSubscriberState {
    SUBSCRIBE_START, SUBSCRIBE_SET, EMPTY, NEXT_REQUEST, NEXT_SET, FULL, PEEK, COMPLETE
  }

  private Subscription subscription;

  private ByteBuffer currentValue;

  private final AtomicReference<SourceSubscriberState> state = new AtomicReference<>(
    SourceSubscriberState.SUBSCRIBE_START);

  private final AtomicReference<Boolean> disposed = new AtomicReference<>(false);

  private final AtomicLong requestedCount = new AtomicLong();

  private final FluxSink<CharBuffer> sink;

  private final int bufferSize;

  private final CharBuffer chars;

  private final CharsetDecoder decoder;

  public ByteBufferToCharBufferHandler(Publisher<ByteBuffer> source,
    final FluxSink<CharBuffer> sink, Charset charset) {
    this(source, sink, charset, 8192);
  }

  public ByteBufferToCharBufferHandler(Publisher<ByteBuffer> source,
    final FluxSink<CharBuffer> sink, Charset charset, int bufferSize) {
    this.bufferSize = bufferSize;
    this.chars = CharBuffer.allocate(bufferSize + 4);

    this.decoder = charset.newDecoder();
    source.subscribe(this);
    this.sink = sink;
    sink.onCancel(this::cancel);
    sink.onDispose(this::dispose);
    sink.onRequest(this::onRequest);
  }

  private void cancel() {
    try {
      if (this.disposed.compareAndSet(false, true)) {
        final Subscription subscription = this.subscription;
        this.subscription = null;
        if (subscription != null) {
          subscription.cancel();
        }
      }
    } catch (final RuntimeException e) {
      Exceptions.throwUncheckedException(e);
    } catch (final Error e) {
      Exceptions.throwUncheckedException(e);
    }
  }

  private void dispose() {
    cancel();
  }

  private synchronized void emit() {
    long requestedCount;
    do {
      if (this.state.compareAndSet(SourceSubscriberState.FULL, SourceSubscriberState.PEEK)) {
        final ByteBuffer buffer = this.currentValue;
        if (buffer.hasRemaining()) {
          this.chars.clear();
          this.decoder.decode(buffer, this.chars, false);
          this.chars.flip();
          this.sink.next(this.chars);
        }
        if (buffer.hasRemaining()) {
          this.state.compareAndSet(SourceSubscriberState.PEEK, SourceSubscriberState.FULL);
        } else if (this.state.compareAndSet(SourceSubscriberState.PEEK,
          SourceSubscriberState.EMPTY)) {
          this.currentValue = null;
          if (this.subscription == null) {
            this.state.compareAndSet(SourceSubscriberState.EMPTY, SourceSubscriberState.COMPLETE);
          }
        }
      }
      synchronized (this.requestedCount) {
        requestedCount = this.requestedCount.get();
        if (requestedCount > 0) {
          if (requestedCount != Long.MAX_VALUE) {
            requestedCount = this.requestedCount.decrementAndGet();
          }
          if (this.state.compareAndSet(SourceSubscriberState.EMPTY,
            SourceSubscriberState.NEXT_REQUEST)) {
            this.subscription.request(1);
          }
        }
      }

      if (isComplete()) {
        this.sink.complete();
      }
    } while (requestedCount > 0 && this.currentValue != null);
  }

  public boolean isComplete() {
    return this.state.get() == SourceSubscriberState.COMPLETE;
  }

  @Override
  public void onComplete() {
    this.subscription = null;
    this.state.compareAndSet(SourceSubscriberState.NEXT_REQUEST, SourceSubscriberState.COMPLETE);
    this.state.compareAndSet(SourceSubscriberState.EMPTY, SourceSubscriberState.COMPLETE);
    emit();
  }

  @Override
  public void onError(final Throwable t) {
    this.sink.error(t);
  }

  @Override
  public void onNext(final ByteBuffer value) {
    if (this.state.compareAndSet(SourceSubscriberState.NEXT_REQUEST,
      SourceSubscriberState.NEXT_SET)) {
      this.currentValue = value;
      this.state.set(SourceSubscriberState.FULL);
    } else {
      throw new IllegalStateException("onNext can only be called in NEXT_REQUEST states");
    }
    emit();
  }

  private void onRequest(final long count) {
    if (count < 0) {
      throw new IllegalArgumentException("Count must be >= 0: " + count);
    }
    if (count > 0) {
      synchronized (this.requestedCount) {
        final long oldCount = this.requestedCount.get();
        if (count == Long.MAX_VALUE || this.requestedCount.addAndGet(count) < oldCount) {
          this.requestedCount.set(Long.MAX_VALUE);
        }
      }

    }
    emit();
  }

  @Override
  public void onSubscribe(final Subscription subscription) {
    if (this.state.compareAndSet(SourceSubscriberState.SUBSCRIBE_START,
      SourceSubscriberState.SUBSCRIBE_SET)) {
      this.subscription = subscription;
      this.state.set(SourceSubscriberState.EMPTY);
    } else {
      throw new IllegalStateException("onSubscribe must not be called twice");
    }
  }

  @Override
  public String toString() {
    return this.state + "\t" + this.currentValue;
  }

}
