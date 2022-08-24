package com.revolsys.reactive.bytebuf;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

public class SplitByteBufPublisher implements Publisher<Flux<ByteBuf>> {

  private class SplitSubscription implements Subscription, CoreSubscriber<ByteBuf> {

    private class InnerSubscription implements Subscription {

      private ByteBuf buffer;

      private boolean complete;

      private Throwable exception;

      private long count;

      private Subscriber<? super ByteBuf> subscriber;

      @Override
      public void cancel() {
        // TODO at some point might need to consume the intermediate items as
        // opposed to cancelling the parent
        SplitSubscription.this.cancel();
      }

      public void onComplete() {
        boolean sendComplete = false;
        Subscriber<?> subscriber;
        synchronized (this) {
          subscriber = this.subscriber;
          if (!this.complete) {
            sendComplete = subscriber != null;
            this.complete = true;
          }
        }
        if (sendComplete) {
          subscriber.onComplete();
        }
      }

      public void onError(final Throwable exception) {
        boolean sendException = false;
        Subscriber<?> subscriber;
        synchronized (this) {
          subscriber = this.subscriber;
          if (!this.complete) {
            this.exception = exception;
            sendException = subscriber != null;
            this.complete = true;
          }
        }
        if (sendException) {
          subscriber.onError(exception);
        }
      }

      public void onNext(final ByteBuf buffer) {
        boolean sendBuffer = false;
        Subscriber<? super ByteBuf> subscriber;
        synchronized (this) {
          subscriber = this.subscriber;
          if (!SplitSubscription.this.completed) {
            this.count += buffer.readableBytes();
            if (subscriber == null) {
              this.buffer = buffer;
            } else {
              this.buffer = null;
              sendBuffer = true;
            }
          }
        }
        if (sendBuffer) {
          subscriber.onNext(buffer);
        }
      }

      @Override
      public void request(final long n) {
        SplitSubscription.this.sourceSubscription.request(n);
      }

      public void setSubscriber(final Subscriber<? super ByteBuf> subscriber) {
        if (this.subscriber != null) {
          throw new IllegalStateException("Cannot subscribe twice");
        } else if (this.exception != null) {
          subscriber.onError(this.exception);
        } else if (this.complete) {
          subscriber.onComplete();
        }
        this.subscriber = subscriber;
        subscriber.onSubscribe(this);
      }
    }

    private long requestedCount = 0;

    private Subscription sourceSubscription;

    private InnerSubscription inner;

    private boolean completed = false;

    private Subscriber<? super Flux<ByteBuf>> subscriber;

    public SplitSubscription(final Subscriber<? super Flux<ByteBuf>> subscriber) {
      this.subscriber = subscriber;
    }

    @Override
    public void cancel() {
      Subscription sourceSubscription;
      synchronized (this) {
        sourceSubscription = this.sourceSubscription;
        this.sourceSubscription = null;
      }
      if (sourceSubscription != null) {
        sourceSubscription.cancel();
      }
    }

    @Override
    public void onComplete() {
      InnerSubscription inner;
      Subscriber<?> subscriber;
      synchronized (this) {
        this.completed = true;
        inner = this.inner;
        subscriber = this.subscriber;
        this.inner = null;
        this.subscriber = null;
      }
      if (inner != null) {
        inner.onComplete();
      }
      if (subscriber != null) {
        subscriber.onComplete();
      }
    }

    @Override
    public void onError(final Throwable exception) {
      InnerSubscription inner;
      Subscriber<?> subscriber;
      synchronized (this) {
        this.completed = true;
        inner = this.inner;
        subscriber = this.subscriber;
        this.inner = null;
        this.subscriber = null;
      }
      if (inner != null) {
        inner.onError(exception);
      } else if (subscriber != null) {
        subscriber.onError(exception);
      }
    }

    @Override
    public void onNext(final ByteBuf buffer) {
      InnerSubscription oldInner = null;
      InnerSubscription inner = null;
      Subscriber<? super Flux<ByteBuf>> sourceSubscriber = null;
      Flux<ByteBuf> innerPublisher = null;
      boolean completed;
      synchronized (this) {
        completed = this.completed;
        if (!completed) {
          final int count = buffer.readableBytes();
          if (count > 0) {

            inner = this.inner;
            if (inner == null || inner.count + count > SplitByteBufPublisher.this.maxSize) {
              oldInner = inner;
              sourceSubscriber = this.subscriber;
              final var newInner = this.inner = inner = new InnerSubscription();

              innerPublisher = Flux.from(subscriber -> newInner.setSubscriber(subscriber));

            }
          }
        }
      }
      if (oldInner != null) {
        oldInner.onComplete();
      }
      if (sourceSubscriber != null && innerPublisher != null) {
        sourceSubscriber
          .onNext(innerPublisher.doOnDiscard(ByteBuf.class, ReferenceCountUtil::release));
      }
      if (inner != null) {
        inner.onNext(buffer);
      }
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
      this.sourceSubscription = subscription;
    }

    @Override
    public void request(final long requestedCount) {
      if (requestedCount > 0) {
        synchronized (this) {
          if (this.sourceSubscription == null) {
            SplitByteBufPublisher.this.source.subscribe(this);
          }
          this.sourceSubscription.request(requestedCount);
          this.requestedCount += requestedCount;
          if (requestedCount < 0) {
            this.requestedCount = Long.MAX_VALUE;
          }
        }
      }
    }
  }

  private final long maxSize;

  private final Flux<? extends ByteBuf> source;

  public SplitByteBufPublisher(final Flux<? extends ByteBuf> source, final long maxSize) {
    this.maxSize = maxSize;
    this.source = source;
  }

  @Override
  public void subscribe(final Subscriber<? super Flux<ByteBuf>> subscriber) {
    final Subscription subscription = new SplitSubscription(subscriber);
    subscriber.onSubscribe(subscription);
  }

}
