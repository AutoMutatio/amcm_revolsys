package com.revolsys.reactive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jeometry.common.exception.Exceptions;
import org.reactivestreams.Publisher;

import com.revolsys.io.BaseCloseable;
import com.revolsys.io.file.Paths;

import io.netty.buffer.ByteBuf;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

public class Reactive {

  private static class LatchDisposable implements Disposable {
    private final CountDownLatch latch;

    private final Disposable disposable;

    public LatchDisposable(final CountDownLatch latch, final Disposable disposable) {
      this.latch = latch;
      this.disposable = disposable;
    }

    @Override
    public void dispose() {
      this.latch.countDown();
      this.disposable.dispose();
    }

    @Override
    public boolean isDisposed() {
      return this.latch.getCount() <= 0 || this.disposable.isDisposed();
    }

  }

  private static final Consumer<Disposable> NOOPCALLBACK = d -> {
  };

  public static BiConsumer<ByteBuf, SynchronousSink<ByteBuffer>> asByteBuffer() {
    return new BiConsumer<ByteBuf, SynchronousSink<ByteBuffer>>() {
      private ByteBuffer buffer;

      private int bufferSize = 0;

      @Override
      public void accept(final ByteBuf source, final SynchronousSink<ByteBuffer> sink) {
        final int maxCapacity = source.maxCapacity();
        if (maxCapacity > 0) {
          if (this.buffer == null) {
            this.bufferSize = maxCapacity;
            this.buffer = ByteBuffer.allocateDirect(maxCapacity);
          }
          int readableBytes = source.readableBytes();
          for (; readableBytes > 0; readableBytes = source.readableBytes()) {
            this.buffer.clear();
            if (readableBytes < this.bufferSize) {
              this.buffer.limit(readableBytes);
            }
            source.readBytes(this.buffer);
            this.buffer.flip();
            sink.next(this.buffer);
          }

        }
      };
    };
  }

  public static <T> Mono<T> asPath(final String baseName, final String extension,
    final Function<Path, Mono<T>> action) {
    return Mono.using(() -> {
      try {
        final Path file = Files.createTempFile(baseName + "_", "." + extension);
        file.toFile().deleteOnExit();
        return file;
      } catch (final IOException e) {
        throw Exceptions.wrap(e);
      }
    }, path -> action.apply(path).doOnError(e -> Paths.deleteFile(path)), c -> {
    });
  }

  public static <R extends AutoCloseable, V> Flux<V> fluxCloseable(
    final Callable<? extends R> supplier,
    final Function<? super R, ? extends Flux<? extends V>> mapper) {
    return Flux.using(supplier, mapper, BaseCloseable.closer());
  }

  public static <IN, OUT> Flux<OUT> fluxCreate(final Publisher<IN> source,
    final Function<FluxSink<OUT>, BaseSubscriber<IN>> subscriberConstructor) {
    return Flux.create(sink -> {
      final Flux<IN> flux = Flux.from(source);
      final BaseSubscriber<IN> subscriber = subscriberConstructor.apply(sink);
      sink.onDispose(subscriber);
      flux.subscribe(subscriber);
    });
  }

  public static <R extends AutoCloseable, V> Mono<V> monoCloseable(
    final Callable<? extends R> supplier,
    final Function<? super R, ? extends Mono<? extends V>> mapper) {
    return Mono.using(supplier, mapper, BaseCloseable.closer());
  }

  public static <T> Mono<T> monoJust(final T value, final Consumer<T> discarder) {
    @SuppressWarnings({
      "rawtypes", "unchecked"
    })
    final Class<? extends T> clazz = (Class)value.getClass();
    return Mono.just(value).doOnDiscard(clazz, discarder);
  }

  public static <T> Consumer<T> once(final Runnable action) {
    return new Consumer<T>() {
      boolean first = true;

      @Override
      public void accept(final T t) {
        if (this.first) {
          this.first = false;
          action.run();
        }
      }
    };
  }

  public static Flux<ByteBuffer> toByteBuffer(final Flux<ByteBuf> source$) {
    return source$.handle(asByteBuffer());
  }

  public static <T> Mono<T> usingPath(final String baseName, final String extension,
    final Function<Path, Mono<T>> action) {
    return Mono.using(() -> {
      try {
        final Path file = Files.createTempFile(baseName + "_", "." + extension);
        file.toFile().deleteOnExit();
        return file;
      } catch (final IOException e) {
        throw Exceptions.wrap(e);
      }
    }, path -> action.apply(path), Paths::deleteFile);
  }

  public static void waitOn(final Flux<?> publisher) {
    waitOn(publisher, NOOPCALLBACK);
  }

  public static void waitOn(final Flux<?> publisher,
    final Consumer<Disposable> subscriptionCallback) {
    final Function<CountDownLatch, Disposable> supplier = latch -> publisher
      .doAfterTerminate(latch::countDown)
      .subscribe();
    waitOn(supplier, subscriptionCallback);
  }

  private static void waitOn(final Function<CountDownLatch, Disposable> subscriptionSupplier,
    final Consumer<? super Disposable> subscriptionCallback) {
    final CountDownLatch latch = new CountDownLatch(1);
    final Disposable subscription = subscriptionSupplier.apply(latch);
    final Disposable latchDisposable = new LatchDisposable(latch, subscription);
    subscriptionCallback.accept(latchDisposable);
    try {
      latch.await();
    } catch (final InterruptedException e) {
      throw Exceptions.wrap(e);
    }
  }

  public static void waitOn(final Mono<?> publisher) {
    waitOn(publisher, NOOPCALLBACK);
  }

  public static void waitOn(final Mono<?> publisher,
    final Consumer<Disposable> subscriptionCallback) {
    final Function<CountDownLatch, Disposable> supplier = latch -> publisher
      .doAfterTerminate(latch::countDown)
      .subscribe();
    waitOn(supplier, subscriptionCallback);
  }

}
