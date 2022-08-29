package com.revolsys.reactive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
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

  private static Object WAIT_SYNC = new Object();

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
        return Files.createTempFile(baseName + "_", "." + extension);
      } catch (final IOException e) {
        throw Exceptions.wrap(e);
      }
    }, path -> action.apply(path), Paths::deleteFile);
  }

  public static void waitOn(final Disposable s) {
    waitOn(s, 1000);
  }

  public static void waitOn(final Disposable s, final long pollInterval) {
    synchronized (WAIT_SYNC) {
      while (!s.isDisposed()) {
        try {
          WAIT_SYNC.wait(pollInterval);
        } catch (final InterruptedException e) {
        }
      }
    }
  }

  public static void waitOn(final Flux<?> publisher) {
    waitOn(publisher, 1000);
  }

  public static void waitOn(final Flux<?> publisher, final long pollInterval) {
    final Disposable subscription = publisher.subscribe();
    waitOn(subscription, pollInterval);
  }

  public static void waitOn(final Mono<?> publisher) {
    waitOn(publisher, 1000);
  }

  public static void waitOn(final Mono<?> publisher, final long pollInterval) {
    final Disposable subscription = publisher.subscribe();
    waitOn(subscription, pollInterval);
  }

}
