package com.revolsys.reactive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.jeometry.common.exception.Exceptions;
import org.reactivestreams.Publisher;

import com.revolsys.io.FileUtil;
import com.revolsys.io.file.Paths;
import com.revolsys.reactive.ReaderWriterCollector.ReaderWriter;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

public class Reactive {

  public static final ReaderWriter<ByteBuf, FileChannel> BYTE_BUF_READER_WRITER = (buf, channel,
    position) -> {
    final int capacity = buf.capacity();
    return buf.readBytes(channel, position, capacity);
  };

  public static final ReaderWriter<ByteBuffer, FileChannel> BYTE_BUFFER_READER_WRITER = (buf,
    channel, position) -> channel.write(buf, position);

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

  public static <IN, OUT> Flux<OUT> fluxCreate(final Publisher<IN> source,
    final Function<FluxSink<OUT>, BaseSubscriber<IN>> subscriberConstructor) {
    return Flux.create(sink -> {
      final Flux<IN> flux = Flux.from(source);
      final BaseSubscriber<IN> subscriber = subscriberConstructor.apply(sink);
      sink.onDispose(subscriber);
      flux.subscribe(subscriber);
    });
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

  /**
   * Collect the {@Link Flux} stream to a {@Path}.
   *
   * @param flux The stream of source data.
   * @param file The file to write to
   * @param readerWriter The function to read from the source and write to the channel.
   * @return The collector;
   */
  public static <S> Mono<Path> toFile(final Flux<S> flux, final Path file,
    final ReaderWriter<S, FileChannel> readerWriter) {
    final Collector<S, FileChannel, Path> collector = toFile(file, readerWriter);
    return flux.collect(collector).doOnDiscard(FileChannel.class, FileUtil::close);
  }

  /**
   * Create a collector to write the {@Link Flux} stream to a {@Path}.
   *
   * To ensure the intermediate {@link FileChannel} is closed use the doOnDiscard as shown below
   * or use the {@link #toFile(Flux, Path, ReaderWriter) function instead.
   *
   * <pre>
   * Flux<ByteBuf> source = ...;
   * Path file = ...;
   * source.
   *   collect(toFile(file, ReactiveIo.BYTE_BUF_READER_WRITER))
   *   .doOnDiscard(FileChannel.class, FileUtil::close)
   *   :
   * </pre>
   *
   * @param file The file to write to
   * @param readerWriter The function to read from the source and write to the channel.
   * @return The collector;
   */
  public static <S> Collector<S, FileChannel, Path> toFile(final Path file,
    final ReaderWriter<S, FileChannel> readerWriter) {
    final Supplier<FileChannel> supplier = () -> {
      try {
        return FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      } catch (final IOException e) {
        throw Exceptions.wrap(e);
      }
    };
    final Function<FileChannel, Path> closer = channel -> {
      FileUtil.closeSilent(channel);
      return file;
    };
    return new ReaderWriterCollector<>(readerWriter, supplier, closer);
  }

  public static <T> Mono<T> usingPath(final String baseName, final String extension,
    final Function<Path, Mono<T>> action) {
    final Flux<T> f = Flux.using(() -> {
      try {
        return Files.createTempFile(baseName + "_", "." + extension);
      } catch (final IOException e) {
        throw Exceptions.wrap(e);
      }
    }, file -> action.apply(file), Paths::deleteFile);
    return f.single();
  }

}
