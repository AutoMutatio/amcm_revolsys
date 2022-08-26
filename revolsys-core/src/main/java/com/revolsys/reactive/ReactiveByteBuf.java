package com.revolsys.reactive;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.revolsys.io.FileUtil;
import com.revolsys.io.file.Paths;
import com.revolsys.reactive.bytebuf.AsynchronousFileChannelToFluxByteBufHandler;
import com.revolsys.reactive.bytebuf.ByteBufAsynchronousFileChannelFlatMap;
import com.revolsys.reactive.bytebuf.ByteBufChannelWriter;
import com.revolsys.reactive.bytebuf.SplitByteBufPublisher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

public class ReactiveByteBuf {

  public static final Callable<Integer> ZERO_SUPPLIER = () -> 0;

  public static final Callable<Long> LONG_ZERO_SUPPLIER = () -> 0L;

  static {
    Hooks.onNextDropped(ReferenceCountUtil::release);
  }

  public static Mono<ByteBuffer> collect(final int length, final Flux<ByteBuf> flux) {
    return flux.collect(() -> ByteBuffer.allocate(length), (target, source) -> {
      final int readableBytes = source.readableBytes();
      target.limit(target.position() + readableBytes);
      source.readBytes(target);
      source.release();
    });
  }

  public static Function<ByteBuf, Publisher<Integer>> flatMapToFileChannel(
    final AsynchronousFileChannel channel) {
    return new ByteBufAsynchronousFileChannelFlatMap(channel);
  }

  public static Flux<ByteBuf> read(final byte[] bytes, final int offset, final int length) {
    final BiFunction<Integer, SynchronousSink<ByteBuf>, Integer> generator = (currentOffset,
      sink) -> {
      try {
        final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(8192);
        final int bufferSize = buffer.capacity();
        int writeSize = length - currentOffset;
        if (writeSize <= 0) {
          sink.complete();
          return currentOffset;
        } else {
          if (writeSize > bufferSize) {
            writeSize = bufferSize;
          }
          buffer.writeBytes(bytes, currentOffset, writeSize);
          sink.next(buffer);
        }
        return currentOffset + writeSize;
      } catch (final Exception e) {
        sink.error(e);
        return currentOffset;
      }
    };
    final Flux<ByteBuf> flux = Flux.generate(() -> offset, generator);
    return ReactiveSchedulers.scheduleNonBlocking(flux)
      .doOnDiscard(ByteBuf.class, ReferenceCountUtil::release);
  }

  public static Flux<ByteBuf> read(final InputStream in) {
    return readChannel(() -> Channels.newChannel(in));
  }

  public static Flux<ByteBuf> read(final Path file) {
    return readAsyncFile(() -> AsynchronousFileChannel.open(file, StandardOpenOption.READ));
  }

  public static Flux<ByteBuf> readAsyncFile(final Callable<AsynchronousFileChannel> source) {
    final Flux<ByteBuf> flux = Flux.using(source,
      channel -> Flux.create(AsynchronousFileChannelToFluxByteBufHandler.create(channel)),
      FileUtil::closeSilent);
    return ReactiveSchedulers.scheduleBlocking(flux)
      .doOnDiscard(ByteBuf.class, ReferenceCountUtil::release);
  }

  public static Flux<ByteBuf> readChannel(final Callable<ReadableByteChannel> channelSupplier) {
    return Flux.using(channelSupplier, ReactiveByteBuf::readChannel, FileUtil::closeSilent)
      .subscribeOn(Schedulers.boundedElastic());
  }

  public static Flux<ByteBuf> readChannel(final ReadableByteChannel channel) {
    if (channel instanceof final FileChannel fileChannel) {
      return readFileChannel(fileChannel);
    } else if (channel instanceof final AsynchronousFileChannel fileChannel) {
      return readAsyncFile(() -> fileChannel);
    } else {
      return readChannelImpl(channel);
    }
  }

  private static Flux<ByteBuf> readChannelImpl(final ReadableByteChannel channel) {
    final ByteBuffer tempBuffer = ByteBuffer.allocate(8192);
    final BiFunction<Long, SynchronousSink<ByteBuf>, Long> generator = (position, sink) -> {
      try {
        if (channel.isOpen()) {
          final int count = channel.read(tempBuffer);
          if (count == -1) {
            sink.complete();
          } else {
            final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(8192);
            tempBuffer.flip();
            buffer.writeBytes(tempBuffer);
            tempBuffer.clear();
            sink.next(buffer);
            return position + count;
          }
        } else {
          sink.complete();
        }
      } catch (final Exception e) {
        sink.error(e);
      }
      return position;
    };
    final Flux<ByteBuf> flux = Flux.generate(LONG_ZERO_SUPPLIER, generator);
    return ReactiveSchedulers.scheduleBlocking(flux)
      .doOnDiscard(ByteBuf.class, ReferenceCountUtil::release);
  }

  public static Flux<ByteBuf> readFileChannel(final FileChannel fileChannel) {
    BiFunction<Long, SynchronousSink<ByteBuf>, Long> generator;
    try {
      final long fileSize = fileChannel.size();
      generator = (position, sink) -> {
        try {
          if (position < fileSize) {
            final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
            final int writeCount = buffer.writeBytes(fileChannel, position, 0);
            sink.next(buffer);
            return position + writeCount;
          } else {
            sink.complete();
          }
        } catch (final IOException e) {
          sink.error(e);
        }
        return position;
      };
      final Flux<ByteBuf> flux = Flux.generate(LONG_ZERO_SUPPLIER, generator);
      return ReactiveSchedulers.scheduleBlocking(flux)
        .doOnDiscard(ByteBuf.class, ReferenceCountUtil::release);
    } catch (final IOException e) {
      return Flux.error(e);
    }
  }

  public static Flux<Publisher<ByteBuf>> split(final Flux<ByteBuf> source, final long pageSize) {
    return Flux.from(new SplitByteBufPublisher(source, pageSize));
  }

  public static Flux<Publisher<ByteBuf>> split(final Flux<ByteBuf> source, final long size,
    final long pageSize) {
    if (size < pageSize) {
      return Flux.just(source);
    } else {
      return Flux.from(new SplitByteBufPublisher(source, pageSize));
    }
  }

  public static <T> Mono<T> usingPath(final String baseName, final String extension,
    final Flux<ByteBuf> source, final Function<Path, Mono<T>> action) {
    return Reactive.usingPath(baseName, extension, file -> write(file, source).flatMap(action));
  }

  public static Mono<Long> write(final Flux<ByteBuf> source, final OutputStream outputStream) {
    return writeByteChannel(source, () -> Channels.newChannel(outputStream));
  }

  public static Flux<Integer> write(final Flux<ByteBuf> source, final WritableByteChannel channel) {
    final var writer = new ByteBufChannelWriter(channel);
    return source.doOnNext(ByteBuf::retain).publishOn(Schedulers.boundedElastic()).map(writer);
  }

  public static Mono<Path> write(final Path file, final Flux<ByteBuf> source) {
    final Flux<Path> f = Flux.using(Paths.asyncWriteFileChannel(file),
      channel -> source.flatMap(flatMapToFileChannel(channel)).then(Mono.just(file)),
      FileUtil::closeSilent);
    return f.single();
  }

  public static Mono<Long> writeByteChannel(final Flux<ByteBuf> source,
    final Callable<WritableByteChannel> channelSupplier) {
    return Flux.using(channelSupplier, channel -> {
      final ByteBufChannelWriter writer = new ByteBufChannelWriter(channel);
      return source.doOnNext(ByteBuf::retain).publishOn(Schedulers.boundedElastic()).map(writer);
    }, FileUtil::closeSilent)
      .subscribeOn(Schedulers.boundedElastic())
      .reduce(1L, (sum, count) -> sum += count);

    // return Mono.fromSupplier(channelSupplier) //
    // .subscribeOn(Schedulers.boundedElastic())
    // .flatMap(channel -> {
    // final var writer = new ByteBufChannelWriter(channel);
    // return source.doOnNext(ByteBuf::retain)
    // .publishOn(Schedulers.boundedElastic())
    // .map(writer)
    // .doOnTerminate(() -> FileUtil.close(channel))
    // .reduce(1L, (sum, count) -> sum += count);
    // });
  }

}
