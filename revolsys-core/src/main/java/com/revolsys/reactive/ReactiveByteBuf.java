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
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import com.revolsys.io.FileUtil;
import com.revolsys.io.file.Paths;
import com.revolsys.reactive.bytebuf.AsynchronousFileChannelByteBufGenerator;
import com.revolsys.reactive.bytebuf.ByteBufAsynchronousFileChannelFlatMap;
import com.revolsys.reactive.bytebuf.SplitByteBufPublisher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
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

  private static Flux<ByteBuf> asByteBuf(final ReadableByteChannel channel) {
    BiFunction<Long, SynchronousSink<ByteBuf>, Long> generator;
    try {
      if (channel instanceof final FileChannel fileChannel) {
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

      } else if (channel instanceof final AsynchronousFileChannel fileChannel) {
        return readAsyncFile(() -> fileChannel);
      } else {
        final ByteBuffer tempBuffer = ByteBuffer.allocate(8192);
        generator = (position, sink) -> {
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
      }
      return Flux.generate(LONG_ZERO_SUPPLIER, generator)
        .doOnDiscard(ByteBuf.class, ReferenceCountUtil::release);

    } catch (final IOException e) {
      return Flux.error(e);
    }
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

  public static Flux<ByteBuf> read(final byte[] bytes) {
    final BiFunction<Integer, SynchronousSink<ByteBuf>, Integer> generator = (offset, sink) -> {
      try {
        final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(8192);
        final int bufferSize = buffer.capacity();
        int writeSize = bytes.length - offset;
        if (writeSize <= 0) {
          sink.complete();
          return offset;
        } else {
          if (writeSize > bufferSize) {
            writeSize = bufferSize;
          }
          buffer.writeBytes(bytes, offset, writeSize);
          sink.next(buffer);
        }
        return offset + writeSize;
      } catch (final Exception e) {
        sink.error(e);
        return offset;
      }
    };
    return Flux.generate(ZERO_SUPPLIER, generator)
      .doOnDiscard(ByteBuf.class, ReferenceCountUtil::release);
  }

  public static Flux<ByteBuf> read(final InputStream in) {
    return readChannel(() -> Channels.newChannel(in));
  }

  public static Flux<ByteBuf> read(final Path file) {
    return Flux.generate(() -> FileChannel.open(file), (fc, sink) -> {
      final ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(8192);
      try {
        if (buf.writeBytes(fc, buf.capacity()) < 0) {
          buf.release();
          sink.complete();
        } else {
          sink.next(buf);
        }
      } catch (final Exception e) {
        buf.release();
        sink.error(e);
      }
      return fc;
    }, FileUtil::close);
  }

  public static Flux<ByteBuf> readAsyncFile(final Callable<AsynchronousFileChannel> source) {
    final AsynchronousFileChannelByteBufGenerator generator = new AsynchronousFileChannelByteBufGenerator();
    return Flux.generate(source, generator, FileUtil::close)
      .doOnDiscard(ByteBuf.class, ReferenceCountUtil::release);
  }

  public static Flux<ByteBuf> readChannel(final Callable<ReadableByteChannel> channelSupplier) {
    return Flux.using(channelSupplier, ReactiveByteBuf::asByteBuf, FileUtil::close)
      .doOnDiscard(ByteBuf.class, ReferenceCountUtil::release);
  }

  public static Flux<Publisher<ByteBuf>> split(final Flux<ByteBuf> source, final long pageSize) {
    return Flux.from(new SplitByteBufPublisher(source, pageSize));
  }

  public static BaseSubscriber<ByteBuf> subscriberWritableByteChannel(final FluxSink<Integer> sink,
    final WritableByteChannel channel) {
    final FluxSinkSubscriber<ByteBuf, Integer> fluxSinkSubscriber = new FluxSinkSubscriber<ByteBuf, Integer>(
      sink) {
      private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(8192);

      @Override
      protected void hookOnNext(final ByteBuf buffer) {
        final int count = buffer.readableBytes();
        try {
          for (int remaining = buffer.readableBytes(); remaining > 0; remaining = buffer
            .readableBytes()) {
            this.byteBuffer.clear();
            this.byteBuffer.limit(Math.min(remaining, 8192));
            buffer.readBytes(this.byteBuffer);
            this.byteBuffer.flip();
            while (this.byteBuffer.hasRemaining()) {
              channel.write(this.byteBuffer);
            }
          }
          this.sink.next(count);
          request(1);
        } catch (final IOException ex) {
          this.sink.error(ex);
        } finally {
          buffer.release();
        }
      }
    };
    return fluxSinkSubscriber;
  }

  public static <T> Mono<T> usingPath(final String baseName, final String extension,
    final Flux<ByteBuf> source, final Function<Path, Mono<T>> action) {
    return Reactive.usingPath(baseName, extension, file -> write(file, source).flatMap(action));
  }

  public static Mono<Path> write(final Path file, final Flux<ByteBuf> source) {
    final Flux<Path> f = Flux.using(Paths.asyncWriteFileChannel(file),
      channel -> source.flatMap(flatMapToFileChannel(channel)).then(Mono.just(file)),
      FileUtil::close);
    return f.single();
  }

  public static Flux<Integer> write(final Publisher<ByteBuf> source,
    final OutputStream outputStream) {
    final WritableByteChannel channel = Channels.newChannel(outputStream);
    return write(source, channel);
  }

  public static Flux<Integer> write(final Publisher<ByteBuf> source,
    final WritableByteChannel channel) {
    final Function<FluxSink<Integer>, BaseSubscriber<ByteBuf>> subscriberConstructor = sink -> subscriberWritableByteChannel(
      sink, channel);
    return Reactive.fluxCreate(source, subscriberConstructor);
  }

  public static Mono<Long> writeByteChannel(final Publisher<ByteBuf> source,
    final Supplier<WritableByteChannel> channelSupplier) {
    return Mono.fromSupplier(channelSupplier)
      .publishOn(Schedulers.parallel())
      .flatMap(channel -> write(source, channel)//
        .doOnTerminate(() -> FileUtil.close(channel))
        .reduce(1L, (sum, count) -> sum += count));
  }

}
