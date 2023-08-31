package com.revolsys.reactive.chars;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;

import com.revolsys.reactive.Reactive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;
import reactor.netty.ByteBufFlux;

public class ByteBufs {

  public static Flux<ByteBuf> alignWindow(final Flux<? extends ByteBuf> source, final long size) {
    return new ByteBufFluxAlignWindow(source, size);
  }

  public static Flux<CharBuffer> asCharBuffer(final Publisher<ByteBuf> publisher,
    final Charset charset, final int bufferSize) {
    final Consumer<? super FluxSink<CharBuffer>> handler = sink -> new ByteBufToCharBufferHandler(
      publisher, sink, charset, bufferSize);
    return Flux.create(handler);
  }

  public static BiConsumer<? super ByteBuf, SynchronousSink<CharBuffer>> fluxHundler(
    final Charset charset, final int bufferSize) {
    final byte[] bytes = new byte[bufferSize];
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final CharBuffer chars = CharBuffer.allocate(bufferSize);
    final CharsetDecoder decoder = charset.newDecoder();
    return (byteBuf, sink) -> {
      final int count = byteBuf.readableBytes();
      byteBuf.readBytes(bytes, 0, count);
      byteBuffer.rewind();
      byteBuffer.limit(count);
      decoder.decode(byteBuffer, chars, false);
      chars.flip();
      sink.next(chars);
    };
  }

  public static ByteBufFlux fromChannel(final Callable<ReadableByteChannel> source) {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
    final Flux<ByteBuf> bytes = Reactive.fluxCloseable(source, channel -> Flux.generate(sink -> {
      try {
        buffer.clear();
        if (channel.read(buffer) < 0) {
          sink.complete();
        } else {
          buffer.flip();
          final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
          buf.writeBytes(buffer);
          sink.next(buf);
        }
      } catch (final IOException e) {
        sink.error(e);
      }
    }));
    return ByteBufFlux.fromInbound(bytes, ByteBufAllocator.DEFAULT);
  }

  public static ByteBufFlux fromInputStream(final Callable<InputStream> source) {
    return fromChannel(() -> Channels.newChannel(source.call()));
  }

  public static ByteBufFlux fromString(final String source) {
    return ByteBufFlux.fromString(Mono.just(source), StandardCharsets.UTF_8,
      ByteBufAllocator.DEFAULT);
  }

  public static InputStream inputStream(final Flux<ByteBuf> bytes) {
    final ByteBufInputStream in = new ByteBufInputStream();
    in.subscribe(bytes);
    return in;
  }

  public static Mono<InputStream> inputStream$(final Flux<ByteBuf> bytes) {
    return Mono.defer(() -> {
      final InputStream in = inputStream(bytes);
      return Mono.just(in).publishOn(Schedulers.boundedElastic());
    });
  }

  public static Flux<Flux<ByteBuf>> splitWindow(final Flux<? extends ByteBuf> source,
    final long size) {
    return new ByteBufFluxSplitWindow(source, size);
  }

  public static Flux<ByteBuffer> toByteBufFlux(final Flux<ByteBuf> source) {
    return source.handle((bb, sink) -> {
      try {
        sink.next(bb.nioBuffer());
      } catch (final IllegalReferenceCountException e) {
        sink.complete();
      }
    });
  }

  public static ByteBufFlux toByteBufFlux(final String value) {
    return ByteBufFlux.fromString(Mono.just(value));
  }

  public static Mono<Long> write(final Flux<ByteBuf> source,
    final AsynchronousByteChannel channel) {
    return Mono
      .<Long> create(sink -> new ByteBufToAsyncChannel(toByteBufFlux(source), channel, sink));
  }

  public static Mono<Long> write(final Flux<ByteBuf> source,
    final AsynchronousFileChannel channel) {
    return Mono.<Long> create(sink -> new ByteBufToFile(toByteBufFlux(source), channel, sink));
  }

  public static Mono<Long> write(final Flux<ByteBuf> source, final Callable<OutputStream> out) {
    return Reactive.monoCloseable(() -> Channels.newChannel(out.call()),
      channel -> write(source, channel));
  }

  public static Mono<Long> write(final Flux<ByteBuf> source, final Channel channel) {
    if (channel instanceof AsynchronousFileChannel) {
      return write(source, (AsynchronousFileChannel)channel);
    } else if (channel instanceof AsynchronousByteChannel) {
      return write(source, (AsynchronousByteChannel)channel);
    } else if (channel instanceof WritableByteChannel) {
      return write(source, (WritableByteChannel)channel);
    } else {
      throw new IllegalArgumentException("Channel not supported: " + channel.getClass());
    }
  }

  public static Mono<Long> write(final Flux<ByteBuf> source, final OutputStream out) {
    return Reactive.monoCloseable(() -> Channels.newChannel(out),
      channel -> write(source, channel));
  }

  public static Mono<Long> write(final Flux<ByteBuf> source, final Path path,
    final OpenOption... options) {
    return Reactive.monoCloseable(() -> AsynchronousFileChannel.open(path, options),
      channel -> write(source, channel));
  }

  public static Mono<Long> write(final Flux<ByteBuf> source, final WritableByteChannel channel) {
    return Mono.<Long> create(
      sink -> new ByteBufToWritableByteChannel(toByteBufFlux(source), channel, sink));
  }

  public static Mono<Long> write(final String source, final Path path,
    final OpenOption... options) {
    return write(fromString(source), path, options);
  }
}
