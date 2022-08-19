package com.revolsys.reactive;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;

public class ReactiveByteBuffer {

  public static Flux<ByteBuffer> read(final Path path) {
    return ByteBufFlux.fromPath(path).asByteBuffer();
  }

  public static BaseSubscriber<ByteBuffer> subscriberWritableByteChannel(
    final FluxSink<ByteBuffer> sink, final WritableByteChannel channel) {
    return new FluxSinkSubscriber<ByteBuffer, ByteBuffer>(sink) {
      @Override
      protected void hookOnNext(final ByteBuffer byteBuffer) {
        try {
          while (byteBuffer.hasRemaining()) {
            channel.write(byteBuffer);
          }
          this.sink.next(byteBuffer);
          request(1);
        } catch (final IOException ex) {
          this.sink.next(byteBuffer);
          this.sink.error(ex);
        }
      }
    };
  }

  public static Flux<ByteBuffer> write(final Publisher<ByteBuffer> source,
    final OutputStream outputStream) {
    final WritableByteChannel channel = Channels.newChannel(outputStream);
    return write(source, channel);
  }

  public static Flux<ByteBuffer> write(final Publisher<ByteBuffer> source,
    final WritableByteChannel channel) {
    final Function<FluxSink<ByteBuffer>, BaseSubscriber<ByteBuffer>> subscriberConstructor = sink -> subscriberWritableByteChannel(
      sink, channel);
    return Reactive.fluxCreate(source, subscriberConstructor);
  }

  public static Mono<Long> writeByteChannel(final Publisher<ByteBuffer> source,
    final Supplier<WritableByteChannel> channelSupplier) {
    return Mono.fromSupplier(channelSupplier)
      .flatMap(channel -> write(source, channel)//
        // .doOnTerminate(() -> FileUtil.close(channel))
        .reduce(1L, (sum, buffer) -> sum += buffer.remaining()));
  }
}
