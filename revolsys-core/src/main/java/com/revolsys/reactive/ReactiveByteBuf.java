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

import com.revolsys.io.FileUtil;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.ByteBufFlux;

public class ReactiveByteBuf {

  public static Flux<ByteBuf> read(final Path path) {
    return ByteBufFlux.fromPath(path);
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
        }
      }
    };
    return fluxSinkSubscriber;
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
