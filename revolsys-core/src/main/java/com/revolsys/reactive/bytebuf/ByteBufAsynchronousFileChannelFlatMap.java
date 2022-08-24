package com.revolsys.reactive.bytebuf;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

public class ByteBufAsynchronousFileChannelFlatMap
  implements Function<ByteBuf, Publisher<Integer>> {

  private class Handler
    implements CompletionHandler<Integer, MonoSink<Integer>>, Consumer<MonoSink<Integer>> {
    private final ByteBuf source;

    public Handler(final ByteBuf source) {
      this.source = source;
    }

    @Override
    public void accept(final MonoSink<Integer> sink) {
      final ByteBufAsynchronousFileChannelFlatMap parent = ByteBufAsynchronousFileChannelFlatMap.this;
      final long position = parent.position;
      final ByteBuffer buffer = this.source.nioBuffer();
      final int count = buffer.remaining();
      if (count == 0) {
        sink.success(count);
      } else {
        parent.position += count;
        parent.channel.write(buffer, position, sink, this);
      }
    }

    @Override
    public void completed(final Integer count, final MonoSink<Integer> sink) {
      if (count > 0) {
        sink.success(count);
      }
    }

    @Override
    public void failed(final Throwable e, final MonoSink<Integer> sink) {
      sink.error(e);
    }
  }

  private long position = 0;

  private final AsynchronousFileChannel channel;

  public ByteBufAsynchronousFileChannelFlatMap(final AsynchronousFileChannel channel) {
    this.channel = channel;
  }

  @Override
  public Publisher<Integer> apply(final ByteBuf source) {
    return Mono.create(new Handler(source));
  }
}
