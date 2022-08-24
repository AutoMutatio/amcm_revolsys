package com.revolsys.reactive.bytebuf;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.function.BiConsumer;

import reactor.core.publisher.SynchronousSink;

public class AsynchronousFileChannelWriter
  implements BiConsumer<ByteBuffer, SynchronousSink<Integer>> {

  private long position = 0;

  private final AsynchronousFileChannel channel;

  public AsynchronousFileChannelWriter(final AsynchronousFileChannel channel) {
    this.channel = channel;
  }

  @Override
  public void accept(final ByteBuffer source, final SynchronousSink<Integer> sink) {
    final long position = this.position;
    final Integer count = source.remaining();
    this.position += count;
    this.channel.write(source, position, count, new CompletionHandler<Integer, Integer>() {
      @Override
      public void completed(final Integer count, final Integer expectedCount) {
        if (count.intValue() != expectedCount.intValue()) {
          System.err.println(position + " " + count + " " + expectedCount);

        }
        if (count > 0) {
          System.out.println(position + " " + count + " " + expectedCount);
          try {
            sink.next(count);
          } catch (final Exception e) {
            System.err.println(position);
            e.printStackTrace();
          }
        }
      }

      @Override
      public void failed(final Throwable e, final Integer attachment) {
        sink.error(e);
      }
    });
  };
}
