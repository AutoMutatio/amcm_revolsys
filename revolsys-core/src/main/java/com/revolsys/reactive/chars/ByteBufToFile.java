package com.revolsys.reactive.chars;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.nio.file.Path;

import org.reactivestreams.Subscription;

import com.revolsys.reactive.Reactive;

import io.netty.buffer.ByteBuf;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

public class ByteBufToFile implements CoreSubscriber<ByteBuf>, CompletionHandler<Integer, Long> {
  public static Mono<Long> create(final Flux<ByteBuf> source, final Path path,
    final OpenOption... options) {
    return Reactive.monoCloseable(() -> AsynchronousFileChannel.open(path, options),
      channel -> Mono.create(sink -> new ByteBufToFile(source, channel, sink)));
  }

  private long offset = 0;

  private AsynchronousFileChannel channel;

  private MonoSink<Long> sink;

  private Subscription subscription;

  private boolean complete;

  private ByteBuf inBuffer;

  private final ByteBuffer outBuffer = ByteBuffer.allocateDirect(8192);

  public ByteBufToFile(final Flux<ByteBuf> source, final AsynchronousFileChannel channel,
    final MonoSink<Long> sink) {
    this.channel = channel;
    this.sink = sink;
    source.subscribe(this);
    this.sink.onCancel(this::cancel)
      .onDispose(this::dispose)
      .onRequest(c -> this.subscription.request(1));
  }

  private void cancel() {
    this.subscription.cancel();
  }

  @Override
  public void completed(final Integer count, final Long attachment) {
    if (count < 0) {
      this.sink.error(new IllegalStateException("Unepected negative write: " + count));
    }
    this.offset += count;
    doWrite();
  }

  private void dispose() {
    if (this.subscription != null) {
      this.subscription.cancel();
    }
  }

  private void doWrite() {
    if (this.outBuffer.hasRemaining()) {
      this.channel.write(this.outBuffer, this.offset, this.offset, this);
    } else if (this.inBuffer != null && this.inBuffer.isReadable()) {
      fillOutBuffer();
      doWrite();
    } else if (this.complete) {
      try {
        this.sink.success(this.offset);
        if (this.subscription != null) {
          this.subscription.cancel();
        }
      } finally {
        this.subscription = null;
        this.sink = null;
        this.channel = null;
      }
      return;
    } else {
      this.subscription.request(1);
    }
  }

  @Override
  public void failed(final Throwable e, final Long attachment) {
    this.sink.error(e);
  }

  private int fillOutBuffer() {
    this.outBuffer.clear();
    final int readCount = Math.min(this.inBuffer.readableBytes(), this.outBuffer.remaining());
    this.outBuffer.limit(readCount);
    this.inBuffer.readBytes(this.outBuffer);
    this.outBuffer.flip();
    return readCount;
  }

  @Override
  public void onComplete() {
    this.complete = true;
    doWrite();
  }

  @Override
  public void onError(final Throwable t) {
    this.sink.error(t);
  }

  @Override
  public void onNext(final ByteBuf bytes) {
    if (this.inBuffer != null) {
      this.inBuffer.release();
    }
    this.inBuffer = bytes;
    bytes.retain();
    fillOutBuffer();
    doWrite();
  }

  @Override
  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;
  }
}
