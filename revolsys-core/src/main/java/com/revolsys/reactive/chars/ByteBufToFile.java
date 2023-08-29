package com.revolsys.reactive.chars;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.nio.file.Path;

import org.reactivestreams.Subscription;

import com.revolsys.reactive.Reactive;
import com.revolsys.reactive.RequestCounter;

import io.netty.buffer.ByteBuf;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

public class ByteBufToFile implements CoreSubscriber<ByteBuf>, CompletionHandler<Integer, Long> {
  public static Mono<Long> create(final Flux<ByteBuf> source, final Path path,
    final OpenOption... options) {
    return Reactive
      .fluxCloseable(() -> AsynchronousFileChannel.open(path, options),
        channel -> Mono.<Long> create(sink -> new ByteBufToFile(source, channel, sink)))
      .singleOrEmpty();
  }

  private long offset = 0;

  private AsynchronousFileChannel channel;

  private MonoSink<Long> sink;

  private Subscription subscription;

  private boolean complete;

  private ByteBuf inBuffer;

  private final ByteBuffer outBuffer = ByteBuffer.allocateDirect(8192);

  private long writeOutstanding = 0;

  private final Flux<ByteBuf> source;

  private final RequestCounter requestCount = new RequestCounter();

  public ByteBufToFile(final Flux<ByteBuf> source, final AsynchronousFileChannel channel,
    final MonoSink<Long> sink) {
    this.source = source;
    this.channel = channel;
    this.sink = sink;
    this.source.limitRate(1).subscribe(this);
    this.sink.onCancel(this::cancel).onDispose(this::dispose).onRequest(this::onRequest);
  }

  private void cancel() {
    final Subscription subscription = this.subscription;
    if (subscription != null) {
      subscription.cancel();
    }
  }

  @Override
  public void completed(final Integer count, final Long attachment) {
    if (count < 0) {
      this.sink.error(new IllegalStateException("Unepected negative write: " + count));
    }
    this.writeOutstanding -= count;
    this.offset += count;
    doWrite();
  }

  private void dispose() {
    final Subscription subscription = this.subscription;
    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void doRequest() {
    if (this.requestCount.release(1) > 0) {
      this.subscription.request(1);
    }
  }

  private void doWrite() {
    if (this.writeOutstanding != 0) {
    } else if (this.outBuffer.hasRemaining()) {
      this.writeOutstanding += this.outBuffer.remaining();
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
      doRequest();
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

  private void onRequest(final long request) {
    if (!this.complete) {
      if (this.subscription == null) {
        // this.source.subscribeOn(Schedulers.parallel()).subscribe(this);
      }
      this.requestCount.request(request);

      doRequest();
    }
  }

  @Override
  public void onSubscribe(final Subscription subscription) {
    this.subscription = subscription;
  }
}
