package com.revolsys.reactive.bytebuf;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.function.Function;

import org.reactivestreams.Subscription;

import com.revolsys.io.FileUtil;

import io.netty.buffer.ByteBuf;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

public class AsynchronousFileChannelFromFluxByteBufHandler {

  private class SourceSubscriber implements CoreSubscriber<ByteBuf> {

    private Subscription subscription;

    public void cancel() {
      if (setCompleted()) {
        this.subscription.cancel();
      }
    }

    @Override
    public void onComplete() {
      if (setCompleted()) {
        AsynchronousFileChannelFromFluxByteBufHandler.this.sink.complete();
      }
    }

    @Override
    public void onError(final Throwable e) {
      if (setCompleted()) {
        AsynchronousFileChannelFromFluxByteBufHandler.this.sink.error(e);
      }
    }

    @Override
    public void onNext(final ByteBuf buffer) {
      if (setStatus(Status.WRITING, Status.REQUEST_MORE)) {
        final var parent = AsynchronousFileChannelFromFluxByteBufHandler.this;
        final ByteBuffer temp = parent.buffer;
        temp.clear();
        final int count = buffer.readableBytes();
        temp.limit(count);
        buffer.readBytes(temp);
        buffer.release();
        temp.flip();
        parent.channel.write(temp, parent.position, null, parent.targetCompletionHandler);
        parent.position += count;
      }
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
      final boolean requestMore = AsynchronousFileChannelFromFluxByteBufHandler.this.status == Status.REQUEST_MORE;
      this.subscription = subscription;
      if (requestMore) {
        this.subscription.request(1);
      }
    }

    public void request(final long n) {
      if (setStatus(Status.REQUEST_MORE, Status.IDLE)) {
        if (this.subscription != null) {
          this.subscription.request(1);
        }
      }
    }
  };

  private enum Status {
    IDLE, WRITING, WRITE_RECEIVED, REQUEST_MORE, COMPLETE;
  }

  private class TargetCompletionHandler implements CompletionHandler<Integer, Void> {
    @Override
    public void completed(final Integer count, final Void attachment) {
      final AsynchronousFileChannelFromFluxByteBufHandler parent = AsynchronousFileChannelFromFluxByteBufHandler.this;
      if (setStatus(Status.WRITE_RECEIVED, Status.WRITING)) {
        parent.sink.next(count);
        setStatus(Status.IDLE, Status.WRITE_RECEIVED);
        if (parent.sink.requestedFromDownstream() > 0) {
          AsynchronousFileChannelFromFluxByteBufHandler.this.sourceSubscriber.request(1);
        }
      }
    }

    @Override
    public void failed(final Throwable e, final Void attachment) {
      if (setCompleted()) {
        AsynchronousFileChannelFromFluxByteBufHandler.this.sink.error(e);
      }
    }

  }

  public static Function<AsynchronousFileChannel, Flux<Integer>> create(
    final Flux<ByteBuf> source) {
    return create(source, 8192);
  }

  public static Function<AsynchronousFileChannel, Flux<Integer>> create(final Flux<ByteBuf> source,
    final int bufferSize) {
    return channel -> Flux.create(
      sink -> new AsynchronousFileChannelFromFluxByteBufHandler(source, channel, bufferSize, sink));
  }

  private final CompletionHandler<Integer, Void> targetCompletionHandler = new TargetCompletionHandler();

  private long position = 0;

  private final ByteBuffer buffer;

  private final AsynchronousFileChannel channel;

  private final FluxSink<Integer> sink;

  private final SourceSubscriber sourceSubscriber = new SourceSubscriber();;

  private Status status = Status.IDLE;

  public AsynchronousFileChannelFromFluxByteBufHandler(final Flux<ByteBuf> source,
    final AsynchronousFileChannel channel, final int bufferSize, final FluxSink<Integer> sink) {
    this.channel = channel;
    this.buffer = ByteBuffer.allocateDirect(bufferSize);
    this.sink = sink;
    source.subscribe(this.sourceSubscriber);
    sink.onCancel(this.sourceSubscriber::cancel);
    sink.onRequest(this.sourceSubscriber::request);
  }

  private boolean setCompleted() {
    boolean set = false;
    synchronized (this) {
      if (this.status != Status.COMPLETE) {
        this.status = Status.COMPLETE;
        set = true;
      }
    }
    if (set) {
      FileUtil.closeSilent(this.channel);
    }
    return set;
  }

  private boolean setStatus(final Status newStatus, final Status expected) {
    synchronized (this) {
      if (this.status == Status.COMPLETE) {
        return false;
      } else if (this.status == expected) {
        this.status = newStatus;
        return true;
      } else {
        return false;
      }
    }
  }
}
