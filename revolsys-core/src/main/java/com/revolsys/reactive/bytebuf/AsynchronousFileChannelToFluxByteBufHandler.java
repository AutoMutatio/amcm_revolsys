package com.revolsys.reactive.bytebuf;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.function.Consumer;

import com.revolsys.io.FileUtil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import reactor.core.publisher.FluxSink;

public class AsynchronousFileChannelToFluxByteBufHandler implements CompletionHandler<Integer, Void> {

  public static Consumer<FluxSink<ByteBuf>> create(final AsynchronousFileChannel channel) {
    return create(channel, 8192);
  }

  public static Consumer<FluxSink<ByteBuf>> create(final AsynchronousFileChannel channel,
    final int bufferSize) {
    return sink -> new AsynchronousFileChannelToFluxByteBufHandler(channel, bufferSize, sink);
  }

  private long position = 0;

  private final ByteBuffer buffer;

  private final int bufferSize;

  private final AsynchronousFileChannel channel;

  private final FluxSink<ByteBuf> sink;

  private boolean complete;

  private boolean reading = false;

  public AsynchronousFileChannelToFluxByteBufHandler(final AsynchronousFileChannel channel,
    final int bufferSize, final FluxSink<ByteBuf> sink) {
    this.channel = channel;
    this.bufferSize = bufferSize;
    this.buffer = ByteBuffer.allocateDirect(bufferSize);
    this.sink = sink;
    sink.onCancel(this::cancel);
    sink.onRequest(this::request);

  }

  public void cancel() {
    this.complete = true;
    closeChannel();
  }

  private void closeChannel() {
    FileUtil.close(this.channel);
  }

  @Override
  public void completed(final Integer count, final Void v) {
    if (this.complete) {
      closeChannel();
      return;
    } else if (count == -1) {
      boolean runComplete;
      synchronized (this) {
        runComplete = this.complete == false;
        this.complete = true;
      }
      if (runComplete) {
        closeChannel();
        this.sink.complete();
      }
    } else {
      synchronized (this) {
        this.position += count;
      }
      final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(this.bufferSize);
      this.buffer.flip();
      buffer.writeBytes(this.buffer);
      this.sink.next(buffer);

      if (this.sink.requestedFromDownstream() > 0) {
        readBuffer();
      } else if (setIdle()) {
        readIfNeeded();
      }
    }
  }

  @Override
  public void failed(final Throwable e, final Void x) {
    boolean runComplete;
    synchronized (this) {
      runComplete = this.complete == false;
      this.complete = true;
    }
    if (runComplete) {
      closeChannel();
    }
    this.sink.error(e);
  }

  private void readBuffer() {
    long position;
    synchronized (this) {
      position = this.position;
    }
    this.buffer.clear();
    this.channel.read(this.buffer, position, null, this);
  }

  private void readIfNeeded() {
    if (this.sink.requestedFromDownstream() > 0 && setReading()) {
      readBuffer();
    }
  }

  public void request(final long n) {
    readIfNeeded();
  }

  private synchronized boolean setIdle() {
    if (this.reading) {
      this.reading = false;
      return true;
    } else {
      return false;
    }
  }

  private synchronized boolean setReading() {
    if (this.reading) {
      return false;
    } else {
      this.reading = true;
      return true;
    }
  }
}
