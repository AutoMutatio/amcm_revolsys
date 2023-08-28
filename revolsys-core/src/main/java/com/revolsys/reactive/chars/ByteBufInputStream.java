package com.revolsys.reactive.chars;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

import org.reactivestreams.Subscription;

import io.netty.buffer.ByteBuf;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

public class ByteBufInputStream extends InputStream {

  private boolean complete = false;

  private final boolean closed = false;

  private ByteBuf buffer;

  private Throwable error;

  private Subscription subscription;

  private final Object sync = new Object();

  ByteBufInputStream() {
  }

  private ByteBuf getBuffer() throws IOException {
    boolean requested = false;
    while (true) {
      synchronized (this.sync) {
        if (this.error != null) {
          this.subscription.cancel();
          if (this.error instanceof final IOException ioe) {
            throw ioe;
          } else {
            throw new IOException(this.error);
          }
        } else if (this.closed) {
          this.subscription.cancel();
          throw new IOException("Closed");
        } else if (this.buffer != null && this.buffer.isReadable()) {
          return this.buffer;
        } else {
          if (this.buffer != null) {
            this.buffer.release();
            this.buffer = null;
          }
          if (this.complete) {
            return null;
          } else if (requested) {
            try {
              this.sync.wait();
            } catch (final InterruptedException e) {
              this.subscription.cancel();
              throw new InterruptedIOException();
            }
          } else {
            requested = true;
            this.subscription.request(1);
          }
        }
      }
    }
  }

  @Override
  public int read() throws IOException {
    final ByteBuf buffer = getBuffer();
    if (buffer == null) {
      return -1;
    } else {
      return buffer.readByte() & 0xFF;
    }
  }

  @Override
  public int read(final byte[] bytes, final int offset, final int length) throws IOException {
    final ByteBuf buffer = getBuffer();
    if (buffer == null) {
      return -1;
    } else {
      final int readCount = Math.min(buffer.readableBytes(), length);
      if (readCount > 0) {
        buffer.readBytes(bytes, offset, readCount);
      }
      return readCount;
    }
  }

  void subscribe(final Flux<ByteBuf> source) {
    source.subscribe(new CoreSubscriber<ByteBuf>() {

      @Override
      public void onComplete() {
        final Object sync = ByteBufInputStream.this.sync;
        synchronized (sync) {
          ByteBufInputStream.this.complete = true;
          sync.notifyAll();
        }
      }

      @Override
      public void onError(final Throwable error) {
        final Object sync = ByteBufInputStream.this.sync;
        synchronized (sync) {
          if (ByteBufInputStream.this.error == null) {
            ByteBufInputStream.this.error = error;
            sync.notifyAll();
          }
        }
      }

      @Override
      public void onNext(final ByteBuf buffer) {
        final Object sync = ByteBufInputStream.this.sync;
        synchronized (sync) {
          ByteBufInputStream.this.buffer = buffer.retain();
          sync.notifyAll();
        }
      }

      @Override
      public void onSubscribe(final Subscription subscription) {
        ByteBufInputStream.this.subscription = subscription;
      }
    });
  }
}
