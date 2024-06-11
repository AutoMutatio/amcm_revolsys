package com.revolsys.net.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.apache.http.entity.AbstractHttpEntity;

import com.revolsys.io.IgnoreCloseDelegatingOutputStream;

public class ByteBufferHttpEntity extends AbstractHttpEntity {
  public static interface WriteTo {
    void writeTo(OutputStream out) throws IOException;
  }

  private final long length;

  private final ByteBuffer buffer;

  public ByteBufferHttpEntity(final ByteBuffer buffer) {
    this.buffer = buffer;
    this.length = buffer.limit();
  }

  @Override
  public InputStream getContent() throws IOException, UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getContentLength() {
    return this.length;
  }

  @Override
  public boolean isRepeatable() {
    return true;
  }

  @Override
  public boolean isStreaming() {
    return false;
  }

  @Override
  public void writeTo(final OutputStream out) throws IOException {
    this.buffer.rewind();
    try (
      WritableByteChannel outChannel = java.nio.channels.Channels
        .newChannel(new IgnoreCloseDelegatingOutputStream(out))) {
      while (this.buffer.hasRemaining()) {
        outChannel.write(this.buffer);
      }
    }
  }

}
