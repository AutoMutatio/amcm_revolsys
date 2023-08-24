package com.revolsys.reactive.chars;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;

public class ByteBufInputStream extends InputStream {

  private boolean complete = false;

  private boolean closed = false;

  private AtomicReference<ByteBuf> buffer = new AtomicReference<>();

  public ByteBufInputStream() {
  }

  @Override
  public int read() throws IOException {
    if (closed) {
      throw new IOException("Closed");
    }
    buffer.
    if (buffer == null || buffer.has)
    return 0;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Closed");
    }
    return super.read(b, off, len);
  }
}
