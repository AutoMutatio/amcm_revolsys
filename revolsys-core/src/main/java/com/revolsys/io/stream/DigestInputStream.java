package com.revolsys.io.stream;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

import com.revolsys.util.MessageDigestProxy;

public class DigestInputStream extends java.security.DigestInputStream
  implements MessageDigestProxy {

  private long size;

  public DigestInputStream(final InputStream stream, final MessageDigest digest) {
    super(stream, digest);
  }

  public long getSize() {
    return this.size;
  }

  @Override
  public int read() throws IOException {
    final int b = super.read();
    if (b >= 0) {
      this.size++;
    }
    return b;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    final int count = super.read(b, off, len);
    if (count >= 0) {
      this.size += count;
    }
    return count;
  }
}
