package com.revolsys.io.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;

import com.revolsys.util.MessageDigestProxy;

public class DigestOutputStream extends java.security.DigestOutputStream
  implements MessageDigestProxy {

  private long size = 0;

  public DigestOutputStream(final OutputStream stream, final MessageDigest digest) {
    super(stream, digest);
  }

  public long size() {
    return this.size;
  }

  @Override
  public void write(final byte[] b) throws IOException {
    super.write(b);
    this.size += b.length;
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    super.write(b, off, len);
    this.size += len - off;
  }

  @Override
  public void write(final int b) throws IOException {
    super.write(b);
    this.size += 1;
  }

}
