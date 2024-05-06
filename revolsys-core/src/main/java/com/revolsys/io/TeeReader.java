package com.revolsys.io;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

public class TeeReader extends Reader {

  private final Reader in;

  private final java.io.Writer out;

  public TeeReader(final Reader in, final Writer out) {
    super();
    this.in = in;
    this.out = out;
  }

  @Override
  public void close() throws IOException {
    this.in.close();
  }

  @Override
  public int read(final char[] cbuf, final int off, final int len) throws IOException {
    final int count = this.in.read(cbuf, off, len);
    if (len > 0) {
      this.out.write(cbuf, off, count);
    }
    return count;
  }

}
