package com.revolsys.io;

import com.revolsys.util.BaseCloseable;

public class CloseableWrapper implements BaseCloseable {

  private BaseCloseable closeable;

  public CloseableWrapper(final BaseCloseable closeable) {
    this.closeable = closeable;
  }

  @Override
  public void close() {
    final BaseCloseable closeable = this.closeable;
    this.closeable = null;
    if (closeable != null) {
      closeable.close();
    }
  }

  @Override
  public String toString() {
    final BaseCloseable closeable = this.closeable;
    if (closeable == null) {
      return super.toString();
    } else {
      return closeable.toString();
    }
  }
}
