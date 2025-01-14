package com.revolsys.parallel.channel;

import com.revolsys.util.BaseCloseable;

public interface ChannelOutput<T> {
  boolean isClosed();

  /**
   * Writes an Object to the Channel. This method also ensures only one of the
   * writers can actually be writing at any time. All other writers are blocked
   * until it completes the write.
   *
   * @param value The object to write to the Channel.
   */
  void write(final T value);

  BaseCloseable writeConnect();

  void writeDisconnect();
}
