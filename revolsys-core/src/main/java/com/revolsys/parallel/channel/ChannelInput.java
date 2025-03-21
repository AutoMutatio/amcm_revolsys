package com.revolsys.parallel.channel;

import com.revolsys.util.BaseCloseable;

public interface ChannelInput<T> extends Iterable<T> {
  boolean isClosed();

  default boolean isOpen() {
    return !isClosed();
  }

  /**
   * Reads an Object from the Channel. This method also ensures only one of the
   * readers can actually be reading at any time. All other readers are blocked
   * until it completes the read.
   *
   * @return The object returned from the Channel.
   */
  T read();

  /**
   * Reads an Object from the Channel. This method also ensures only one of the
   * readers can actually be reading at any time. All other readers are blocked
   * until it completes the read. If no data is available to be read after the
   * timeout the method will return null.
   *
   * @param timeout The maximum time to wait in milliseconds.
   * @return The object returned from the Channel.
   */
  T read(long timeout);

  BaseCloseable readConnect();

  void readDisconnect();
}
