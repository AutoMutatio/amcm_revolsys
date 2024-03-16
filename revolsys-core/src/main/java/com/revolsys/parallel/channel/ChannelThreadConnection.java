package com.revolsys.parallel.channel;

import com.revolsys.collection.map.ThreadLocalMap;
import com.revolsys.parallel.ReentrantLockEx;

public class ChannelThreadConnection {

  private static final ReentrantLockEx LOCK = new ReentrantLockEx();

  private static final ThreadLocalMap<ChannelOutput<?>, ChannelThreadConnection> CONNECTIONS = new ThreadLocalMap<>();

  public static void writeConnect(final ChannelOutput<?> channel) {
    try (
      var l = LOCK.lockX()) {
      ChannelThreadConnection connection = CONNECTIONS.get(channel);
      if (connection == null) {
        connection = new ChannelThreadConnection(channel);
        CONNECTIONS.put(channel, connection);
        channel.writeConnect();
      }
    }
  }

  private final ChannelOutput<?> channel;

  private ChannelThreadConnection(final ChannelOutput<?> channel) {
    this.channel = channel;
  }

  @Override
  public void finalize() {
    this.channel.writeDisconnect();
  }
}
