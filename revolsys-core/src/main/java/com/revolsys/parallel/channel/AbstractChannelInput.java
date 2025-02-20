package com.revolsys.parallel.channel;

import java.util.Iterator;
import java.util.concurrent.locks.Condition;

import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.util.BaseCloseable;

public abstract class AbstractChannelInput<T> implements ChannelInput<T> {
  /** Flag indicating if the channel has been closed. */
  private boolean closed = false;

  /** The monitor reads must synchronize on */
  private final ReentrantLockEx lock = new ReentrantLockEx();

  private final Condition lockCondition = this.lock.newCondition();

  /** The name of the channel. */
  private String name;

  /** Number of readers connected to the channel. */
  private int numReaders = 0;

  /** The monitor reads must synchronize on */
  private final ReentrantLockEx readLock = new ReentrantLockEx();

  public AbstractChannelInput() {

  }

  public AbstractChannelInput(final String name) {
    this.name = name;
  }

  public void close() {
    this.closed = true;
  }

  public String getName() {
    return this.name;
  }

  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public Iterator<T> iterator() {
    return new ChannelInputIterator<>(this);
  }

  /**
   * Reads an Object from the Channel. This method also ensures only one of the
   * readers can actually be reading at any time. All other readers are blocked
   * until it completes the read.
   *
   * @return The object returned from the Channel.
   */
  @Override
  public T read() {
    try (
      var rl = this.readLock.lockX()) {
      try (
        var l = this.lock.lockX()) {
        if (isClosed()) {
          throw new ClosedException();
        }
        return readDo();
      }
    }
  }

  /**
   * Reads an Object from the Channel. This method also ensures only one of the
   * readers can actually be reading at any time. All other readers are blocked
   * until it completes the read. If no data is available to be read after the
   * timeout the method will return null.
   *
   * @param timeout The maximum time to wait in milliseconds.
   * @return The object returned from the Channel.
   */
  @Override
  public T read(final long timeout) {
    try (
      var rl = this.readLock.lockX()) {
      try (
        var l = this.lock.lockX()) {
        if (isClosed()) {
          throw new ClosedException();
        }
        return readDo(timeout);
      }
    }
  }

  @Override
  public BaseCloseable readConnect() {
    try (
      var l = this.lock.lockX()) {
      if (isClosed()) {
        throw new IllegalStateException("Cannot connect to a closed channel");
      } else {
        this.numReaders++;
      }
    }
    return this::readDisconnect;
  }

  @Override
  public void readDisconnect() {
    try (
      var l = this.lock.lockX()) {
      if (!this.closed) {
        this.numReaders--;
        if (this.numReaders <= 0) {
          close();
          this.lockCondition.signalAll();
        }
      }

    }
  }

  protected abstract T readDo();

  protected abstract T readDo(long timeout);

  @Override
  public String toString() {
    if (this.name == null) {
      return super.toString();
    } else {
      return this.name;
    }
  }
}
