package com.revolsys.parallel.channel;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;

import com.revolsys.exception.Exceptions;
import com.revolsys.exception.WrappedInterruptedException;
import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.parallel.channel.store.IterableStore;
import com.revolsys.parallel.channel.store.ZeroBuffer;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.ExitLoopException;

public class Channel<T> implements SelectableChannelInput<T>, ChannelOutput<T>, Iterable<T> {
  public static <V> Channel<V> fromIterable(final Iterable<V> data) {
    final var store = new IterableStore<>(data);
    final var channel = new Channel<>(store);
    channel.writeClosed = true;
    return channel;
  }

  /** The Alternative class which will control the selection */
  protected MultiInputSelector alt;

  /** Flag indicating if the channel has been closed. */
  private boolean closed = false;

  /** The ChannelValueStore used to store the data for the Channel */
  protected ChannelValueStore<T> data;

  /** The monitor reads must synchronize on */
  protected ReentrantLockEx lock = new ReentrantLockEx();

  private final Condition lockCondition = this.lock.newCondition();

  /** The name of the channel. */
  private String name;

  /** Number of readers connected to the channel. */
  private int numReaders = 0;

  /** Number of writers connected to the channel. */
  private int numWriters = 0;

  /** The monitor reads must synchronize on */
  private final ReentrantLockEx readLock = new ReentrantLockEx();

  /** Flag indicating if the channel is closed for writing. */
  private boolean writeClosed;

  /** The monitor writes must synchronize on */
  private final ReentrantLockEx writeLock = new ReentrantLockEx();

  /**
   * Constructs a new Channel<T> with a ZeroBuffer ChannelValueStore.
   */
  public Channel() {
    this(new ZeroBuffer<>());
  }

  /**
   * Constructs a new Channel<T> with the specified ChannelValueStore.
   *
   * @param data The ChannelValueStore used to store the data for the Channel
   */
  public Channel(final ChannelValueStore<T> data) {
    this.data = data;
  }

  public Channel(final String name) {
    this();
    this.name = name;
  }

  public Channel(final String name, final ChannelValueStore<T> data) {
    this.name = name;
    this.data = data;
  }

  public void close() {
    this.closed = true;
  }

  @Override
  public boolean disable() {
    this.alt = null;
    return this.data.getState() != ChannelValueStore.EMPTY;
  }

  @Override
  public boolean enable(final MultiInputSelector alt) {
    try (
      var l = this.lock.lockX()) {
      if (this.data.getState() == ChannelValueStore.EMPTY) {
        this.alt = alt;
        return false;
      } else {
        return true;
      }
    }
  }

  @Override
  public void forEach(final Consumer<? super T> action) {
    try {
      while (!isClosed()) {
        final var url = read();
        action.accept(url);
      }
    } catch (final ExitLoopException | ClosedException e) {
    }
  }

  public String getName() {
    return this.name;
  }

  @Override
  public boolean isClosed() {
    if (!this.closed) {
      if (this.writeClosed) {
        if (this.data.getState() == ChannelValueStore.EMPTY) {
          close();
        }
      }
    }

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
    return read(0);
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
        if (this.data.getState() == ChannelValueStore.EMPTY) {
          try {
            try {
              if (timeout == 0) {
                this.lockCondition.await();
              } else {
                this.lockCondition.await(timeout, TimeUnit.MILLISECONDS);
              }
            } catch (final InterruptedException e) {
              throw Exceptions.toRuntimeException(e);
            }
            if (isClosed()) {
              throw new ClosedException();
            }
          } catch (final WrappedInterruptedException e) {
            close();
            this.lockCondition.signalAll();
            throw e;
          }
        }
        if (this.data.getState() == ChannelValueStore.EMPTY) {
          return null;
        } else {
          final T value = this.data.get();
          this.lockCondition.signalAll();
          return value;
        }
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

  @Override
  public String toString() {
    if (this.name == null) {
      return this.data.toString();
    } else {
      return this.name;
    }
  }

  /**
   * Writes an Object to the Channel. This method also ensures only one of the
   * writers can actually be writing at any time. All other writers are blocked
   * until it completes the write.
   *
   * @param value The object to write to the Channel.
   */
  @Override
  public void write(final T value) {
    try (
      var wl = this.writeLock.lockX()) {
      try (
        var l = this.lock.lockX()) {
        if (this.closed) {
          throw new ClosedException();
        }
        final MultiInputSelector tempAlt = this.alt;
        this.data.put(value);
        if (tempAlt != null) {
          tempAlt.schedule();
        } else {
          this.lockCondition.signalAll();
        }
        if (this.data.getState() == ChannelValueStore.FULL) {
          try {
            try {
              this.lockCondition.await();
            } catch (final InterruptedException e) {
              throw Exceptions.toRuntimeException(e);
            }
            if (this.closed) {
              throw new ClosedException();
            }
          } catch (final WrappedInterruptedException e) {
            close();
            this.lockCondition.signalAll();
            throw e;
          }
        }
      }
    }
  }

  @Override
  public BaseCloseable writeConnect() {
    try (
      var l = this.lock.lockX()) {
      if (this.writeClosed) {
        throw new IllegalStateException("Cannot connect to a closed channel");
      } else {
        this.numWriters++;
      }

    }
    return this::writeDisconnect;
  }

  @Override
  public void writeDisconnect() {
    try (
      var l = this.lock.lockX()) {
      if (!this.writeClosed) {
        this.numWriters--;
        if (this.numWriters <= 0) {
          this.writeClosed = true;
          final MultiInputSelector tempAlt = this.alt;
          if (tempAlt != null) {
            tempAlt.closeChannel();
          } else {
            this.lockCondition.signalAll();
          }
        }
      }

    }
  }
}
