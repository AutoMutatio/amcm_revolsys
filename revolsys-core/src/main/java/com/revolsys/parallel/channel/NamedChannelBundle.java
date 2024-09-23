package com.revolsys.parallel.channel;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;

import com.revolsys.exception.Exceptions;
import com.revolsys.exception.WrappedInterruptedException;
import com.revolsys.parallel.ReentrantLockEx;

public class NamedChannelBundle<T> {

  /** Flag indicating if the channel has been closed. */
  private boolean closed = false;

  /** The monitor reads must synchronize on */
  private final ReentrantLockEx lock = new ReentrantLockEx();

  private final Condition lockCondition = this.lock.newCondition();

  /** The name of the channel. */
  private String name;

  /** Number of readers connected to the channel. */
  private int numReaders = 0;

  /** Number of writers connected to the channel. */
  private int numWriters = 0;

  private int readerNotifyCount = 0;

  /** The monitor reads must synchronize on */
  private final ReentrantLockEx readLock = new ReentrantLockEx();

  private AtomicLong sequence = new AtomicLong();

  private Map<String, Queue<Long>> sequenceQueueByName = new HashMap<>();

  /** The ChannelValueStore used to store the valueQueueByName for the Channel */
  protected Map<String, Queue<T>> valueQueueByName = new HashMap<>();

  /** Flag indicating if the channel is closed for writing. */
  private boolean writeClosed;

  /** The monitor writes must synchronize on */
  private final ReentrantLockEx writeLock = new ReentrantLockEx();

  public NamedChannelBundle() {
  }

  public NamedChannelBundle(final String name) {
    this.name = name;
  }

  public void close() {
    this.closed = true;
    try (
      var l = this.lock.lockX()) {
      this.valueQueueByName = null;
      this.sequence = null;
      this.sequenceQueueByName = null;
      this.lockCondition.signalAll();
    }
  }

  public String getName() {
    return this.name;
  }

  private Queue<T> getNextValueQueue(Collection<String> names) {
    String selectedName = null;
    long lowestSequence = Long.MAX_VALUE;
    if (names == null) {
      names = this.sequenceQueueByName.keySet();
    }
    for (final String name : names) {
      final Queue<Long> sequenceQueue = this.sequenceQueueByName.get(name);
      if (sequenceQueue != null && !sequenceQueue.isEmpty()) {
        final long sequence = sequenceQueue.peek();
        if (sequence < lowestSequence) {
          lowestSequence = sequence;
          selectedName = name;
        }
      }
    }
    if (selectedName == null) {
      return null;
    } else {
      final Queue<Long> sequenceQueue = this.sequenceQueueByName.get(selectedName);
      sequenceQueue.remove();
      return getValueQueue(selectedName);
    }
  }

  private Queue<Long> getSequenceQueue(final String name) {
    Queue<Long> queue = this.sequenceQueueByName.get(name);
    if (queue == null) {
      queue = new LinkedList<>();
      this.sequenceQueueByName.put(name, queue);
    }
    return queue;
  }

  private Queue<T> getValueQueue(final String name) {
    Queue<T> queue = this.valueQueueByName.get(name);
    if (queue == null) {
      queue = new LinkedList<>();
      this.valueQueueByName.put(name, queue);
    }
    return queue;
  }

  public boolean isClosed() {
    if (!this.closed) {
      if (this.writeClosed) {
        boolean empty = true;
        try (
          var l = this.lock.lockX()) {
          for (final Queue<T> queue : this.valueQueueByName.values()) {
            if (!queue.isEmpty()) {
              empty = false;
            }
          }
          if (empty) {
            close();
          }
        }
      }
    }

    return this.closed;
  }

  public void notifyReaders() {
    try (
      var l = this.lock.lockX()) {
      this.readerNotifyCount++;
      this.lockCondition.signalAll();
    }
  }

  /**
   * Reads an Object from the Channel. This method also ensures only one of the
   * readers can actually be reading at any time. All other readers are blocked
   * until it completes the read.
   *
   * @return The object returned from the Channel.
   */
  public T read() {
    return read(0, Collections.<String> emptyList());
  }

  public T read(final Collection<String> names) {
    return read(0, names);
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
  public T read(final long timeout) {
    return read(timeout, Collections.<String> emptyList());
  }

  public T read(final long timeout, final Collection<String> names) {
    try (
      var rl = this.readLock.lockX()) {
      try (
        var l = this.lock.lockX()) {
        final int readerNotifyCount = this.readerNotifyCount;
        try {
          long maxTime = 0;
          if (timeout > 0) {
            maxTime = System.currentTimeMillis() + timeout;
          }
          if (isClosed()) {
            throw new ClosedException();
          }
          Queue<T> queue = getNextValueQueue(names);
          if (timeout == 0) {
            while (queue == null && readerNotifyCount == this.readerNotifyCount) {
              try {
                this.lockCondition.await();
              } catch (final InterruptedException e) {
                throw Exceptions.toRuntimeException(e);
              }
              if (isClosed()) {
                throw new ClosedException();
              }

              queue = getNextValueQueue(names);
            }
          } else if (timeout > 0) {
            long waitTime = maxTime - System.currentTimeMillis();
            while (queue == null && waitTime > 0 && readerNotifyCount == this.readerNotifyCount) {
              final long milliSeconds = waitTime;
              try {
                this.lockCondition.await(milliSeconds, TimeUnit.MILLISECONDS);
              } catch (final InterruptedException e) {
                throw Exceptions.toRuntimeException(e);
              }
              if (isClosed()) {
                throw new ClosedException();
              }

              queue = getNextValueQueue(names);
              waitTime = maxTime - System.currentTimeMillis();
            }
          } else {
            queue = getNextValueQueue(names);
          }
          if (queue == null) {
            return null;
          } else {
            final T value = queue.remove();
            this.lockCondition.signalAll();
            return value;
          }
        } catch (final WrappedInterruptedException e) {
          close();
          this.lockCondition.signalAll();
          throw e;
        }
      }
    }
  }

  public T read(final long timeout, final String... names) {
    return read(timeout, Arrays.asList(names));
  }

  public T read(final String... names) {
    return read(0, Arrays.asList(names));
  }

  public void readConnect() {
    try (
      var l = this.lock.lockX()) {
      if (isClosed()) {
        throw new IllegalStateException("Cannot connect to a closed channel");
      } else {
        this.numReaders++;
      }

    }
  }

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

  public Collection<T> remove(final String name) {
    try (
      var l = this.lock.lockX()) {
      this.sequenceQueueByName.remove(name);
      final Queue<T> values = this.valueQueueByName.remove(name);
      this.lockCondition.signalAll();
      return values;
    }
  }

  /**
   * Writes a named Object to the Channel. This method also ensures only one of
   * the writers can actually be writing at any time. All other writers are
   * blocked until it completes the write. The channel can never be full so it
   * does not block on write.
   *
   * @param value The object to write to the Channel.
   */
  public void write(final String name, final T value) {
    try (
      var wl = this.writeLock.lockX()) {
      try (
        var l = this.lock.lockX()) {
        if (this.closed) {
          this.lockCondition.signalAll();
          throw new ClosedException();
        } else {
          final Long sequence = this.sequence.getAndIncrement();
          final Queue<Long> sequenceQueue = getSequenceQueue(name);
          sequenceQueue.add(sequence);

          final Queue<T> queue = getValueQueue(name);
          queue.add(value);

          this.lockCondition.signalAll();
        }
      }
    }
  }

  public void writeConnect() {
    try (
      var l = this.lock.lockX()) {
      if (this.writeClosed) {
        throw new IllegalStateException("Cannot connect to a closed channel");
      } else {
        this.numWriters++;
      }

    }
  }

  public void writeDisconnect() {
    try (
      var l = this.lock.lockX()) {
      if (!this.writeClosed) {
        this.numWriters--;
        if (this.numWriters <= 0) {
          this.writeClosed = true;
          this.lockCondition.signalAll();
        }
      }
    }
  }
}
