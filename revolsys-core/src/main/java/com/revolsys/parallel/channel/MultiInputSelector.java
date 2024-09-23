package com.revolsys.parallel.channel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import com.revolsys.exception.Exceptions;
import com.revolsys.parallel.ReentrantLockEx;

public class MultiInputSelector {
  private int enabledChannels = 0;

  private int guardEnabledChannels = 0;

  private long maxWait;

  private final ReentrantLockEx lock = new ReentrantLockEx();

  private final Condition lockCondition = this.lock.newCondition();

  private boolean scheduled;

  void closeChannel() {
    try (
      var l = this.lock.lockX()) {
      this.enabledChannels--;
      if (this.enabledChannels <= 0) {
        this.lockCondition.signalAll();
      }
    }
  }

  private int disableChannels(final List<? extends SelectableInput> channels) {
    int closedCount = 0;
    int selected = -1;
    for (int i = channels.size() - 1; i >= 0; i--) {
      final SelectableInput channel = channels.get(i);
      if (channel.disable()) {
        selected = i;
      } else if (channel.isClosed()) {
        closedCount++;
      }
    }
    if (closedCount == channels.size()) {
      throw new ClosedException();
    } else {
      return selected;
    }

  }

  private int disableChannels(final List<? extends SelectableInput> channels,
    final List<Boolean> guard) {
    int closedCount = 0;
    int selected = -1;
    for (int i = channels.size() - 1; i >= 0; i--) {
      final SelectableInput channel = channels.get(i);
      if (guard.get(i) && channel.disable()) {
        selected = i;
      } else if (channel == null || channel.isClosed()) {
        closedCount++;
      }
    }
    if (closedCount == channels.size()) {
      throw new ClosedException();
    } else {
      return selected;
    }
  }

  private boolean enableChannels(final List<? extends SelectableInput> channels) {
    this.enabledChannels = 0;
    this.scheduled = false;
    this.maxWait = Long.MAX_VALUE;
    int closedCount = 0;
    for (final SelectableInput channel : channels) {
      if (!channel.isClosed()) {
        if (channel.enable(this)) {
          this.enabledChannels++;
          return true;
        } else if (channel instanceof Timer) {
          final Timer timer = (Timer)channel;
          this.maxWait = Math.min(this.maxWait, timer.getWaitTime());
        }
      } else {
        closedCount++;
      }
    }
    return closedCount == channels.size();
  }

  private boolean enableChannels(final List<? extends SelectableInput> channels,
    final List<Boolean> guard) {
    this.enabledChannels = 0;
    this.scheduled = false;
    this.maxWait = Long.MAX_VALUE;
    int closedCount = 0;
    int activeChannelCount = 0;
    for (int i = 0; i < channels.size(); i++) {
      final SelectableInput channel = channels.get(i);
      if (guard.get(i)) {
        activeChannelCount++;
        if (!channel.isClosed()) {
          if (channel.enable(this)) {
            this.enabledChannels++;
            return true;
          } else if (channel instanceof Timer) {
            final Timer timer = (Timer)channel;
            this.maxWait = Math.min(this.maxWait, timer.getWaitTime());
          }
        } else {
          closedCount++;
        }
      }
    }
    this.guardEnabledChannels = activeChannelCount - closedCount;
    return closedCount == activeChannelCount;
  }

  void schedule() {
    try (
      var l = this.lock.lockX()) {
      this.scheduled = true;
      this.lockCondition.signalAll();
    }
  }

  public synchronized int select(final List<? extends SelectableInput> channels) {
    return select(Long.MAX_VALUE, channels);
  }

  public synchronized int select(final List<? extends SelectableInput> channels,
    final boolean skip) {
    if (skip) {
      enableChannels(channels);
      return disableChannels(channels);
    } else {
      return select(channels);
    }
  }

  public synchronized int select(final List<? extends SelectableInput> channels,
    final List<Boolean> guard) {
    return select(channels, guard, Long.MAX_VALUE);
  }

  public synchronized int select(final List<? extends SelectableInput> channels,
    final List<Boolean> guard, final boolean skip) {
    if (skip) {
      enableChannels(channels, guard);
      return disableChannels(channels, guard);
    } else {
      return select(channels, guard);
    }
  }

  public synchronized int select(final List<? extends SelectableInput> channels,
    final List<Boolean> guard, final long msecs) {
    return select(channels, guard, msecs, TimeUnit.MILLISECONDS);
  }

  public synchronized int select(final List<? extends SelectableInput> channels,
    final List<Boolean> guard, final long time, final TimeUnit unit) {
    if (!enableChannels(channels, guard) && this.guardEnabledChannels > 0) {
      try (
        var l = this.lock.lockX()) {
        if (!this.scheduled) {
          try {
            final long waitTime = Math.min(unit.toNanos(time),
              TimeUnit.MILLISECONDS.toNanos(this.maxWait));
            if (waitTime == 0 || waitTime == Long.MAX_VALUE) {
              this.lockCondition.await();
            } else {
              this.lockCondition.awaitNanos(waitTime);
            }
          } catch (final InterruptedException e) {
            throw Exceptions.toRuntimeException(e);
          }
        }
      }
    }
    return disableChannels(channels, guard);
  }

  public synchronized int select(final long msecs, final List<? extends SelectableInput> channels) {
    return select(msecs, TimeUnit.MILLISECONDS, channels);
  }

  public synchronized int select(final long msecs, final SelectableInput... channels) {
    return select(msecs, TimeUnit.MILLISECONDS, channels);
  }

  public synchronized int select(final long time, final TimeUnit unit,
    final List<? extends SelectableInput> channels) {
    if (!enableChannels(channels)) {
      try (
        var l = this.lock.lockX()) {
        if (!this.scheduled) {
          try {
            final long waitTime = Math.min(unit.toNanos(time),
              TimeUnit.MILLISECONDS.toNanos(this.maxWait));
            if (waitTime == 0 || waitTime == Long.MAX_VALUE) {
              this.lockCondition.await();
            } else {
              this.lockCondition.awaitNanos(waitTime);
            }
          } catch (final InterruptedException e) {
            throw Exceptions.toRuntimeException(e);
          }
        }
      }
    }
    return disableChannels(channels);
  }

  public synchronized int select(final long time, final TimeUnit unit,
    final SelectableInput... channels) {
    return select(time, unit, Arrays.asList(channels));
  }

  public synchronized int select(final SelectableInput... channels) {
    return select(Long.MAX_VALUE, channels);
  }

  public synchronized int select(final SelectableInput[] channels, final boolean skip) {
    return select(Arrays.asList(channels), skip);
  }

  public synchronized int select(final SelectableInput[] channels, final boolean[] guard) {
    return select(channels, guard, Long.MAX_VALUE);
  }

  public synchronized int select(final SelectableInput[] channels, final boolean[] guard,
    final boolean skip) {
    final List<Boolean> guardList = new ArrayList<>();
    for (final boolean enabled : guard) {
      guardList.add(enabled);
    }
    return select(Arrays.asList(channels), guardList, skip);
  }

  public synchronized int select(final SelectableInput[] channels, final boolean[] guard,
    final long msecs) {
    return select(channels, guard, msecs, TimeUnit.MILLISECONDS);
  }

  public synchronized int select(final SelectableInput[] channels, final boolean[] guard,
    final long time, final TimeUnit unit) {
    final List<Boolean> guardList = new ArrayList<>();
    for (final boolean enabled : guard) {
      guardList.add(enabled);
    }
    return select(Arrays.asList(channels), guardList, time, unit);
  }

  public synchronized <T extends SelectableInput> T selectChannelInput(final List<T> channels) {
    final int index = select(Long.MAX_VALUE, channels);
    if (index == -1) {
      return null;
    } else {
      return channels.get(index);
    }
  }

}
