package com.revolsys.parallel.channel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import com.revolsys.collection.list.Lists;
import com.revolsys.parallel.ThreadInterruptedException;

import reactor.core.publisher.Flux;

public class MultiInputSelector extends AbstractMultiInputSelector {
  public interface Guard {
    static Guard ENABLED = () -> true;

    static Guard DISABLED = () -> false;

    static Guard from(final boolean enabled) {
      if (enabled) {
        return ENABLED;
      } else {
        return DISABLED;
      }
    }

    boolean isEnabled();
  }

  private int enabledInputCount = 0;

  private int guardEnabledInputCount = 0;

  private Instant maxWait;

  private final Object monitor = new Object();

  private boolean scheduled;

  private List<SelectableInput> inputs = Collections.emptyList();

  private List<Guard> guards = Collections.emptyList();

  private boolean blockOnNoInput = false;

  public MultiInputSelector addInput(final SelectableInput input) {
    return addInput(input, Guard.ENABLED);
  }

  public MultiInputSelector addInput(final SelectableInput input, final Guard guard) {
    final List<SelectableInput> inputs = Collections.singletonList(input);
    final List<Guard> guards = Collections.singletonList(guard);
    return addInputs(inputs, guards);
  }

  public MultiInputSelector addInputs(final Iterable<? extends SelectableInput> inputs) {
    final List<Guard> guards = Collections.emptyList();
    return addInputs(inputs, guards);
  }

  public MultiInputSelector addInputs(final Iterable<? extends SelectableInput> inputs,
    final Iterable<Guard> guards) {
    synchronized (this.monitor) {
      // Outer synchronization required to safely get existing inputs and guards
      final List<SelectableInput> newInputs = Lists.toArray(this.inputs);
      final List<Guard> newGuards = Lists.toArray(this.guards);
      return updateInputs(newInputs, newGuards, inputs, guards);
    }
  }

  public MultiInputSelector addInputs(final SelectableInput... inputs) {
    return addInputs(Arrays.asList(inputs));
  }

  public <C extends SelectableInput> Flux<C> asFlux() {
    return Flux.generate(sink -> {
      while (true) {
        try {
          final C input = selectInput();
          if (input != null) {
            sink.next(input);
            return;
          }
        } catch (final ClosedException e) {
          sink.complete();
          return;
        } catch (final Throwable e) {
          sink.error(e);
          return;
        }
      }
    });
  }

  @Override
  void closeInput(final SelectableInput source) {
    synchronized (this.monitor) {
      this.enabledInputCount--;
      if (this.enabledInputCount <= 0) {
        this.monitor.notifyAll();
      }
    }
  }

  private int disableInputs(final List<? extends SelectableInput> inputs,
    final List<Guard> guards) {
    int closedCount = 0;
    int selected = -1;
    for (int i = inputs.size() - 1; i >= 0; i--) {
      final SelectableInput input = inputs.get(i);
      if (guards.get(i).isEnabled() && input.disable(this)) {
        selected = i;
      } else if (input.isClosed()) {
        closedCount++;
      }
    }
    if (!inputs.isEmpty() && closedCount == inputs.size()) {
      throw new ClosedException();
    } else {
      return selected;
    }
  }

  private boolean enableInputs(final List<? extends SelectableInput> inputs,
    final List<Guard> guard) {
    this.enabledInputCount = 0;
    this.scheduled = false;
    this.maxWait = Instant.MAX;
    int closedCount = 0;
    int activeInputCount = 0;
    for (int i = 0; i < inputs.size(); i++) {
      final SelectableInput input = inputs.get(i);
      if (guard.get(i).isEnabled()) {
        activeInputCount++;
        if (!input.isClosed()) {
          if (input.enable(this)) {
            this.enabledInputCount++;
            return true;
          } else if (input instanceof Timer) {
            final Timer timer = (Timer)input;
            final Instant disabledTime = timer.getDisabledTime(this);
            if (disabledTime.isBefore(this.maxWait)) {
              this.maxWait = disabledTime;
            }
          }
        } else {
          closedCount++;
        }
      }
    }
    this.guardEnabledInputCount = activeInputCount - closedCount;
    return closedCount == activeInputCount;
  }

  public boolean isEnabled(final int index) {
    return this.guards.get(index).isEnabled();
  }

  @Override
  void schedule(final SelectableInput source) {
    synchronized (this.monitor) {
      this.scheduled = true;
      this.monitor.notifyAll();
    }
  }

  public synchronized int select() {
    return select(Long.MAX_VALUE);
  }

  public synchronized int select(final boolean skip) {
    if (skip) {
      final List<SelectableInput> inputs;
      final List<Guard> guards;
      synchronized (this.monitor) {
        inputs = this.inputs;
        guards = this.guards;
      }
      enableInputs(inputs, guards);
      return disableInputs(inputs, guards);
    } else {
      return select();
    }
  }

  public synchronized int select(final long msecs) {
    return select(msecs, 0);
  }

  public synchronized int select(final long msecs, final int nsecs) {
    return selectInternal(msecs, nsecs, (i, input) -> i);
  }

  public synchronized <T extends SelectableInput> T selectInput() {
    return selectInternal(Long.MAX_VALUE, 0, (final Integer i, final T input) -> input);
  }

  private <I, R> R selectInternal(final long msecs, final int nsecs,
    final BiFunction<Integer, I, R> handler) {
    List<SelectableInput> inputs;
    List<Guard> guards;
    do {
      synchronized (this.monitor) {
        inputs = this.inputs;
        guards = this.guards;
      }
      if (!enableInputs(inputs, guards)
        && (this.blockOnNoInput || this.guardEnabledInputCount > 0)) {
        if (msecs + nsecs >= 0) {
          synchronized (this.monitor) {
            if (!this.scheduled) {
              try {
                try {
                  if (Instant.MAX == this.maxWait) {
                    this.monitor.wait(msecs, nsecs);
                  } else {
                    final Instant now = Instant.now();
                    final long waitTime = Duration.between(now, this.maxWait).toMillis();
                    if (waitTime >= 0) {
                      this.monitor.wait(waitTime);
                    }
                  }
                } catch (final InterruptedException e) {
                  throw new ThreadInterruptedException(e);
                }
              } catch (final ThreadInterruptedException e) {
                throw new ClosedException(e);
              }
            }
          }
        }
      }
      final int selectedInput = disableInputs(inputs, guards);
      if (selectedInput != -1) {
        @SuppressWarnings("unchecked")
        final I input = (I)inputs.get(selectedInput);
        return handler.apply(selectedInput, input);
      }
    } while (this.inputs != inputs || this.blockOnNoInput);
    return handler.apply(-1, null);
  }

  public MultiInputSelector setBlockOnNoInput(final boolean blockOnNoInput) {
    this.blockOnNoInput = blockOnNoInput;
    return this;
  }

  public MultiInputSelector setGuard(final int index, final Guard gaurd) {
    synchronized (this.monitor) {
      if (index >= 0) {
        this.guards.set(index, gaurd);
        this.monitor.notifyAll();
      }
    }
    return this;
  }

  public MultiInputSelector setGuard(final SelectableInput input, final Guard gaurd) {
    synchronized (this.monitor) {
      final int index = this.inputs.indexOf(input);
      return setGuard(index, gaurd);
    }
  }

  public MultiInputSelector setInputs(final Iterable<? extends SelectableInput> inputs,
    final Iterable<Guard> guards) {
    final List<SelectableInput> newInputs = new ArrayList<>();
    final List<Guard> newGuards = new ArrayList<>();
    return updateInputs(newInputs, newGuards, inputs, guards);
  }

  private MultiInputSelector updateInputs(final List<SelectableInput> newInputs,
    final List<Guard> newGuards, final Iterable<? extends SelectableInput> inputs,
    final Iterable<Guard> guards) {
    if (newInputs != null) {
      synchronized (this.monitor) {
        Iterator<Guard> guardIterator;
        if (guards == null) {
          guardIterator = Collections.emptyIterator();
        } else {
          guardIterator = guards.iterator();
        }
        for (final SelectableInput input : inputs) {
          Guard guard = null;
          ;
          if (guardIterator.hasNext()) {
            guard = guardIterator.next();
          }
          if (input != null) {
            newInputs.add(input);
            if (guard == null) {
              guard = Guard.ENABLED;
            }
            newGuards.add(guard);
          }
        }
        this.inputs = newInputs;
        this.guards = newGuards;
        this.monitor.notifyAll();
      }
    }
    return this;
  }
}
