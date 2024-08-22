package com.revolsys.parallel.channel.store;

import java.util.Iterator;

import com.revolsys.parallel.channel.ChannelValueStore;

/**
 * <h2>Description</h2>
 * <p>
 * The Buffer class is an implementation of ChannelValueStore which is pre-populated from an
 * iterable.
 * </p>
 * <p>
 * The getState method will return EMPTY if the Channel does not contain any
 * Objects, FULL if it cannot accept more data and NONEMPTYFULL otherwise.
 * </p>
 *
 * @author P.D.Austin
 */
public class IterableStore<T> extends ChannelValueStore<T> {
  /** The storage for the buffered Objects */
  private final Iterable<T> iterable;

  private final Iterator<T> iterator;

  /**
   * Construct a new Buffer with no maximum size.
   */
  public IterableStore(final Iterable<T> iterable) {
    this.iterable = iterable;
    this.iterator = iterable.iterator();
  }

  /**
   * Returns a new Object with the same creation parameters as this Object. This
   * method should be overridden by subclasses to return a new Object that is
   * the same type as this Object. The new instance should be created by
   * constructing a new instance with the same parameters as the original.
   * <I>NOTE: Only the sizes of the data should be cloned not the stored
   * data.</I>
   *
   * @return The cloned instance of this Object.
   */
  @Override
  protected Object clone() {
    return new UnsupportedOperationException("Clone not supported");
  }

  /**
   * Returns the first Object from the Buffer and removes the Object from the
   * Buffer.
   * <P>
   * <I>NOTE: getState should be called before this method to check that the
   * state is not EMPTY. If the state is EMPTY the Buffer will be left in an
   * undefined state.</I>
   * <P>
   * Pre-condition: The state must not be EMPTY
   *
   * @return The next available Object from the Buffer
   */
  @Override
  protected T get() {
    return this.iterator.next();
  }

  /**
   * Returns the current state of the store, should be called to ensure the
   * Pre-conditions of the other methods are not broken.
   *
   * @return The current state of the store (EMPTY, NONEMPTYFULL or FULL)
   */
  @Override
  protected int getState() {
    if (this.iterator.hasNext()) {
      return NONEMPTYFULL;
    } else {
      return EMPTY;
    }
  }

  /**
   * Puts a new Object into the store.
   * <P>
   * <I>NOTE: This methods is not supported</I>
   * <P>
   *
   * @param value The object to put in the Buffer
   */
  @Override
  protected void put(final T value) {
    throw new UnsupportedOperationException("Cannot add new items to the store");
  }

  @Override
  public String toString() {
    return this.iterable.toString();
  }
}
