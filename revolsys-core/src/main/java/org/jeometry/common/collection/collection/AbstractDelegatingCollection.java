package org.jeometry.common.collection.collection;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class AbstractDelegatingCollection<V> implements Collection<V> {

  private boolean editable = true;

  public AbstractDelegatingCollection(final boolean editable) {
    this.editable = editable;
  }

  @Override
  public boolean add(final V e) {
    final Collection<V> collection = getEditableCollection();
    return collection.add(e);
  }

  @Override
  public boolean addAll(final Collection<? extends V> c) {
    final Collection<V> collection = getEditableCollection();
    return collection.addAll(c);
  }

  @Override
  public void clear() {
    final Collection<V> collection = getEditableCollection();
    collection.clear();
  }

  @Override
  public boolean contains(final Object o) {
    final Collection<V> collection = getCollection();
    return collection.contains(o);
  }

  @Override
  public boolean containsAll(final Collection<?> c) {
    final Collection<V> collection = getCollection();
    return collection.containsAll(c);
  }

  @Override
  public boolean equals(final Object obj) {
    final Collection<V> collection = getCollection();
    return collection.equals(obj);
  }

  @Override
  public void forEach(final Consumer<? super V> action) {
    final Collection<V> collection = getCollection();
    collection.forEach(action);
  }

  protected abstract Collection<V> getCollection();

  protected Collection<V> getEditableCollection() {
    if (!isEditable()) {
      throw new IllegalStateException("Set is not editable");
    }
    return getCollection();
  }

  @Override
  public int hashCode() {
    final Collection<V> collection = getCollection();
    return collection.hashCode();
  }

  public boolean isEditable() {
    return this.editable;
  }

  @Override
  public boolean isEmpty() {
    final Collection<V> collection = getCollection();
    return collection.isEmpty();
  }

  @Override
  public Iterator<V> iterator() {
    final Collection<V> collection = getCollection();
    final Iterator<V> iterator = collection.iterator();
    if (isEditable()) {
      return iterator;
    } else {
      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public V next() {
          return iterator.next();
        }

        @Override
        public String toString() {
          return iterator.toString();
        }
      };
    }
  }

  @Override
  public Stream<V> parallelStream() {
    final Collection<V> collection = getCollection();
    return collection.parallelStream();
  }

  @Override
  public boolean remove(final Object o) {
    final Collection<V> collection = getEditableCollection();
    return collection.remove(o);
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    final Collection<V> collection = getEditableCollection();
    return collection.removeAll(c);
  }

  @Override
  public boolean removeIf(final Predicate<? super V> filter) {
    final Collection<V> collection = getEditableCollection();
    return collection.removeIf(filter);
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    final Collection<V> collection = getEditableCollection();
    return collection.retainAll(c);
  }

  @Override
  public int size() {
    final Collection<V> collection = getCollection();
    return collection.size();
  }

  @Override
  public Spliterator<V> spliterator() {
    final Collection<V> collection = getCollection();
    return collection.spliterator();
  }

  @Override
  public Stream<V> stream() {
    final Collection<V> collection = getCollection();
    return collection.stream();
  }

  @Override
  public Object[] toArray() {
    final Collection<V> collection = getCollection();
    return collection.toArray();
  }

  @Override
  public <T> T[] toArray(final IntFunction<T[]> generator) {
    final Collection<V> collection = getCollection();
    return collection.toArray(generator);
  }

  @Override
  public <T> T[] toArray(final T[] a) {
    final Collection<V> collection = getCollection();
    return collection.toArray(a);
  }

  @Override
  public String toString() {
    final Collection<V> collection = getCollection();
    return collection.toString();
  }

}
