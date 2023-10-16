package com.revolsys.collection.collection;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.revolsys.collection.iterator.BaseIterable;

public abstract class AbstractDelegatingCollection<T> implements Collection<T>, BaseIterable<T> {

  private boolean editable = true;

  public AbstractDelegatingCollection(final boolean editable) {
    this.editable = editable;
  }

  @Override
  public boolean add(final T e) {
    final Collection<T> collection = getEditableCollection();
    return collection.add(e);
  }

  @Override
  public boolean addAll(final Collection<? extends T> c) {
    final Collection<T> collection = getEditableCollection();
    return collection.addAll(c);
  }

  @Override
  public void clear() {
    final Collection<T> collection = getEditableCollection();
    collection.clear();
  }

  @Override
  public boolean contains(final Object o) {
    final Collection<T> collection = getCollection();
    return collection.contains(o);
  }

  @Override
  public boolean containsAll(final Collection<?> c) {
    final Collection<T> collection = getCollection();
    return collection.containsAll(c);
  }

  @Override
  public boolean equals(final Object obj) {
    final Collection<T> collection = getCollection();
    return collection.equals(obj);
  }

  @Override
  public void forEach(final Consumer<? super T> action) {
    final Collection<T> collection = getCollection();
    collection.forEach(action);
  }

  protected abstract Collection<T> getCollection();

  protected Collection<T> getEditableCollection() {
    if (!isEditable()) {
      throw new IllegalStateException("Set is not editable");
    }
    return getCollection();
  }

  @Override
  public T getFirst() {
    return BaseIterable.super.getFirst();
  }

  @Override
  public int hashCode() {
    final Collection<T> collection = getCollection();
    return collection.hashCode();
  }

  public boolean isEditable() {
    return this.editable;
  }

  @Override
  public boolean isEmpty() {
    final Collection<T> collection = getCollection();
    return collection.isEmpty();
  }

  @Override
  public Iterator<T> iterator() {
    final Collection<T> collection = getCollection();
    final Iterator<T> iterator = collection.iterator();
    if (isEditable()) {
      return iterator;
    } else {
      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public T next() {
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
  public Stream<T> parallelStream() {
    final Collection<T> collection = getCollection();
    return collection.parallelStream();
  }

  @Override
  public boolean remove(final Object o) {
    final Collection<T> collection = getEditableCollection();
    return collection.remove(o);
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    final Collection<T> collection = getEditableCollection();
    return collection.removeAll(c);
  }

  @Override
  public boolean removeIf(final Predicate<? super T> filter) {
    final Collection<T> collection = getEditableCollection();
    return collection.removeIf(filter);
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    final Collection<T> collection = getEditableCollection();
    return collection.retainAll(c);
  }

  @Override
  public int size() {
    final Collection<T> collection = getCollection();
    return collection.size();
  }

  @Override
  public Spliterator<T> spliterator() {
    final Collection<T> collection = getCollection();
    return collection.spliterator();
  }

  @Override
  public Stream<T> stream() {
    final Collection<T> collection = getCollection();
    return collection.stream();
  }

  @Override
  public Object[] toArray() {
    final Collection<T> collection = getCollection();
    return collection.toArray();
  }

  @Override
  public <V> V[] toArray(final IntFunction<V[]> generator) {
    final Collection<T> collection = getCollection();
    return collection.toArray(generator);
  }

  @Override
  public <V> V[] toArray(final V[] a) {
    final Collection<T> collection = getCollection();
    return collection.toArray(a);
  }

  @Override
  public String toString() {
    final Collection<T> collection = getCollection();
    return collection.toString();
  }

}
