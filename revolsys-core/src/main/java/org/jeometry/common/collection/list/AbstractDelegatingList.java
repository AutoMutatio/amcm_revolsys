package org.jeometry.common.collection.list;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

import org.jeometry.common.collection.collection.AbstractDelegatingCollection;

public abstract class AbstractDelegatingList<V> extends AbstractDelegatingCollection<V>
  implements List<V> {
  public static <V1> ListIterator<V1> unmodifiableListIterator(final ListIterator<V1> iterator) {
    return new ListIterator<>() {
      @Override
      public void add(final V1 e) {
        throw new IllegalStateException("List is not editable");
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public boolean hasPrevious() {
        return iterator.hasPrevious();
      }

      @Override
      public V1 next() {
        return iterator.next();
      }

      @Override
      public int nextIndex() {
        return iterator.nextIndex();
      }

      @Override
      public V1 previous() {
        return iterator.previous();
      }

      @Override
      public int previousIndex() {
        return iterator.previousIndex();
      }

      @Override
      public void remove() {
        throw new IllegalStateException("List is not editable");
      }

      @Override
      public void set(final V1 e) {
        throw new IllegalStateException("List is not editable");
      }

      @Override
      public String toString() {
        return iterator.toString();
      }
    };
  }

  public AbstractDelegatingList(final boolean editable) {
    super(editable);
  }

  @Override
  public void add(final int index, final V element) {
    final List<V> list = getEditableList();
    list.add(index, element);
  }

  @Override
  public boolean addAll(final int index, final Collection<? extends V> c) {
    final List<V> list = getEditableList();
    return list.addAll(c);
  }

  @Override
  public V get(final int index) {
    final List<V> list = getList();
    return list.get(index);
  }

  @Override
  protected Collection<V> getCollection() {
    return getList();
  }

  @Override
  protected Collection<V> getEditableCollection() {
    return getEditableList();
  }

  protected List<V> getEditableList() {
    if (!isEditable()) {
      throw new IllegalStateException("List is not editable");
    }
    return getList();
  }

  protected abstract List<V> getList();

  @Override
  public int indexOf(final Object o) {
    final List<V> list = getList();
    return list.indexOf(o);
  }

  @Override
  public int lastIndexOf(final Object o) {
    final List<V> list = getList();
    return list.lastIndexOf(o);
  }

  @Override
  public ListIterator<V> listIterator() {
    final List<V> list = getList();
    final ListIterator<V> iterator = list.listIterator();
    if (isEditable()) {
      return iterator;
    } else {
      return unmodifiableListIterator(iterator);
    }
  }

  @Override
  public ListIterator<V> listIterator(final int index) {
    final List<V> list = getList();
    final ListIterator<V> listIterator = list.listIterator(index);
    return unmodifiableListIterator(listIterator);
  }

  @Override
  public V remove(final int index) {
    final List<V> list = getEditableList();
    return list.remove(index);
  }

  @Override
  public V set(final int index, final V element) {
    final List<V> list = getEditableList();
    return list.set(index, element);
  }

  @Override
  public void sort(final Comparator<? super V> c) {
    final List<V> list = getEditableList();
    list.sort(c);
  }

  @Override
  public List<V> subList(final int fromIndex, final int toIndex) {
    final List<V> list = getList();
    return list.subList(fromIndex, toIndex);
  }

}
