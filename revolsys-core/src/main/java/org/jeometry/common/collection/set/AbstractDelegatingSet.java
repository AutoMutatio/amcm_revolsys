package org.jeometry.common.collection.set;

import java.util.Collection;
import java.util.Set;

import org.jeometry.common.collection.collection.AbstractDelegatingCollection;

public abstract class AbstractDelegatingSet<V> extends AbstractDelegatingCollection<V>
  implements Set<V> {

  public AbstractDelegatingSet(final boolean editable) {
    super(editable);
  }

  @Override
  protected Collection<V> getCollection() {
    return getSet();
  }

  @Override
  protected Collection<V> getEditableCollection() {
    return getEditableSet();
  }

  protected Set<V> getEditableSet() {
    if (!isEditable()) {
      throw new IllegalStateException("Set is not editable");
    }
    return getSet();
  }

  protected abstract Set<V> getSet();
}
