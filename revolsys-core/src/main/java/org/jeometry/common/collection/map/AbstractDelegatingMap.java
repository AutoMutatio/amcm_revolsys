package org.jeometry.common.collection.map;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public abstract class AbstractDelegatingMap<K, V> implements Map<K, V> {

  private final boolean editable;

  public AbstractDelegatingMap(final boolean editable) {
    this.editable = editable;
  }

  @Override
  public void clear() {
    final Map<K, V> map = getEditableMap();
    map.clear();
  }

  @Override
  public boolean containsKey(final Object key) {
    final Map<K, V> map = getMap();
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    final Map<K, V> map = getMap();
    return map.containsValue(value);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    final Map<K, V> map = getMap();
    final Set<Entry<K, V>> set = map.entrySet();
    if (isEditable()) {
      return Collections.unmodifiableSet(set);
    } else {
      return set;
    }
  }

  @Override
  public boolean equals(final Object o) {
    final Map<K, V> map = getMap();
    return map.equals(o);
  }

  @Override
  public V get(final Object key) {
    final Map<K, V> map = getMap();
    return map.get(key);
  }

  protected Map<K, V> getEditableMap() {
    if (!isEditable()) {
      throw new IllegalStateException("Map is not editable");
    }
    return getMap();
  }

  protected abstract Map<K, V> getMap();

  @Override
  public int hashCode() {
    final Map<K, V> map = getMap();
    return map.hashCode();
  }

  protected boolean isEditable() {
    return this.editable;
  }

  @Override
  public boolean isEmpty() {
    final Map<K, V> map = getMap();
    return map.isEmpty();
  }

  @Override
  public Set<K> keySet() {
    final Map<K, V> map = getMap();
    final Set<K> set = map.keySet();
    if (isEditable()) {
      return Collections.unmodifiableSet(set);
    } else {
      return set;
    }
  }

  @Override
  public V put(final K key, final V value) {
    final Map<K, V> map = getEditableMap();
    return map.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> m) {
    final Map<K, V> map = getEditableMap();
    map.putAll(m);
  }

  @Override
  public V remove(final Object key) {
    final Map<K, V> map = getEditableMap();
    return map.remove(key);
  }

  @Override
  public int size() {
    final Map<K, V> map = getMap();
    return map.size();
  }

  @Override
  public String toString() {
    final Map<K, V> map = getMap();
    return map.toString();
  }

  @Override
  public Collection<V> values() {
    final Map<K, V> map = getMap();

    final Collection<V> values = map.values();
    if (isEditable()) {
      return Collections.unmodifiableCollection(values);
    } else {
      return values;
    }
  }

}
