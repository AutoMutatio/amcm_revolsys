
package com.revolsys.collection.map;

import java.net.URI;
import java.sql.Clob;
import java.sql.SQLException;
import java.time.Instant;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import com.revolsys.collection.iterator.BaseIterable;
import com.revolsys.collection.iterator.Iterables;
import com.revolsys.collection.json.Json;
import com.revolsys.collection.json.JsonList;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.JsonType;
import com.revolsys.collection.list.ListEx;
import com.revolsys.comparator.CompareUtil;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypeProxy;
import com.revolsys.data.type.DataTypeValueFactory;
import com.revolsys.data.type.DataTypes;
import com.revolsys.util.Property;

public interface MapDefault<K, KA, V, M extends MapDefault<K, KA, V, M>>
    extends Map<K, V>, Cloneable {
  @SuppressWarnings("unchecked")
  default M add(final KA key, final V value) {
    final var k = toK(key);
    put(k, value);
    return (M) this;
  }

  @SuppressWarnings("unchecked")
  default M addAll(final Map<? extends KA, ? extends V> map) {
    for (final var entry : map.entrySet()) {
      final var key = entry.getKey();
      final var value = entry.getValue();
      addValue(key, value);
    }
    return (M) this;
  }

  default M addFieldValue(final KA key, final Map<? extends KA, ? extends V> source) {
    final var value = source.get(key);
    return addValue(key, value);
  }

  default <SK> M addFieldValue(final KA key, final Map<SK, ? extends V> source,
      final SK sourceKey) {
    final var value = source.get(sourceKey);
    return addValue(key, value);
  }

  @SuppressWarnings("unchecked")
  default M addFieldValues(final MapDefault<?, KA, ? extends V, ?> source, final DataType dataType,
      final KA... fieldNames) {
    for (final KA fieldName : fieldNames) {
      final V value = source.getTypedValue(fieldName, dataType);
      if (value == null) {
        if (source.containsKey(fieldName)) {
          removeValue(fieldName);
        }
      } else {
        addValue(fieldName, value);
      }
    }
    return (M) this;
  }

  @SuppressWarnings("unchecked")
  default M addValue(final KA key, final V value) {
    final K k = toK(key);
    put(k, value);
    return (M) this;
  }

  @SuppressWarnings("unchecked")
  default M addValue(final KA key, V value, final DataType dataType) {
    value = dataType.toObject(value);
    addValue(key, value);
    return (M) this;
  }

  @Override
  default void clear() {
    final Set<java.util.Map.Entry<K, V>> entrySet = entrySet();
    entrySet.clear();
  }

  M clone();

  default int compareValue(final KA fieldName, final Object value) {
    if (containsKey(fieldName)) {
      final Object fieldValue = getValue(fieldName);
      return CompareUtil.compare(fieldValue, value);
    } else {
      return -1;
    }
  }

  default int compareValue(final Map<KA, ?> map, final KA fieldName) {
    if (map != null) {
      final Object value = map.get(fieldName);
      return compareValue(fieldName, value);
    }
    return -1;
  }

  default int compareValue(final Map<KA, ?> map, final KA fieldName, final boolean nullsFirst) {
    final Comparable<Object> value1 = getValue(fieldName);
    Object value2;
    if (map == null) {
      value2 = null;
    } else {
      value2 = map.get(fieldName);
    }
    return CompareUtil.compare(value1, value2, nullsFirst);
  }

  default int compareValue(final MapDefault<?, KA, ?, ?> map, final KA fieldName) {
    if (map != null) {
      final Object value = map.get(fieldName);
      return compareValue(fieldName, value);
    }
    return -1;
  }

  default int compareValue(final MapDefault<?, KA, ?, ?> map, final KA fieldName,
      final boolean nullsFirst) {
    final Comparable<Object> value1 = getValue(fieldName);
    Object value2;
    if (map == null) {
      value2 = null;
    } else {
      value2 = map.getValue(fieldName);
    }
    return CompareUtil.compare(value1, value2, nullsFirst);
  }

  @Override
  default boolean containsKey(final Object key) {
    final Set<Entry<K, V>> entrySet = entrySet();
    if (key == null) {
      for (final Entry<K, V> entry : entrySet) {
        final K entryKey = entry.getKey();
        if (entryKey == null) {
          return true;
        }
      }
    } else {
      for (final Entry<K, V> entry : entrySet) {
        final K entryKey = entry.getKey();
        if (key.equals(entryKey)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  default boolean containsValue(final Object value) {
    final Set<Entry<K, V>> entrySet = entrySet();
    if (value == null) {
      for (final Entry<K, V> entry : entrySet) {
        final V entryValue = entry.getValue();
        if (entryValue == null) {
          return true;
        }
      }
    } else {
      for (final Entry<K, V> entry : entrySet) {
        final V entryValue = entry.getValue();
        if (value.equals(entryValue)) {
          return true;
        }
      }
    }
    return false;
  }

  default <T extends V> T ensureValue(final KA key, final DataTypeProxy dataType,
      final Supplier<T> supplier) {
    final Object value = getValue(key);
    if (value == null) {
      final T newValue = supplier.get();
      addValue(key, newValue);
      return newValue;
    } else {
      final T convertedValue = dataType.toObject(value);
      if (convertedValue != value) {
        addValue(key, convertedValue);
      }
      return convertedValue;
    }
  }

  default <T extends V> T ensureValue(final KA key, final DataTypeValueFactory<T> factory) {
    return ensureValue(key, factory, factory);
  }

  @SuppressWarnings("unchecked")
  default <T extends V> T ensureValue(final KA key, final Supplier<T> supplier) {
    V value = getValue(key);
    if (value == null) {
      value = supplier.get();
      addValue(key, value);
    }
    return (T) value;
  }

  default boolean equalValue(final KA fieldName, final Object value) {
    final Object fieldValue = getValue(fieldName);
    return DataType.equal(fieldValue, value);
  }

  @Override
  default V get(final Object key) {
    final Set<Entry<K, V>> entrySet = entrySet();
    if (key == null) {
      for (final Entry<K, V> entry : entrySet) {
        final K entryKey = entry.getKey();
        if (entryKey == null) {
          final V entryValue = entry.getValue();
          return entryValue;
        }
      }
    } else {
      for (final Entry<K, V> entry : entrySet) {
        final K entryKey = entry.getKey();
        if (key.equals(entryKey)) {
          final V entryValue = entry.getValue();
          return entryValue;
        }
      }
    }
    return null;
  }

  default Boolean getBoolean(final KA name) {
    return getTypedValue(name, DataTypes.BOOLEAN);
  }

  default boolean getBoolean(final KA name, final boolean defaultValue) {
    final Object value = getTypedValue(name, DataTypes.BOOLEAN);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    } else {
      return Boolean.parseBoolean(value.toString());
    }
  }

  default Byte getByte(final KA name) {
    return getTypedValue(name, DataTypes.BYTE);
  }

  default byte getByte(final KA name, final byte defaultValue) {
    final Byte value = getByte(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default Double getDouble(final KA name) {
    return getTypedValue(name, DataTypes.DOUBLE);
  }

  default double getDouble(final KA name, final double defaultValue) {
    final Double value = getDouble(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default <E extends Enum<E>> E getEnum(final Class<E> enumType, final KA fieldName) {
    final String value = getString(fieldName);
    if (value == null) {
      return null;
    } else {
      return Enum.valueOf(enumType, value);
    }
  }

  default Float getFloat(final KA name) {
    return getTypedValue(name, DataTypes.FLOAT);
  }

  default float getFloat(final KA name, final float defaultValue) {
    final Float value = getFloat(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default Identifier getIdentifier(final KA fieldName) {
    final Object value = getValue(fieldName);
    return Identifier.newIdentifier(value);
  }

  default Identifier getIdentifier(final KA fieldName, final DataType dataType) {
    final Object value = getTypedValue(fieldName, dataType);
    return Identifier.newIdentifier(value);
  }

  default Instant getInstant(final KA name) {
    return getTypedValue(name, DataTypes.INSTANT);
  }

  default Integer getInteger(final KA name) {
    return getTypedValue(name, DataTypes.INT);
  }

  default int getInteger(final KA name, final int defaultValue) {
    final Integer value = getInteger(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  @SuppressWarnings("unchecked")
  default <T extends V> BaseIterable<T> getIterable(final KA name) {
    final Object value = getValue(name);
    if (value == null) {
      return Iterables.empty();
    } else if (value instanceof final BaseIterable iterable) {
      return iterable;
    } else if (value instanceof final Iterable iterable) {
      return Iterables.fromIterable(iterable);
    } else if (value instanceof final Iterator iterator) {
      return Iterables.fromIterator(iterator);
    } else {
      throw new IllegalArgumentException("Cannot convert to iterable:" + value);
    }
  }

  default JsonList getJsonList(final KA name) {
    return getTypeValue(name, Json.JSON_LIST, JsonList.EMPTY);
  }

  default JsonList getJsonList(final KA name, final JsonList defaultValue) {
    final JsonList value = getJsonList(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default JsonObject getJsonObject(final KA name) {
    return getTypeValue(name, Json.JSON_OBJECT, JsonObject.EMPTY);
  }

  default JsonObject getJsonObject(final KA name, final JsonObject defaultValue) {
    final JsonObject value = getJsonObject(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default <T extends V> ListEx<T> getList(final KA name) {
    return getTypeValue(name, DataTypes.LIST, ListEx.empty());
  }

  default Long getLong(final KA name) {
    return getTypedValue(name, DataTypes.LONG);
  }

  default long getLong(final KA name, final long defaultValue) {
    final Long value = getLong(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default Short getShort(final KA name) {
    return getTypedValue(name, DataTypes.SHORT);
  }

  default short getShort(final KA name, final short defaultValue) {
    final Short value = getShort(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default String getString(final KA fieldName) {
    final Object value = getValue(fieldName);
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      return value.toString();
    } else if (value instanceof Clob) {
      final Clob clob = (Clob) value;
      try {
        return clob.getSubString(1, (int) clob.length());
      } catch (final SQLException e) {
        throw new RuntimeException("Unable to read clob", e);
      }
    } else {
      return DataTypes.toString(value);
    }
  }

  default String getString(final KA name, final String defaultValue) {
    final String value = getString(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default <T extends Object> T getTypedValue(final KA name, final DataType dataType) {
    final Object value = getValue(name);
    return dataType.toObject(value);
  }

  default <T extends Object> T getTypeValue(final KA name, final DataType dataType,
      final T defaultValue) {
    final T value = getTypedValue(name, dataType);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default String getUpperString(final KA fieldName) {
    final String string = getString(fieldName);
    if (string == null) {
      return null;
    } else {
      return string.toUpperCase();
    }
  }

  default URI getURI(final KA name) {
    return getTypedValue(name, DataTypes.ANY_URI);
  }

  default UUID getUUID(final KA name) {
    return getTypedValue(name, DataTypes.UUID);
  }

  /**
   * Get the value of the field with the specified name.
   *
   * @param name The name of the field.
   * @return The field value.
   */
  @SuppressWarnings("unchecked")
  default <T extends Object> T getValue(final KA key) {
    if (key == null) {
      return null;
    } else {
      final var k = toK(key);
      return (T) get(k);
    }
  }

  default <T extends Object> T getValue(final KA name,
      final DataTypeValueFactory<T> defaultValueFactory) {
    final DataType dataType = defaultValueFactory.getDataType();
    final T value = getTypedValue(name, dataType);
    if (value == null) {
      return defaultValueFactory.get();
    } else {
      return value;
    }
  }

  default <I, O> O getValue(final KA name, final Function<I, O> converter) {
    final I value = getValue(name);
    if (value == null) {
      return null;
    } else {
      return converter.apply(value);
    }
  }

  default <T extends V> T getValue(final KA name, final Supplier<T> defaultValue) {
    final T value = getValue(name);
    if (value == null) {
      return defaultValue.get();
    } else {
      return value;
    }
  }

  default <T extends V> T getValue(final KA name, final T defaultValue) {
    final T value = getValue(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  default boolean hasValue(final KA name) {
    final Object value = getValue(name);
    return Property.hasValue(value);
  }

  default boolean hasValuesAll(@SuppressWarnings("unchecked") final KA... names) {
    if (names == null || names.length == 0) {
      return false;
    } else {
      for (final KA name : names) {
        if (!hasValue(name)) {
          return false;
        }
      }
      return true;
    }
  }

  default boolean hasValuesAny(@SuppressWarnings("unchecked") final KA... names) {
    if (names == null || names.length == 0) {
      return false;
    } else {
      for (final KA name : names) {
        if (hasValue(name)) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  default boolean isEmpty() {
    return size() == 0;
  }

  default boolean isTrue(final KA name) {
    return getBoolean(name, false);
  }

  @Override
  default Set<K> keySet() {
    return new AbstractSet<>() {
      @Override
      public void clear() {
        MapDefault.this.clear();
      }

      @Override
      public boolean contains(final Object k) {
        return MapDefault.this.containsKey(k);
      }

      @Override
      public boolean isEmpty() {
        return MapDefault.this.isEmpty();
      }

      @Override
      public Iterator<K> iterator() {
        return new Iterator<>() {
          private final Iterator<Entry<K, V>> interator = entrySet().iterator();

          @Override
          public boolean hasNext() {
            return this.interator.hasNext();
          }

          @Override
          public K next() {
            return this.interator.next().getKey();
          }

          @Override
          public void remove() {
            this.interator.remove();
          }
        };
      }

      @Override
      public int size() {
        return MapDefault.this.size();
      }
    };
  }

  default boolean mapEquals(final Map<?, ?> map) {
    if (map == this) {
      return true;
    } else if (map.size() != size()) {
      return false;
    } else {
      try {
        final Set<Entry<K, V>> entrySet = entrySet();
        for (final Entry<K, V> entry : entrySet) {
          final K key = entry.getKey();
          final V value = entry.getValue();
          if (value == null) {
            if (!(map.get(key) == null && map.containsKey(key))) {
              return false;
            }
          } else {
            if (!value.equals(map.get(key))) {
              return false;
            }
          }
        }
      } catch (final ClassCastException unused) {
        return false;
      } catch (final NullPointerException unused) {
        return false;
      }
      return true;
    }
  }

  default int mapHashCode() {
    int hash = 0;
    final Set<Entry<K, V>> entrySet = entrySet();
    for (final Entry<K, V> entry : entrySet) {
      hash += entry.hashCode();
    }
    return hash;
  }

  default String mapToString() {
    final Set<java.util.Map.Entry<K, V>> entrySet = entrySet();
    final Iterator<Entry<K, V>> i = entrySet.iterator();
    if (entrySet.size() == 0) {
      return "{}";
    } else {
      final StringBuilder string = new StringBuilder();
      string.append('{');
      boolean first = true;
      for (final Entry<K, V> entry : entrySet) {
        if (first) {
          first = false;
        } else {
          string.append(',').append(' ');
        }
        final K key = entry.getKey();
        final V value = entry.getValue();
        string.append(key == this ? "(this Map)" : key);
        string.append('=');
        string.append(value == this ? "(this Map)" : value);
        if (!i.hasNext()) {
        }
        string.append(',').append(' ');
      }
      return string.append('}').toString();
    }
  }

  @Override
  default V put(final K key, final V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  default void putAll(final Map<? extends K, ? extends V> values) {
    for (final Entry<? extends K, ? extends V> entry : values.entrySet()) {
      final K key = entry.getKey();
      final V value = entry.getValue();
      put(key, value);
    }
  }

  @Override
  default V remove(final Object key) {
    final Set<Map.Entry<K, V>> entrySet = entrySet();
    final Iterator<Entry<K, V>> i = entrySet.iterator();
    Entry<K, V> correctEntry = null;
    if (key == null) {
      while (correctEntry == null && i.hasNext()) {
        final Entry<K, V> e = i.next();
        if (e.getKey() == null) {
          correctEntry = e;
        }
      }
    } else {
      while (correctEntry == null && i.hasNext()) {
        final Entry<K, V> e = i.next();
        if (key.equals(e.getKey())) {
          correctEntry = e;
        }
      }
    }

    V oldValue = null;
    if (correctEntry != null) {
      oldValue = correctEntry.getValue();
      i.remove();
    }
    return oldValue;
  }

  default boolean removeEmptyProperties() {
    boolean removed = false;
    final Collection<?> entries = values();
    for (final Iterator<?> iterator = entries.iterator(); iterator.hasNext();) {
      final Object value = iterator.next();
      if (value instanceof JsonType) {
        final JsonType jsonValue = (JsonType) value;
        jsonValue.removeEmptyProperties();
        if (jsonValue.isEmpty()) {
          iterator.remove();
          removed = true;
        }
      } else if (!Property.hasValue(value)) {
        iterator.remove();
        removed = true;
      }
    }
    return removed;
  }

  @SuppressWarnings("unchecked")
  default <T extends Object> T removeValue(final KA name) {
    if (name == null) {
      return null;
    } else {
      return (T) remove(name.toString());
    }
  }

  default <T extends V> T removeValue(final KA name, final DataType dataType) {
    final Object value = removeValue(name);
    return dataType.toObject(value);
  }

  default <I extends V, O> O removeValue(final KA name, final Function<I, O> converter) {
    final I value = removeValue(name);
    if (value == null) {
      return null;
    } else {
      return converter.apply(value);
    }
  }

  default <T extends V> T removeValue(final KA name, final Supplier<T> defaultValue) {
    final T value = removeValue(name);
    if (value == null) {
      return defaultValue.get();
    } else {
      return value;
    }
  }

  default <T extends V> T removeValue(final KA name, final T defaultValue) {
    final T value = removeValue(name);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  @SuppressWarnings("unchecked")
  default M removeValues(final KA... keys) {
    for (final var name : keys) {
      removeValue(name);
    }
    return (M) this;
  }

  @SuppressWarnings("unchecked")
  default M renameProperty(final KA oldName, final KA newName) {
    if (hasValue(oldName) && !oldName.equals(newName)) {
      final V value = removeValue(oldName);
      addValue(newName, value);
    }
    return (M) this;
  }

  @SuppressWarnings("unchecked")
  default M renameProperty(final KA oldName, final KA newName, final DataType dataType) {
    if (hasValue(oldName) && !oldName.equals(newName)) {
      final var value = removeValue(oldName, dataType);
      addValue(newName, value);
    }
    return (M) this;
  }

  @Override
  default int size() {
    return entrySet().size();
  }

  K toK(KA key);

  @Override
  default Collection<V> values() {
    return new AbstractCollection<>() {
      @Override
      public void clear() {
        MapDefault.this.clear();
      }

      @Override
      public boolean contains(final Object v) {
        return MapDefault.this.containsValue(v);
      }

      @Override
      public boolean isEmpty() {
        return MapDefault.this.isEmpty();
      }

      @Override
      public Iterator<V> iterator() {
        return new Iterator<>() {
          private final Iterator<Entry<K, V>> iterator = entrySet().iterator();

          @Override
          public boolean hasNext() {
            return this.iterator.hasNext();
          }

          @Override
          public V next() {
            return this.iterator.next().getValue();
          }

          @Override
          public void remove() {
            this.iterator.remove();
          }
        };
      }

      @Override
      public int size() {
        return MapDefault.this.size();
      }
    };
  }

}
