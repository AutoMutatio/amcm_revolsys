package com.revolsys.collection.map;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.revolsys.collection.json.Json;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.Jsonable;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypedValue;
import com.revolsys.logging.Logs;
import com.revolsys.util.Property;

public interface MapEx
  extends MapDefault<String, CharSequence, Object, MapEx>, DataTypedValue, Jsonable {
  static MapEx asEx(final Map<String, ? extends Object> map) {
    if (map instanceof MapEx) {
      return (MapEx)map;
    } else {
      return JsonObject.hash();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  default boolean equals(final Object object2,
    final Collection<? extends CharSequence> excludeFieldNames) {
    final Map<Object, Object> map2 = (Map<Object, Object>)object2;
    final Set<Object> keys = new TreeSet<>();
    keys.addAll(keySet());
    keys.addAll(map2.keySet());
    keys.removeAll(excludeFieldNames);

    for (final Object key : keys) {
      final Object value1 = get(key);
      final Object value2 = map2.get(key);
      if (!DataType.equal(value1, value2, excludeFieldNames)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  default <T> T getValueByPath(final CharSequence path) {
    final String[] propertyPath = path.toString()
      .split("\\.");
    Object propertyValue = this;
    for (int i = 0; i < propertyPath.length && propertyValue != null; i++) {
      final String propertyName = propertyPath[i];
      if (propertyValue instanceof Map) {
        final Map<String, Object> map = (Map<String, Object>)propertyValue;
        propertyValue = map.get(propertyName);
        if (propertyValue == null) {
          return null;
        }
      } else {
        try {
          final Object object = propertyValue;
          propertyValue = Property.getSimple(object, propertyName);
        } catch (final IllegalArgumentException e) {
          Logs.debug(this, "Path does not exist " + path, e);
          return null;
        }
      }
    }
    return (T)propertyValue;
  }

  @Override
  default JsonObject toJson() {
    return JsonObject.hash(this);
  }

  @Override
  default String toJsonString() {
    return Json.toString(this);
  }

  @Override
  default String toJsonString(final boolean indent) {
    return Json.toString(this, indent);
  }

  @Override
  default String toK(final CharSequence key) {
    if (key == null) {
      return null;
    } else {
      return key.toString();
    }
  }
}
