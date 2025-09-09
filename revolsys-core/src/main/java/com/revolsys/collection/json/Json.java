package com.revolsys.collection.json;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.AbstractDataType;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.data.type.FunctionDataType;
import com.revolsys.exception.Exceptions;
import com.revolsys.io.JavaIo;
import com.revolsys.util.Property;

public class Json {

  private static class JsonDataType extends AbstractDataType {

    public JsonDataType() {
      super("JsonType", JsonType.class, true);
    }

    @Override
    public boolean equalsNotNull(final Object object1, final Object object2) {
      return object1.equals(object2);
    }

    @Override
    protected boolean equalsNotNull(final Object object1, final Object object2,
      final Collection<? extends CharSequence> exclude) {
      final JsonType json = (JsonType)object1;
      return json.equals(object2, exclude);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object toObjectDo(final Object value) {
      if (value instanceof JsonType) {
        return value;
      } else if (value instanceof final Jsonable jsonable) {
        return jsonable.asJson();
      } else if (value instanceof Map) {
        return new JsonObjectHash((Map<? extends String, ? extends Object>)value);
      } else if (value instanceof final List<?> list) {
        return JsonList.array(list);
      } else if (value instanceof final String string) {
        final Object read = JsonParser.read(string);
        if (read instanceof JsonType) {
          return read;
        } else if (read instanceof Boolean) {
          return read;
        } else if (read instanceof String) {
          return read;
        } else {
          return value;
        }
      } else {
        return value;
      }
    }

    @Override
    protected String toStringDo(final Object value) {
      if (value instanceof Jsonable) {
        final Jsonable json = (Jsonable)value;
        return json.toJsonString(true);
      } else {
        return Json.toString(value);
      }
    }
  }

  public static class JsonListDataType extends AbstractDataType {

    public JsonListDataType() {
      super("JsonList", JsonList.class, true);
    }

    @Override
    protected boolean equalsExactNotNull(final Object value1, final Object value2) {
      final var list1 = (List<?>)value1;
      final var list2 = (List<?>)value1;
      if (list1.size() != list2.size()) {
        return false;
      }
      for (int i = 0; i < list1.size(); i++) {
        if (!DataType.equalExact(value1, value2)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean equalsNotNull(final Object object1, final Object object2) {
      return object1.equals(object2);
    }

    @Override
    protected boolean equalsNotNull(final Object object1, final Object object2,
      final Collection<? extends CharSequence> exclude) {
      final JsonList list1 = (JsonList)object1;
      return list1.equals(object2);
    }

    @Override
    protected Object toObjectDo(final Object value) {
      if (value instanceof JsonList) {
        return value;
      } else if (value instanceof final Jsonable jsonable) {
        return jsonable.asJson();
      } else if (value instanceof final Collection<?> collection) {
        return JsonList.array(collection);
      } else {
        final Object json = JsonParser.read(value.toString());
        if (json instanceof JsonList) {
          return json;
        } else {
          return JsonList.array(json);
        }
      }
    }

    @Override
    protected String toStringDo(final Object value) {
      if (value instanceof JsonList) {
        return ((JsonList)value).toJsonString();
      } else if (value instanceof List<?>) {
        return Json.toString(value);
      } else if (value == null) {
        return null;
      } else {
        return value.toString();
      }
    }
  }

  private static class JsonObjectDataType extends AbstractDataType {

    public JsonObjectDataType(final String name, final Class<?> javaClass) {
      super(name, javaClass, true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equalsExactNotNull(final Object object1, final Object object2) {
      final Map<Object, Object> map1 = (Map<Object, Object>)object1;
      final Map<Object, Object> map2 = (Map<Object, Object>)object2;
      if (map1.size() == map2.size()) {
        final Set<Object> keys1 = map1.keySet();
        final Set<Object> keys2 = map2.keySet();
        if (keys1.equals(keys2)) {
          for (final Object key : keys1) {
            final Object value1 = map1.get(key);
            final Object value2 = map2.get(key);
            if (!DataType.equalExact(value1, value2)) {
              return false;
            }
          }
        } else {
          return false;
        }
        return true;
      } else {
        return false;
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equalsNotNull(final Object object1, final Object object2) {
      final Map<Object, Object> map1 = (Map<Object, Object>)object1;
      final Map<Object, Object> map2 = (Map<Object, Object>)object2;
      if (map1.size() == map2.size()) {
        final Set<Object> keys1 = map1.keySet();
        final Set<Object> keys2 = map2.keySet();
        if (keys1.equals(keys2)) {
          for (final Object key : keys1) {
            final Object value1 = map1.get(key);
            final Object value2 = map2.get(key);
            if (!DataType.equal(value1, value2)) {
              return false;
            }
          }
        } else {
          return false;
        }
        return true;
      } else {
        return false;
      }
    }

    @SuppressWarnings({
      "unchecked"
    })
    @Override
    protected boolean equalsNotNull(final Object object1, final Object object2,
      final Collection<? extends CharSequence> exclude) {
      final Map<Object, Object> map1 = (Map<Object, Object>)object1;
      final Map<Object, Object> map2 = (Map<Object, Object>)object2;
      final Set<Object> keys = new TreeSet<>();
      keys.addAll(map1.keySet());
      keys.addAll(map2.keySet());
      keys.removeAll(exclude);

      for (final Object key : keys) {
        final Object value1 = map1.get(key);
        final Object value2 = map2.get(key);
        if (!DataType.equal(value1, value2, exclude)) {
          return false;
        }
      }
      return true;
    }

    protected JsonObject toJsonObject(final Map<? extends String, ? extends Object> map) {
      return new JsonObjectHash(map);
    }

    @SuppressWarnings({
      "unchecked"
    })
    @Override
    protected Object toObjectDo(final Object value) {
      if (value instanceof JsonObject) {
        return toJsonObject((JsonObject)value);
      } else if (value instanceof Jsonable) {
        return ((Jsonable)value).asJson();
      } else if (value instanceof Map) {
        final Map<? extends String, ? extends Object> map = (Map<? extends String, ? extends Object>)value;
        return toJsonObject(map);
      } else if (value instanceof String) {
        final JsonObject map = Json.toObjectMap((String)value);
        if (map == null) {
          return null;
        } else {
          return toJsonObject(map);
        }
      } else {
        return toJsonObject(JsonParser.read(value));
      }
    }

    @SuppressWarnings({
      "unchecked"
    })
    @Override
    protected String toStringDo(final Object value) {
      if (value instanceof Jsonable) {
        return ((Jsonable)value).toJsonString();
      } else if (value instanceof Map) {
        final Map<? extends String, ? extends Object> map = (Map<? extends String, ? extends Object>)value;
        return Json.toString(map);
      } else if (value == null) {
        return null;
      } else {
        return value.toString();
      }
    }
  }

  private static class JsonObjectTreeDataType extends JsonObjectDataType {
    public JsonObjectTreeDataType() {
      super("JsonObjectTree", JsonObjectTree.class);
    }

    @Override
    protected JsonObject toJsonObject(final Map<? extends String, ? extends Object> map) {
      if (map instanceof JsonObjectTree) {
        return (JsonObjectTree)map;
      } else {
        return new JsonObjectTree(map);
      }
    }
  }

  @SuppressWarnings({
    "rawtypes", "unchecked"
  })
  public static final DataType TREE_MAP_TYPE = new FunctionDataType("TreeMap", JsonObjectTree.class,
    true, value -> {
      if (value instanceof JsonObjectTree) {
        return (JsonObjectTree)value;
      } else if (value instanceof Map) {
        return new JsonObjectTree((Map)value);
      } else if (value instanceof String) {
        final MapEx map = Json.toObjectMap((String)value);
        if (map == null) {
          return null;
        } else {
          return new JsonObjectTree(map);
        }
      } else {
        return value;
      }
    }, value -> {
      if (value instanceof Map) {
        return Json.toString((Map)value);
      } else if (value == null) {
        return null;
      } else {
        return value.toString();
      }

    }, FunctionDataType.MAP_EQUALS, FunctionDataType.MAP_EQUALS_EXCLUDES);

  public static final DataType JSON_OBJECT = new JsonObjectDataType("JsonObject", JsonObject.class);

  public static final DataType JSON_OBJECT_TREE = new JsonObjectTreeDataType();

  public static final DataType JSON_TYPE = new JsonDataType();

  public static DataType JSON_LIST = new JsonListDataType();

  static {
    DataTypes.registerDataTypes(Json.class);
  }

  public static final String FILE_EXTENSION = "json";

  public static final String MIME_TYPE = "application/json";

  public static final String MIME_TYPE_UTF8 = "application/json;charset=utf-8";

  public static Appendable appendJson(final Collection<?> collection, final Appendable appendable) {
    try {
      appendable.append('[');
      boolean first = true;
      for (final Object value : collection) {
        if (first) {
          first = false;
        } else {
          appendable.append(',');
        }
        JsonWriterUtil.appendValue(appendable, value);
      }
      appendable.append(']');
      return appendable;
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public static JsonObject clone(final JsonObject object) {
    if (object == null) {
      return null;
    } else {
      return object.clone();
    }
  }

  public static Map<String, Object> getMap(final Map<String, Object> record,
    final String fieldName) {
    final String value = (String)record.get(fieldName);
    return toObjectMap(value);
  }

  public static <V> V parse(final String json) {
    return JsonParser.read(json);
  }

  /**
   * Write any object as a JSON string without any indentation or formatting.
   *
   * @param value The value to write
   * @return
   */
  public static String toJsonString(final Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof final Jsonable jsonable) {
      return jsonable.toJsonString();
    } else if (value instanceof final Number number) {
      return DataTypes.toString(number);
    } else if (value instanceof final Boolean bool) {
      return DataTypes.toString(bool);
    } else if (value instanceof final CharSequence chars) {
      final var s = new StringBuilder();
      JsonWriterUtil.appendText(s, value);
      return s.toString();
    } else {
      return toString(value, false);
    }
  }

  public static JsonObject toMap(final Object source) {
    return toMap(JavaIo.createReader(source));
  }

  public static JsonObject toMap(final Reader in) {
    if (in == null) {
      return null;
    } else {
      try (
        Reader inClosable = in;
        final JsonMapIterator iterator = new JsonMapIterator(in, true)) {
        if (iterator.hasNext()) {
          return iterator.next();
        } else {
          return null;
        }
      } catch (final IOException e) {
        throw new RuntimeException("Unable to read JSON map", e);
      }
    }
  }

  public static Map<String, String> toMap(final String string) {
    final JsonObject map = toObjectMap(string);
    if (map.isEmpty()) {
      return new LinkedHashMap<>();
    } else {
      final Map<String, String> stringMap = new LinkedHashMap<>();
      for (final Entry<String, Object> entry : map.entrySet()) {
        final String key = entry.getKey();
        final Object value = entry.getValue();
        if (value == null) {
          stringMap.put(key, null);
        } else {
          stringMap.put(key, value.toString());
        }
      }
      return stringMap;
    }
  }

  public static final List<JsonObject> toMapList(final Object source) {
    try (
      final Reader in = JavaIo.createReader(source);
      final JsonObjectReader jsonReader = new JsonObjectReader(in)) {
      return jsonReader.toList();
    } catch (final IOException e) {
      return Exceptions.throwUncheckedException(e);
    }
  }

  public static List<JsonObject> toMapList(final String string) {
    final StringReader in = new StringReader(string);
    try (
      final JsonObjectReader reader = new JsonObjectReader(in)) {
      return reader.toList();
    }
  }

  public static JsonObject toObjectMap(final String string) {
    if (Property.hasValue(string)) {
      final StringReader in = new StringReader(string);
      try (
        final JsonObjectReader reader = new JsonObjectReader(in, true)) {
        for (final JsonObject object : reader) {
          return object;
        }
      }
    }
    return new JsonObjectHash();
  }

  public static String toString(final Map<String, ? extends Object> values) {
    final StringWriter writer = new StringWriter();
    try (
      final JsonWriter jsonWriter = new JsonWriter(writer, false)) {
      jsonWriter.write(values);
    }
    return writer.toString();
  }

  public static String toString(final Map<String, ? extends Object> values, final boolean indent) {
    final StringWriter writer = new StringWriter();
    try (
      final JsonWriter jsonWriter = new JsonWriter(writer, indent)) {
      jsonWriter.write(values);
    }
    return writer.toString();
  }

  public static String toString(final Object value) {
    return toString(value, true);
  }

  public static String toString(final Object value, final boolean indent) {
    final StringWriter stringWriter = new StringWriter();
    try (
      JsonWriter jsonWriter = new JsonWriter(stringWriter, indent)) {
      jsonWriter.value(value);
    }
    return stringWriter.toString();
  }

  public static void writeMap(final Map<String, ? extends Object> object, final Object target) {
    writeMap(object, target, true);
  }

  public static void writeMap(final Map<String, ? extends Object> object, final Object target,
    final boolean indent) {
    try (
      final Writer writer = JavaIo.createWriter(target)) {
      writeMap(writer, object, indent);
    } catch (final IOException e) {
    }
  }

  public static void writeMap(final Writer writer, final Map<String, ? extends Object> object) {
    writeMap(writer, object, true);
  }

  public static void writeMap(final Writer writer, final Map<String, ? extends Object> object,
    final boolean indent) {
    try (
      final JsonWriter out = new JsonWriter(writer, indent)) {
      out.write(object);
    } catch (final RuntimeException | Error e) {
      throw e;
    }
  }

}
