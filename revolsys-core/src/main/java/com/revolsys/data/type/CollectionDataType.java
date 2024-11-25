package com.revolsys.data.type;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import com.revolsys.collection.json.Json;
import com.revolsys.collection.json.JsonType;
import com.revolsys.exception.Exceptions;

public class CollectionDataType extends SimpleDataType {

  public static void parseStringCollection(final Collection<Object> newCollection,
    final DataType contentType, final String string) {
    int i = 0;
    if (string.charAt(i) == '[') {
      if (JsonType.class.isAssignableFrom(contentType.getJavaClass())) {
        final List<?> list = Json.toMapList(string);
        newCollection.addAll(list);
        return;
      }
      i++;
    }
    final StringBuilder text = new StringBuilder();
    boolean inString = false;
    for (; i < string.length(); i++) {
      char c = string.charAt(i);
      if (c == ',') {
        if (inString) {
          text.append(',');
        } else {
          final Object element = contentType.toObject(text.toString());
          newCollection.add(element);
          text.setLength(0);
        }
      } else if (c == '"') {
        if (inString) {
          inString = false;
          final Object element = contentType.toObject(text.toString());
          newCollection.add(element);
          text.setLength(0);
          if (i + 1 < string.length()) {
            c = string.charAt(i + 1);
            if (c == ',') {
              i++;
            }
          }
        } else {
          inString = true;
        }
      } else if (c == '\\' && inString) {
        i++;
        c = string.charAt(i);
        switch (c) {
          case 'b':
            text.setLength(text.length() - 1);
          break;
          case '"':
            text.append('"');
          break;
          case '/':
            text.append('/');
          break;
          case '\\':
            text.append('\\');
          break;
          case 'f':
            text.append('\f');
          case 'n':
            text.append('\n');
          break;
          case 'r':
            text.append('\r');
          break;
          case 't':
            text.append('\t');
          break;
          case 'u':
            final char[] buf = new char[] {
              string.charAt(++i), string.charAt(++i), string.charAt(++i), string.charAt(++i)
            };
            final String unicodeText = String.valueOf(buf, 0, 4);
            try {
              final int unicode = Integer.parseInt(unicodeText, 16);
              text.append((char)unicode);
            } catch (final NumberFormatException e) {
              throw e;
            }
          break;
          default:
            throw new IllegalStateException("Invalid escape character: \\" + c);
        }
      } else if (c == ']' && !inString) {
        break;
      } else {
        text.append(c);
      }
    }
    if (text.length() > 0) {
      final var s = text.toString();
      if ("null".equalsIgnoreCase(s)) {
        newCollection.add(null);
      } else {
        newCollection.add(text);
      }
    }
  }

  private final DataType contentType;

  public CollectionDataType(final String name, final Class<?> javaClass,
    final DataType contentType) {
    super(name, javaClass);
    this.contentType = contentType;
  }

  public DataType getContentType() {
    return this.contentType;
  }

  @SuppressWarnings({
    "rawtypes", "unchecked"
  })
  private Collection<Object> newCollection() {
    try {
      final Class<?> javaClass = getJavaClass();
      final Collection<Object> newCollection;
      if (Collection.class == javaClass) {
        newCollection = new ArrayList<>();
      } else if (List.class == javaClass) {
        newCollection = new ArrayList<>();
      } else if (Set.class == javaClass) {
        newCollection = new LinkedHashSet<>();
      } else {
        final Constructor<?> declaredConstructor = javaClass.getDeclaredConstructor();
        newCollection = (Collection)declaredConstructor.newInstance();
      }
      return newCollection;
    } catch (InvocationTargetException | NoSuchMethodException | InstantiationException
        | IllegalAccessException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @SuppressWarnings({
    "unchecked"
  })
  @Override
  protected Object toObjectDo(final Object value) {
    if (value instanceof final Iterable iterable) {
      final Collection<Object> newCollection = newCollection();
      final Consumer<Object> action = newCollection::add;
      iterable.forEach(action);
      return newCollection;
    } else if (value instanceof CharSequence) {
      final Collection<Object> newCollection = newCollection();
      final String string = value.toString();
      parseStringCollection(newCollection, this.contentType, string);
      return newCollection;
    } else {
      return super.toObjectDo(value);
    }
  }
}
