package com.revolsys.data.type;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import com.revolsys.collection.list.ArrayListEx;
import com.revolsys.collection.list.ListEx;

public class ListDataType extends SimpleDataType {

  public static ListDataType of(final DataType contentType) {
    return new ListDataType(ListEx.class, contentType);
  }

  private final DataType contentType;

  public ListDataType(final Class<?> javaClass, final DataType contentType) {
    this("List", javaClass, contentType);
  }

  public ListDataType(final String name, final Class<?> javaClass, final DataType contentType) {
    super(name, javaClass);
    this.contentType = contentType;
  }

  @SuppressWarnings("unchecked")
  private ListEx<Object> createList() throws NoSuchMethodException, InstantiationException,
    IllegalAccessException, InvocationTargetException {
    final Class<?> javaClass = getJavaClass();
    final ListEx<Object> newCollection;
    if (ListEx.class == javaClass) {
      newCollection = new ArrayListEx<>();
    } else {
      final Constructor<?> declaredConstructor = javaClass.getDeclaredConstructor();
      newCollection = (ListEx)declaredConstructor.newInstance();
    }
    return newCollection;
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2) {
    final ListEx<?> list1 = (ListEx<?>)value1;
    final ListEx<?> list2 = (ListEx<?>)value2;
    if (list1.size() != list2.size()) {
      return false;
    } else {
      for (int i = 0; i < list1.size(); i++) {
        final Object value11 = list1.get(i);
        final Object value21 = list2.get(i);
        if (!DataType.equal(value11, value21)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  protected boolean equalsNotNull(final Object value1, final Object value2,
    final Collection<? extends CharSequence> excludeFieldNames) {
    final ListEx<?> list1 = (ListEx<?>)value1;
    final ListEx<?> list2 = (ListEx<?>)value2;
    if (list1.size() != list2.size()) {
      return false;
    } else {
      for (int i = 0; i < list1.size(); i++) {
        final Object value11 = list1.get(i);
        final Object value21 = list2.get(i);
        if (!DataType.equal(value11, value21, excludeFieldNames)) {
          return false;
        }
      }
    }
    return true;
  }

  public DataType getContentType() {
    return this.contentType;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Object toObjectDo(final Object value) {
    if (value instanceof Collection) {
      try {
        final Collection<?> collection = (Collection<?>)value;
        final ListEx<Object> list = createList();
        list.addAll(collection);
        return list;
      } catch (InvocationTargetException | NoSuchMethodException | InstantiationException
          | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    } else if (value instanceof final Iterable iterable) {
      try {
        final ListEx<Object> list = createList();
        iterable.forEach(list::add);
        return list;
      } catch (InvocationTargetException | NoSuchMethodException | InstantiationException
          | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    } else if (value instanceof CharSequence) {
      try {
        // TODO
        final ListEx<Object> list = createList();
        final String string = value.toString();
        int i = 0;
        if (string.charAt(i) == '[') {
          i++;
        }
        final StringBuilder text = new StringBuilder();
        boolean inString = false;
        for (; i < string.length(); i++) {
          char c = string.charAt(i);
          if (c == ',') {
            final Object element = this.contentType.toObject(text.toString());
            list.add(element);
            text.setLength(0);
          } else if (c == '"') {
            if (inString) {
              inString = false;
              final Object element = this.contentType.toObject(text.toString());
              list.add(element);
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
        return list;
      } catch (InvocationTargetException | NoSuchMethodException | InstantiationException
          | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    } else {
      return super.toObjectDo(value);
    }
  }

  @Override
  public String toString() {
    return super.toString() + "<" + this.contentType + ">";
  }
}
