package com.revolsys.record.io.format.xml.stax;

import java.io.InputStream;
import java.io.Reader;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamConstants;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.jeometry.common.exception.Exceptions;

import com.revolsys.record.io.format.json.JsonList;
import com.revolsys.record.io.format.json.JsonObject;
import com.revolsys.record.io.format.xml.XsiConstants;

public class StaxToJson {

  private final Set<String> listParentElements = new TreeSet<>();

  private final Set<String> listElements = new TreeSet<>();

  private final Set<String> textElements = new TreeSet<>();

  private final Set<String> dontLogDuplicateElements = new TreeSet<>();

  public StaxToJson dontLogDuplicateElements(final String... names) {
    for (final String name : names) {
      this.dontLogDuplicateElements.add(name);
    }
    return this;
  }

  public StaxToJson listElements(final String... names) {
    for (final String name : names) {
      this.listElements.add(name);
    }
    return this;
  }

  public StaxToJson listParentElements(final String... names) {
    for (final String name : names) {
      this.listParentElements.add(name);
    }
    return this;
  }

  public <V> V process(final HttpResponse r) {
    final HttpEntity entity = r.getEntity();
    try (
      final InputStream in = entity.getContent()) {
      return process(in);
    } catch (final Exception e) {
      throw Exceptions.wrap(e);
    }
  }

  public <V> V process(final HttpResponse r, final V defaultValue) {
    final V value = process(r);
    if (value == null) {
      return defaultValue;
    } else {
      return value;
    }
  }

  public <V> V process(final InputStream in) {
    return process(StaxReader.newXmlReader(in));
  }

  public <V> V process(final Reader in) {
    return process(StaxReader.newXmlReader(in));
  }

  @SuppressWarnings("unchecked")
  public <V> V process(final StaxReader in) {
    in.skipToStartElement();
    return (V)processElement(in);
  }

  private Object processElement(final StaxReader in) {
    if (!"true".equals(in.getAttribute(XsiConstants.NIL))) {
      final String name = in.getLocalName();
      if (this.listParentElements.contains(name)) {
        return processList(in);
      } else {
        int state = in.skipWhitespace();
        switch (state) {
          case XMLStreamConstants.CHARACTERS: {
            final StringBuilder s = new StringBuilder();
            do {
              if (state == XMLStreamConstants.CHARACTERS) {
                final String text = in.getText();
                s.append(text);
              }
              state = in.next();
            } while (state == XMLStreamConstants.CHARACTERS || state == XMLStreamConstants.SPACE);
            return s.toString();
          }
          case XMLStreamConstants.END_ELEMENT:
            return null;

          default:
            return processObject(in);
        }
      }
    }
    return null;
  }

  private JsonList processList(final StaxReader in) {
    final JsonList list = JsonList.array();
    final int depth = in.getDepth();
    while (in.skipToStartElement(depth)) {
      final Object value = processElement(in);
      if (value != null) {
        list.add(value);
      }
    }
    return list;
  }

  private JsonObject processObject(final StaxReader in) {
    final JsonObject object = JsonObject.hash();
    final int depth = in.getDepth() - 1;
    do {
      final String name = in.getLocalName();
      final Object value;
      if (this.textElements.contains(name)) {
        value = in.getAsText();
      } else {
        value = processElement(in);
      }
      if (value != null) {
        if (this.listElements.contains(name)) {
          final JsonList list = object.ensureValue(name, JsonList.ARRAY_SUPPLIER);
          list.add(value);
        } else {
          if (object.hasValue(name) && !this.dontLogDuplicateElements.contains(name)) {
            System.out.println(name);
          }
          object.addNotEmpty(name, value);
        }
      }
    } while (in.skipToStartElement(depth));
    return object;
  }

  public StaxToJson textElements(final String... names) {
    for (final String name : names) {
      this.textElements.add(name);
    }
    return this;
  }
}