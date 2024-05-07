package com.revolsys.record.io.format.xml.stax;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;

import com.revolsys.collection.json.JsonList;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.set.Sets;
import com.revolsys.exception.Exceptions;
import com.revolsys.record.io.format.xml.XsiConstants;
import com.revolsys.util.CaseConverter;

public class StaxToJson {

  private static final Set<String> EXCLUDE_ATTRIBUTE_NAMESPACES = Sets.newHash("xsi", "xsd",
    "xlink", "xi");

  private static final Function<String, String> TO_LOWER_CASE = CaseConverter::toLowerCamelCase;

  private static final Function<String, String> INTERN = String::intern;

  private final Set<String> ignoreElements = new TreeSet<>();

  private final Set<String> listParentElements = new TreeSet<>();

  private final Set<String> listElements = new TreeSet<>();

  private final Set<String> textElements = new TreeSet<>();

  private final Set<String> dontLogDuplicateElements = new TreeSet<>();

  private boolean includeAttributes = true;

  private final Map<String, String> attributes = new LinkedHashMap<>();

  private boolean stripValues = false;

  private boolean toLowerCamelCase;

  private Function<String, String> fieldNameFactory = INTERN;

  private final ConcurrentHashMap<String, String> fieldNameMap = new ConcurrentHashMap<>();

  public StaxToJson dontLogDuplicateElements(final String... names) {
    for (final String name : names) {
      this.dontLogDuplicateElements.add(name);
    }
    return this;
  }

  public StaxToJson ignoreElements(final String... names) {
    for (final String name : names) {
      this.ignoreElements.add(name);
    }
    return this;
  }

  public boolean isIncludeAttributes() {
    return this.includeAttributes;
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
      throw Exceptions.toRuntimeException(e);
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
    final BufferedInputStream inputStream = new BufferedInputStream(in);
    final StaxReader xmlReader = StaxReader.newXmlReader(inputStream);
    return process(xmlReader);
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
      if (this.ignoreElements.contains(name)) {
        in.skipSubTree();
        return null;
      } else if (this.listParentElements.contains(name)) {
        return processList(in);
      } else {
        try {
          if (isIncludeAttributes()) {
            for (int i = 0; i < in.getAttributeCount(); i++) {
              final QName qName = in.getAttributeName(i);
              if (!EXCLUDE_ATTRIBUTE_NAMESPACES.contains(qName.getPrefix())) {
                final var attName = qName.getLocalPart();
                final String value = in.getAttributeValue(i)
                  .strip();
                if (!value.isBlank()) {
                  final var fieldName = toFieldName(attName);
                  this.attributes.put(fieldName, value);
                }
              }
            }
          }
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
              if (!this.attributes.isEmpty()) {
                final JsonObject object = JsonObject.hash();
                object.addAll(this.attributes);
                return object;
              }
              return null;

            default:
              return processObject(in);
          }
        } finally {
          this.attributes.clear();
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
    object.addAll(this.attributes);
    this.attributes.clear();
    final int depth = in.getDepth() - 1;
    do {
      final String name = in.getLocalName();
      Object value;
      if (this.textElements.contains(name)) {
        value = in.getAsText();
      } else {
        value = processElement(in);
      }
      if (value != null) {
        final var fieldName = toFieldName(name);
        if (this.listElements.contains(name)) {
          final JsonList list = object.ensureValue(fieldName, JsonList.ARRAY_SUPPLIER);
          list.add(value);
        } else {
          if (object.hasValue(name) && !this.dontLogDuplicateElements.contains(name)) {
            System.out.println(name);
          }
          if (this.stripValues && value instanceof final String s) {
            value = s.strip();
          }
          object.addNotEmpty(fieldName, value);
        }
      }
    } while (in.skipToStartElement(depth));
    return object;

  }

  public StaxToJson setIncludeAttributes(final boolean includeAttributes) {
    this.includeAttributes = includeAttributes;
    return this;
  }

  public StaxToJson stripValues(final boolean stripValues) {
    this.stripValues = stripValues;
    return this;
  }

  public StaxToJson textElements(final String... names) {
    for (final String name : names) {
      this.textElements.add(name);
    }
    return this;
  }

  private String toFieldName(final String name) {
    return this.fieldNameMap.computeIfAbsent(name, this.fieldNameFactory);
  }

  public StaxToJson toLowerCamelCase() {
    this.fieldNameFactory = TO_LOWER_CASE;
    return this;
  }

}
