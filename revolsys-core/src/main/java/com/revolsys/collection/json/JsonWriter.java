package com.revolsys.collection.json;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.revolsys.collection.list.Lists;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.exception.Exceptions;
import com.revolsys.io.MapSerializer;
import com.revolsys.number.Doubles;
import com.revolsys.number.Numbers;
import com.revolsys.record.Record;
import com.revolsys.record.io.format.json.JsonStringEncodingWriter;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.Property;

public class JsonWriter implements BaseCloseable {

  public static JsonWriter nullWriter() {
    final Writer nullWriter = Writer.nullWriter();
    return new JsonWriter(nullWriter);
  }

  private int depth = 0;

  private final List<JsonWriterState> depthStack = new ArrayList<>();

  private boolean indent;

  private Writer out;

  private JsonWriterState state = JsonWriterState.START_DOCUMENT;

  private final JsonStringEncodingWriter encodingOut;

  private boolean closeTargetWriter = true;

  private boolean indented = false;

  public JsonWriter(final OutputStream out, final boolean indent) {
    this(new OutputStreamWriter(out), indent);
  }

  public JsonWriter(final Writer out) {
    this(out, true);
  }

  public JsonWriter(final Writer out, final boolean indent) {
    this.out = out;
    this.indent = indent;
    this.encodingOut = new JsonStringEncodingWriter(out);

  }

  private void blockEnd(final JsonWriterState startState, final JsonWriterState endState) {
    if (this.depth > 0) {
      this.depth--;
    }

    if (this.depth < this.depthStack.size()) {
      this.depthStack.remove(this.depth);
    }
    if (this.state != startState) {
      indent();
    }
    writeState(endState);
  }

  private void blockStart(final JsonWriterState state) {
    writeState(state);
    this.depth++;
    this.depthStack.add(state);
  }

  public void charSequence(final CharSequence string) {
    try {
      this.encodingOut.append(string);
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @Override
  public void close() {
    final Writer out = this.out;
    if (out != null) {
      this.out = null;
      try {
        try {
          for (int i = this.depth; i > 0; i--) {
            final JsonWriterState state = this.depthStack.remove(i - 1);
            final char endChar = state.getEndChar();
            out.write(endChar);
          }
          out.flush();
        } catch (final Exception e) {
          throw Exceptions.toRuntimeException(e);
        }
        this.depthStack.clear();
        this.depth = 0;
      } finally {
        if (this.closeTargetWriter) {
          BaseCloseable.closeSilent(out);
        }
      }
    }
  }

  public void endAttribute() {
    if (this.state != JsonWriterState.END_ATTRIBUTE
      && this.state != JsonWriterState.START_DOCUMENT) {
      try {
        this.out.write(',');
        setState(JsonWriterState.END_ATTRIBUTE);
      } catch (final Exception e) {
        throw Exceptions.toRuntimeException(e);
      }
    }
  }

  public void endList() {
    blockEnd(JsonWriterState.START_LIST, JsonWriterState.END_LIST);
  }

  public void endObject() {
    blockEnd(JsonWriterState.START_OBJECT, JsonWriterState.END_OBJECT);
  }

  public void flush() {
    try {
      this.out.flush();
    } catch (final Exception e) {
    }
  }

  public void indent() {
    if (this.indent && !this.indented) {
      this.indented = true;
      try {
        final Writer out = this.out;
        out.write('\n');
        final int depth = this.depth;
        for (int i = 0; i < depth; i++) {
          out.write("  ");
        }
      } catch (final Exception e) {
        throw Exceptions.toRuntimeException(e);
      }
    }
  }

  public boolean isCloseTargetWriter() {
    return this.closeTargetWriter;
  }

  public void label(final String key) {
    if (this.state != JsonWriterState.START_OBJECT && this.state != JsonWriterState.END_ATTRIBUTE) {
      endAttribute();
    }
    try {
      indent();
      string(key);
      this.out.write(": ");
      setState(JsonWriterState.LABEL);
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public void labelValue(final String key, final Object value) {
    label(key);
    value(value);
  }

  public void list(final Iterable<?> values) throws IOException {
    startList();
    for (final Object value : values) {
      value(value);
    }
    endList();
  }

  public void list(final Object... values) throws IOException {
    startList();
    for (final Object value : values) {
      value(value);
    }
    endList();
  }

  public void newLineForce() {
    try {
      this.out.write('\n');
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public void print(final char value) {
    try {
      this.out.write(value);
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public void print(final Object value) {
    if (value != null) {
      try {
        this.out.write(value.toString());
      } catch (final Exception e) {
        throw Exceptions.toRuntimeException(e);
      }
    }
  }

  public JsonWriter setCloseTargetWriter(final boolean closeTargetWriter) {
    this.closeTargetWriter = closeTargetWriter;
    return this;
  }

  public void setIndent(final boolean indent) {
    this.indent = indent;
  }

  private void setState(final JsonWriterState state) {
    this.state = state;
    this.indented = false;
  }

  public void startList() {
    final boolean indent = true;
    startList(indent);
  }

  public void startList(final boolean indent) {
    final JsonWriterState state = this.state;
    if (state == JsonWriterState.START_LIST) {
      if (this.indent) {
        indent();
      }
    } else if (state == JsonWriterState.START_DOCUMENT || state == JsonWriterState.LABEL) {
    } else {
      endAttribute();
      if (indent) {
        indent();
      }
    }
    blockStart(JsonWriterState.START_LIST);
  }

  public void startObject() {
    if (this.state == JsonWriterState.START_LIST) {
      if (this.indent) {
        indent();
      }
    } else if (this.state == JsonWriterState.START_DOCUMENT
      || this.state == JsonWriterState.LABEL) {
    } else {
      endAttribute();
      if (this.indent) {
        indent();
      }
    }
    blockStart(JsonWriterState.START_OBJECT);
  }

  public void string(final String string) {
    try {
      final Writer out = this.out;
      if (string == null) {
        out.write("null");
      } else {
        out.write('"');
        this.encodingOut.write(string);
        out.write('"');
      }
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public void value(final DataType dataType, final Object value) throws IOException {
    valuePre();
    final Writer out = this.out;
    if (value == null) {
      out.write("null");
    } else if (value instanceof final Boolean bool) {
      if (bool) {
        out.write("true");
      } else {
        out.write("false");
      }
    } else if (value instanceof final Number number) {
      out.write(Numbers.toString(number));
    } else if (value instanceof final List list) {
      list(list);
    } else if (value instanceof final Iterable iterable) {
      list(iterable);
    } else if (value instanceof final Map map) {
      write(map);
    } else if (value instanceof final CharSequence string) {
      string(string.toString());
    } else if (dataType == null) {
      string(value.toString());
    } else {
      final String string = dataType.toString(value);
      string(string);
    }
    setState(JsonWriterState.VALUE);

  }

  @SuppressWarnings("unchecked")
  public void value(final Object value) {
    valuePre();
    try {
      if (value == null) {
        this.out.write("null");
      } else if (value instanceof final Boolean bool) {
        if (bool) {
          this.out.write("true");
        } else {
          this.out.write("false");
        }
      } else if (value instanceof final Number number) {
        final double doubleValue = number.doubleValue();
        if (Double.isInfinite(doubleValue) || Double.isNaN(doubleValue)) {
          this.out.write("null");
        } else {
          this.out.write(Doubles.toString(doubleValue));
        }
      } else if (value instanceof final MapSerializer serialzer) {
        final JsonObject map = serialzer.toMap();
        write(map);
      } else if (value instanceof final Collection list) {
        list(list);
      } else if (value instanceof final Iterable list) {
        list(list);
      } else if (value instanceof final Jsonable jsonable) {
        final JsonType json = jsonable.toJson();
        if (value instanceof final JsonObject map) {
          write(map);
        } else if (value instanceof final JsonList list) {
          list(list);
        } else {
          value(json);
        }
      } else if (value instanceof final Map map) {
        write(map);
      } else if (value instanceof final String string) {
        this.out.write('"');
        this.encodingOut.write(string);
        this.out.write('"');
      } else if (value instanceof final CharSequence string) {
        this.out.write('"');
        this.encodingOut.append(string);
        this.out.write('"');
      } else if (value.getClass()
        .isArray()) {
        final List<? extends Object> list = Lists.arrayToList(value);
        list(list);
      } else {
        value(DataTypes.toString(value));
      }
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
    setState(JsonWriterState.VALUE);
  }

  private void valuePre() {
    final JsonWriterState state = this.state;
    if (state == JsonWriterState.LABEL) {
    } else if (state == JsonWriterState.START_LIST) {
      if (this.indent) {
        indent();
      }
    } else {
      if (state != JsonWriterState.END_ATTRIBUTE) {
        endAttribute();
      }
      if (this.indent) {
        indent();
      }
    }
  }

  public <K, V> void write(final Map<K, V> values) {
    startObject();
    if (values != null) {
      for (final Entry<K, V> entry : values.entrySet()) {
        final K key = entry.getKey();
        final Object value = entry.getValue();
        label(key.toString());
        value(value);
      }
    }
    endObject();
  }

  public <K, V> void writeMap(final Map<K, V> values) {
    startObject();
    if (values != null) {
      for (final Entry<K, V> entry : values.entrySet()) {
        final K key = entry.getKey();
        final Object value = entry.getValue();
        label(key.toString());
        value(value);
      }
    }
    endObject();
  }

  public void writeNull() {
    valuePre();
    try {
      this.out.write("null");
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public void writeRecord(final Record record) {
    try {
      startObject();
      final RecordDefinition recordDefinition = record.getRecordDefinition();
      final List<FieldDefinition> fieldDefinitions = recordDefinition.getFieldDefinitions();

      for (final FieldDefinition field : fieldDefinitions) {
        final int fieldIndex = field.getIndex();
        final Object value = record.getValue(fieldIndex);
        if (Property.hasValue(value)) {
          final String name = field.getName();
          label(name);

          final DataType dataType = field.getDataType();
          value(dataType, value);
        }
      }
      endObject();
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  private void writeState(final JsonWriterState state) {
    try {
      setState(state);
      final char c = state.getChar();
      this.out.write(c);
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }
}

enum JsonWriterState {
  START_DOCUMENT, START_OBJECT('{', '}'), END_OBJECT('}'), START_LIST('[',
    ']'), END_LIST(']'), VALUE, LABEL, END_ATTRIBUTE;

  private char c;

  private char endChar;

  private JsonWriterState() {
  }

  private JsonWriterState(final char c) {
    this.c = c;
  }

  private JsonWriterState(final char c, final char endChar) {
    this.c = c;
    this.endChar = endChar;
  }

  public char getChar() {
    return this.c;
  }

  public char getEndChar() {
    return this.endChar;
  }

}
