package com.revolsys.record.io;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.geometry.model.ClockDirection;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.io.FileUtil;
import com.revolsys.io.IoFactory;
import com.revolsys.io.MultiRecordWriter;
import com.revolsys.io.Writer;
import com.revolsys.io.file.Paths;
import com.revolsys.record.ArrayRecord;
import com.revolsys.record.Record;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.spring.resource.Resource;

public interface RecordWriter extends Writer<Record>, RecordDefinitionProxy {
  static boolean isWritable(final File file) {
    for (final String fileNameExtension : FileUtil.getFileNameExtensions(file)) {
      if (isWritable(fileNameExtension)) {
        return true;
      }
    }
    return false;
  }

  static boolean isWritable(final String fileNameExtension) {
    return IoFactory.isAvailable(RecordWriterFactory.class, fileNameExtension);
  }

  public static RecordWriter newMultiFormat(final Path parent, final String baseName,
    final RecordDefinition recordDefinition, final Iterable<String> formats) {
    final ListEx<RecordWriter> writers = Lists.newArray();
    Paths.createDirectories(parent);
    for (final var format : formats) {
      final var path = parent.resolve(baseName + "." + format);
      final var writer = newRecordWriter(recordDefinition, path);
      writers.add(writer);
    }
    return new MultiRecordWriter(recordDefinition, writers);
  }

  public static RecordWriter newMultiFormat(final Path parent, final String baseName,
    final RecordDefinition recordDefinition, final String... formats) {
    final ListEx<RecordWriter> writers = Lists.newArray();
    for (final var format : formats) {
      final var path = parent.resolve(baseName + "." + format);
      final var writer = newRecordWriter(recordDefinition, path);
      writers.add(writer);
    }
    return new MultiRecordWriter(recordDefinition, writers);
  }

  static RecordWriter newRecordWriter(final RecordDefinitionProxy recordDefinition,
    final Object target) {
    if (recordDefinition != null) {
      final RecordDefinition definition = recordDefinition.getRecordDefinition();
      if (definition != null) {
        final Resource resource = Resource.getResource(target);
        final RecordWriterFactory writerFactory = IoFactory.factory(RecordWriterFactory.class,
          resource);
        if (writerFactory != null) {
          final RecordWriter writer = writerFactory.newRecordWriter(definition, resource);
          return writer;
        }
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  static <R extends RecordWriter> R newRecordWriter(final String fileExtension,
    final RecordDefinitionProxy recordDefinition, final Object target) {
    if (recordDefinition != null) {
      final RecordDefinition definition = recordDefinition.getRecordDefinition();
      if (definition != null) {
        final Resource resource = Resource.getResource(target);
        final RecordWriterFactory writerFactory = IoFactory
          .factoryByFileExtension(RecordWriterFactory.class, fileExtension);
        if (writerFactory != null) {
          final RecordWriter writer = writerFactory.newRecordWriter(definition, resource);
          return (R)writer;
        }
      }
    }
    return null;
  }

  default RecordWriter composeWith(final RecordWriter writer) {
    return new MultiRecordWriter(this, this, writer);
  }

  @Override
  default GeometryFactory getGeometryFactory() {
    final RecordDefinition recordDefinition = getRecordDefinition();
    if (recordDefinition == null) {
      return GeometryFactory.DEFAULT_2D;
    } else {
      return recordDefinition.getGeometryFactory();
    }
  }

  default ClockDirection getPolygonRingDirection() {
    return ClockDirection.NONE;
  }

  @Override
  default RecordDefinition getRecordDefinition() {
    return null;
  }

  boolean isIndent();

  default boolean isValueWritable(final Object value) {
    return value != null || isWriteNulls() || value instanceof Geometry;
  }

  boolean isWriteCodeValues();

  boolean isWriteNulls();

  default Record newRecord() {
    final RecordDefinition recordDefinition = getRecordDefinition();
    return new ArrayRecord(recordDefinition);
  }

  default Record newRecord(final Iterable<? extends Object> values) {
    final RecordDefinition recordDefinition = getRecordDefinition();
    return new ArrayRecord(recordDefinition, values);
  }

  default Record newRecord(final Map<String, ? extends Object> values) {
    final RecordDefinition recordDefinition = getRecordDefinition();
    return new ArrayRecord(recordDefinition, values);
  }

  default Record newRecord(final Object... values) {
    final RecordDefinition recordDefinition = getRecordDefinition();
    return new ArrayRecord(recordDefinition, values);
  }

  void setIndent(final boolean indent);

  void setWriteCodeValues(boolean writeCodeValues);

  void setWriteNulls(boolean writeNulls);

  default void write(final Iterable<? extends Object> values) {
    final Record record = newRecord(values);
    write(record);
  }

  default void write(final Map<String, ? extends Object> map) {
    final Record record = newRecord(map);
    write(record);
  }

  default void write(final Object... values) {
    final Record record = newRecord(values);
    write(record);
  }

  default int writeAll(final Iterable<? extends Record> records) {
    int i = 0;
    for (final Record record : records) {
      write(record);
      i++;
    }
    return i;
  }

  default <I, R extends Record> int writeAll(final Iterable<I> values,
    final Function<I, R> mapper) {
    int i = 0;
    for (final I value : values) {
      final R record = mapper.apply(value);
      write(record);
      i++;
    }
    return i;
  }

  default void writeNewRecord(final Record record) {
    final Record writeRecord = newRecord(record);
    write(writeRecord);
  }

  default void writeNewRecords(final Iterable<? extends Record> records) {
    for (final Record record : records) {
      writeNewRecord(record);
    }
  }
}
