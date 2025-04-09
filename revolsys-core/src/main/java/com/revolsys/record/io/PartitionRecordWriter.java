package com.revolsys.record.io;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.value.ValueHolder;
import com.revolsys.data.type.DataTypes;
import com.revolsys.io.AbstractRecordWriter;
import com.revolsys.io.file.AtomicPathUpdater;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.BaseCloseable;

public class PartitionRecordWriter extends AbstractRecordWriter {

  private record PartitionFile(Path path, RecordWriter writer, AtomicPathUpdater updater)
    implements BaseCloseable {
    @Override
    public void close() {
      try {
        this.writer.close();
      } catch (RuntimeException | Error e) {
        this.updater.cancel();
        throw e;
      } finally {
        this.updater.close();
      }
    }

    public void write(final Record record) {
      this.writer.write(record);
    }
  }

  private final ValueHolder<String> suffix = ValueHolder.lazy(this::suffixInit);

  private Supplier<String> suffixSupplier;

  private final Path basePath;

  private final ListEx<ListEx<String>> partitionFieldSets = Lists.newArray();

  private final ConcurrentHashMap<Path, PartitionFile> writerByPath = new ConcurrentHashMap<>();

  public PartitionRecordWriter(final Path basePath, final RecordDefinition recordDefinition) {
    super(recordDefinition);
    this.basePath = basePath;
  }

  @Override
  public void close() {
    super.close();
    BaseCloseable.closeSilent(this.writerByPath.values());
    this.writerByPath.clear();
  }

  public PartitionRecordWriter fieldSet(final String... fieldNames) {
    this.partitionFieldSets.add(Lists.newArray(fieldNames));
    return this;
  }

  private PartitionFile newWriter(final Path directory) {
    Paths.createDirectories(directory);

    final var updater = AtomicPathUpdater.builder()
      .targetDirectory(directory)
      .fileName("data")
      .suffix(this.suffix)
      .fileExtension("tsv")
      .build();
    final Path dataFile = updater.getTempFile();
    final var writer = RecordWriter.newRecordWriter(this.recordDefinition, dataFile);
    writer.setProperty("jsonFormat", true);
    return new PartitionFile(directory, writer, updater);
  }

  public PartitionRecordWriter suffix(final Supplier<String> suffixSupplier) {
    this.suffixSupplier = suffixSupplier;
    return this;
  }

  private String suffixInit() {
    if (this.suffixSupplier != null) {
      final var suffix = this.suffixSupplier.get();
      if (suffix.length() > 0) {
        return "_" + suffix;
      }
    }
    return "";
  }

  @Override
  public void write(final Record record) {
    writeRecord(this.basePath, record);
    for (final var partitionFields : this.partitionFieldSets) {
      writeParition(this.basePath, record, partitionFields);
    }

  }

  @SuppressWarnings({
    "unchecked",
  })
  private void writeParition(final Path messagePath, final Record record,
    final ListEx<String> partitionFields) {
    if (!partitionFields.isEmpty()) {
      final String fieldName = partitionFields.get(0);
      final var subFields = partitionFields.subList(1);
      final Path fieldPath = messagePath.resolve("[" + fieldName + "]");
      final Object value = record.getValue(fieldName);
      Iterable<Object> values;
      if (value == null) {
        values = Lists.empty();
      } else if (value instanceof final Collection list) {
        values = list;
      } else {
        values = Lists.newArray(value);
      }
      for (final var v : values) {
        if (v != null) {
          final Path valuePath = fieldPath.resolve(DataTypes.toString(v));
          writeRecord(valuePath, record);
          writeParition(valuePath, record, subFields);
        }
      }
    }
  }

  private void writeRecord(final Path path, final Record record) {
    final var writer = this.writerByPath.computeIfAbsent(path, this::newWriter);
    writer.write(record);
  }

}
