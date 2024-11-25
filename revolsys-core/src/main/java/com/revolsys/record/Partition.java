package com.revolsys.record;

import java.nio.file.Path;

import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.RecordDefinition;

public record Partition(Partition parent, Path path, String key, Object value, String fullName) {

  private static Path parentPath(final Partition parent, final String fullName) {
    if (parent == null) {
      return Path.of(fullName);
    } else {
      return parent.path()
        .resolve(fullName);
    }
  }

  private Partition(final Partition parent, final String key, final Object value) {
    this(parent, key, value, key + "=" + value);
  }

  private Partition(final Partition parent, final String key, final Object value,
    final String fullName) {
    this(parent, parentPath(parent, fullName), key, value, fullName);
  }

  public Partition(final String key, final Object value) {
    this(null, key, value);
  }

  public Partition child(final String key, final Object value) {
    return new Partition(this, key, value);
  }

  public Path fullPath(final Path root) {
    return root.resolve(this.path);
  }

  public Path fullPath(final Path root, final String typeName) {
    return root.resolve(typeName)
      .resolve(this.path);
  }

  public RecordWriter newMultiWriter(final Path root, final String typeName,
    final RecordDefinition recordDefinition, final Iterable<String> formats) {
    final Path dir = fullPath(root, typeName);
    return RecordWriter.newMultiFormat(dir, "data", recordDefinition, formats);
  }

}
