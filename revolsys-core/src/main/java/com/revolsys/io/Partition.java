package com.revolsys.io;

import java.nio.file.Path;

public record Partition(Partition parent, Path path, String key, Object value, String fullName) {

  private Partition(final Partition parent, final String key, final Object value) {
    this(parent, key, value, key + "=" + value);
  }

  private Partition(final Partition parent, final String key, final Object value,
    final String fullName) {
    this(parent, parent.path().resolve(fullName), key, value, fullName);
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
}
