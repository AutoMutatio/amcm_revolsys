package com.revolsys.io.file;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.function.Supplier;

import com.revolsys.collection.value.ValueHolder;
import com.revolsys.exception.Exceptions;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.Cancellable;

public class AtomicPathUpdater implements BaseCloseable, Cancellable {

  public static class Builder {
    private final AtomicPathUpdater updater = new AtomicPathUpdater();

    private Builder() {

    }

    public AtomicPathUpdater build() {
      return this.updater;
    }

    public Builder cancellable(final Cancellable cancellable) {
      this.updater.cancellable = cancellable;
      return this;
    }

    public Builder fileExtension(final String fileExtension) {
      this.updater.fileExtension = fileExtension;
      return this;
    }

    public Builder fileName(final String fileName) {
      this.updater.baseFileName = fileName;
      return this;
    }

    public Builder suffix(final Supplier<String> suffix) {
      this.updater.suffix = suffix;
      return this;
    }

    public Builder targetDirectory(final Path targetDirectory) {
      this.updater.targetDirectory = targetDirectory;
      return this;
    }
  }

  private static final CopyOption[] MOVE_OPTIONS = {
    StandardCopyOption.REPLACE_EXISTING
  };

  private static final DateTimeFormatter FORMAT = DateTimeFormatter
    .ofPattern("yyyyMMdd_HHmmss_SSSSSS")
    .withZone(ZoneId.of("UTC"));

  public static Builder builder() {
    return new Builder();
  }

  public static String timestampSuffix() {
    return FORMAT.format(Instant.now());
  }

  private Path targetDirectory;

  private String baseFileName;

  private String fileExtension;

  private final ValueHolder<Path> tempFile = ValueHolder.lazy(this::getTempFileInit);

  private final ValueHolder<Path> targetFile = ValueHolder.lazy(this::getTargetFileInit);

  private boolean cancelled = false;

  private Cancellable cancellable;

  private Supplier<String> suffix;

  private AtomicPathUpdater() {
  }

  @Override
  public void cancel() {
    this.cancelled = true;
  }

  @Override
  public void close() {
    if (this.tempFile.isPresent()) {
      final var tempFile = getTempFile();
      try {
        if (!isCancelled()) {
          final var targetFile = getTargetFile();
          final Path targetPath = targetFile;
          try {
            if (Files.exists(tempFile)) {
              Files.move(tempFile, targetPath, MOVE_OPTIONS);
            }
          } catch (final IOException e) {
            throw Exceptions.wrap("Error moving file: " + tempFile + " to " + targetPath, e);
          }
        }
      } finally {
        Paths.deleteDirectories(tempFile);
      }
    }
  }

  public Path getTargetDirectory() {
    return this.targetDirectory;
  }

  public Path getTargetFile() {
    return this.targetFile.getValue();
  }

  private Path getTargetFileInit() {
    final var fileName = new StringBuilder(this.baseFileName);
    if (this.suffix != null) {
      fileName.append(this.suffix.get());
    }
    fileName.append('.').append(this.fileExtension);
    final var file = this.targetDirectory.resolve(fileName.toString());
    Paths.createParentDirectories(file);
    return file;
  }

  public Path getTargetPath() {
    return this.targetDirectory.resolve(this.baseFileName);
  }

  public Path getTempFile() {
    return this.tempFile.getValue();
  }

  private Path getTempFileInit() {
    final var file = this.targetDirectory
      .resolve("." + this.baseFileName + "_" + timestampSuffix() + "." + this.fileExtension);
    Paths.createParentDirectories(file);
    return file;
  }

  @Override
  public boolean isCancelled() {
    return this.cancelled || this.cancellable != null && this.cancellable.isCancelled();
  }

  @Override
  public String toString() {
    final StringBuilder s = new StringBuilder(this.targetDirectory.toString()).append('/')
      .append(this.baseFileName);
    if (this.suffix != null) {
      s.append(this.suffix.get());
    }
    return s.append('.').append(this.fileExtension).toString();
  }

}
