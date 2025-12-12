package com.revolsys.record.io.format.csv;

import java.io.IOException;
import java.io.Writer;

import com.revolsys.exception.WrappedIoException;
import com.revolsys.util.BaseCloseable;

public class DsvWriter implements BaseCloseable {

  private static final char[] EOL = {
    '\r', '\n'
  };

  private static final char[] QUOTE = {
    '"'
  };

  public static final char[] COMMA = new char[] {
    ','
  };

  private final Writer writer;

  private final char[] fieldSeparator;

  private final char[] buffer = new char[1024];

  public DsvWriter(final Writer writer, final char fieldSeparator) {
    final char[] separatorChars = {
      fieldSeparator
    };
    this(writer, separatorChars);
  }

  public DsvWriter(final Writer writer, final char[] fieldSeparator) {
    this.writer = writer;
    this.fieldSeparator = fieldSeparator;
  }

  @Override
  public void close() {
    BaseCloseable.closeSilent(this.writer);
  }

  /**
   * End a record (write a end of line).
   *
   * @return this
   */
  public DsvWriter endRecord() {
    return rawChars(EOL);
  }

  /**
   * Write the string, escaping " as "".
   *
   * @param chars The string to write.
   * @return this
   */
  public DsvWriter escapedString(final String string) {
    if (string == null || string.length() == 0) {
      return this;
    }
    try {
      int start = 0;
      final int length = string.length();
      for (int i = 0; i < length; i++) {
        final var end = i + 1;
        final var writeLength = end - start;
        final var c = string.charAt(i);
        if (c == '"' || end == length || writeLength == this.buffer.length) {
          string.getChars(start, end, this.buffer, 0);
          this.writer.write(this.buffer, 0, writeLength);
          start = i + 1;
        }
        if (c == '"') {
          quote();
        }
      }
    } catch (final IOException e) {
      throw new WrappedIoException(e);
    }
    return this;
  }

  public void flush() {
    try {
      this.writer.flush();
    } catch (final IOException e) {
      throw new WrappedIoException(e);
    }
  }

  /**
   * Write a quote.
   *
   * @return this
   */
  public DsvWriter quote() {
    return rawChars(QUOTE);
  }

  /**
   * Write the string, escaping " as "".
   *
   * @param chars The string to write.
   * @return this
   */
  public DsvWriter quotedString(final String string) {
    if (string == null || string.length() == 0) {
      return this;
    }
    quote();
    escapedString(string);
    quote();
    return this;
  }

  /**
   * Write the characters without any escaping.
   *
   * @param chars The characters to write.
   * @return this
   */
  public DsvWriter rawChars(final char[] chars) {
    try {
      this.writer.write(chars);
    } catch (final IOException e) {
      throw new WrappedIoException(e);
    }
    return this;
  }

  /**
   * Write the string without any escaping.
   *
   * @param chars The string to write.
   * @return this
   */
  public DsvWriter rawString(final String string) {
    if (string == null) {
      return this;
    }
    // Write in blocks of 1024 bytes as the underlying StreamEncoder
    // creates a new char[] array for each string to write
    try {
      int offset = 0;
      final int length = string.length();
      while (offset < length) {
        final int writeCount = Math.min(this.buffer.length, length - offset);
        string.getChars(offset, offset + writeCount, this.buffer, 0);
        this.writer.write(this.buffer, 0, writeCount);
        offset += writeCount;
      }
    } catch (final IOException e) {
      throw new WrappedIoException(e);
    }
    return this;
  }

  /**
   * Start a value (write a quote).
   *
   * @return this
   */
  public DsvWriter separator() {
    return rawChars(this.fieldSeparator);
  }

}
