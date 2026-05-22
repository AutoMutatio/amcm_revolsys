package com.revolsys.record.io.format.csv;

import java.io.OutputStream;
import java.nio.CharBuffer;

import com.revolsys.record.io.BufferedWriterEx;
import com.revolsys.util.BaseCloseable;

public class DsvWriter implements BaseCloseable {

  public static CharBuffer comma() {
    return CharBuffer.wrap(new char[] {
      ','
    });
  }

  private final char[] eol = new char[] {
    '\r', '\n'
  };

  private final char quote = '"';

  private int inQuotedCount = 0;

  private final BufferedWriterEx writer;

  private final char fieldSeparator;

  public DsvWriter(final BufferedWriterEx writer, final char fieldSeparator) {
    this.writer = writer;
    this.fieldSeparator = fieldSeparator;
  }

  public DsvWriter(final OutputStream out, final char fieldSeparator) {
    final var writer = BufferedWriterEx.forStream(out);
    this(writer, fieldSeparator);
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
    this.writer.write(this.eol);
    return this;
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
    int start = 0;
    final int length = string.length();
    for (int i = 0; i < length; i++) {
      final var end = i + 1;
      final var writeCount = end - start;
      final var c = string.charAt(i);
      if (c == '"' || end == length) {
        this.writer.write(string, start, writeCount);
        start = i + 1;
      }
      if (c == '"') {
        this.writer.append(this.quote);
      }
    }
    return this;
  }

  public void flush() {
    this.writer.flush();
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
    quoteStart();
    escapedString(string);
    quoteEnd();
    return this;
  }

  /**
   * Start a string wrapped in quoutes. Uses the inQuotedCount to
   * allow nested calls to quotedString.
   *
   * @return this
   */
  public DsvWriter quoteEnd() {
    this.inQuotedCount--;
    if (this.inQuotedCount == 0) {
      this.writer.append(this.quote);
    }
    return this;
  }

  /**
   * Start a string wrapped in quoutes. Uses the inQuotedCount to
   * allow nested calls to quotedString.
   *
   * @return this
   */
  public DsvWriter quoteStart() {
    if (this.inQuotedCount == 0) {
      this.writer.append(this.quote);
    }
    this.inQuotedCount++;
    return this;
  }

  /**
   * Write the characters without any escaping.
   *
   * @param chars The characters to write.
   * @return this
   */
  public DsvWriter rawChars(final char[] chars) {
    this.writer.write(chars);
    return this;
  }

  /**
   * Write the characters without any escaping.
   *
   * @param chars The characters to write.
   * @return this
   */
  public DsvWriter rawChars(final CharBuffer chars) {
    this.writer.append(chars);
    return this;
  }

  /**
   * Write the string without any escaping.
   *
   * @param chars The string to write.
   * @return this
   */
  public DsvWriter rawString(final String string) {
    this.writer.write(string);
    return this;
  }

  /**
   * Start a value (write a quote).
   *
   * @return this
   */
  public DsvWriter separator() {
    this.writer.append(this.fieldSeparator);
    return this;
  }

}
