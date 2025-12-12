package com.revolsys.record.io.format.csv;

import java.io.IOException;
import java.io.Writer;
import java.nio.CharBuffer;

import com.revolsys.exception.WrappedIoException;
import com.revolsys.util.BaseCloseable;

public class DsvWriter implements BaseCloseable {

  public static CharBuffer comma() {
    return CharBuffer.wrap(new char[] {
      ','
    });
  }

  private final CharBuffer eol = CharBuffer.wrap(new char[] {
    '\r', '\n'
  });

  private final CharBuffer quote = CharBuffer.wrap(new char[] {
    '"'
  });

  private int inQuotedCount = 0;

  private final Writer writer;

  private final CharBuffer fieldSeparator;

  private final char[] charsBuffer = new char[1024];

  private final CharBuffer charBuffer = CharBuffer.wrap(this.charsBuffer);

  public DsvWriter(final Writer writer, final char fieldSeparator) {
    final char[] separatorChars = {
      fieldSeparator
    };
    this(writer, separatorChars);
  }

  public DsvWriter(final Writer writer, final char[] fieldSeparator) {
    this.writer = writer;
    this.fieldSeparator = CharBuffer.wrap(fieldSeparator);
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
    return rawCharsClear(this.eol);
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
        final var writeCount = end - start;
        final var c = string.charAt(i);
        if (c == '"' || end == length || writeCount == this.charsBuffer.length) {
          string.getChars(start, end, this.charsBuffer, 0);
          this.charBuffer.clear();
          this.charBuffer.limit(writeCount);
          this.writer.append(this.charBuffer);
          start = i + 1;
        }
        if (c == '"') {
          rawCharsClear(this.quote);
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
    if (this.inQuotedCount == 0) {
      rawCharsClear(this.quote);
    }
    this.inQuotedCount++;
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
      rawCharsClear(this.quote);
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
    try {
      this.writer.write(chars);
    } catch (final IOException e) {
      throw new WrappedIoException(e);
    }
    return this;
  }

  /**
   * Write the characters without any escaping.
   *
   * @param chars The characters to write.
   * @return this
   */
  public DsvWriter rawChars(final CharBuffer chars) {
    try {
      this.writer.append(chars);
    } catch (final IOException e) {
      throw new WrappedIoException(e);
    }
    return this;
  }

  public DsvWriter rawCharsClear(final CharBuffer chars) {
    chars.clear();
    return rawChars(chars);
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
        final int writeCount = Math.min(this.charsBuffer.length, length - offset);
        string.getChars(offset, offset + writeCount, this.charsBuffer, 0);
        this.charBuffer.clear();
        this.charBuffer.limit(writeCount);
        this.writer.append(this.charBuffer);
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
    return rawCharsClear(this.fieldSeparator);
  }

}
