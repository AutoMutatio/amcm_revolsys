package com.revolsys.record.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.revolsys.exception.Exceptions;
import com.revolsys.exception.WrappedIoException;

public class BufferedWriterEx extends Writer {

  private final CharBuffer buffer;

  private final char[] chars;

  private final Writer writer;

  public BufferedWriterEx(final OutputStream out) {
    this(out, StandardCharsets.UTF_8, 8192);
  }

  public BufferedWriterEx(final OutputStream out, final Charset charset, final int bufferSize) {
    final var writer = new OutputStreamWriter(out, charset);
    this(writer, bufferSize);
  }

  public BufferedWriterEx(final OutputStream out, final int bufferSize) {
    this(out, StandardCharsets.UTF_8, bufferSize);
  }

  public BufferedWriterEx(final Writer writer, final int bufferSize) {
    if (bufferSize < 1024) {
      throw new IndexOutOfBoundsException(bufferSize);
    }
    this.writer = writer;
    Objects.checkIndex(bufferSize, 1048576);
    this.chars = new char[bufferSize];
    this.buffer = CharBuffer.wrap(this.chars);
  }

  @Override
  public Writer append(final char c) {
    this.buffer.put(c);
    flushBufferIfNeeded();
    return this;
  }

  @Override
  public Writer append(final CharSequence charSequence) {
    try {
      if (charSequence instanceof final CharBuffer buffer) {
        flushBuffer();
        if (this.writer instanceof OutputStreamWriter) {
          this.writer.append(buffer);
        } else {
          final int position = buffer.position();
          try {
            this.writer.append(charSequence);
          } finally {
            buffer.position(position);
          }
        }
        flushBufferIfNeeded();
      } else {
        append(charSequence, 0, charSequence.length());
      }
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return this;
  }

  @Override
  public Writer append(final CharSequence charSequence, final int start, final int end) {
    if (charSequence == null) {
      return this;
    }
    int i = start;
    while (i < end) {
      final int writeCount = Math.min(this.buffer.remaining(), end - i);
      final int bufferOffset = this.buffer.position();
      charSequence.getChars(i, i + writeCount, this.chars, bufferOffset);
      this.buffer.position(bufferOffset + writeCount);
      flushBufferIfNeeded();
      i += writeCount;
    }
    return this;
  }

  @Override
  public void close() {
    try {
      flushBuffer();
      this.writer.close();
    } catch (final IOException e) {
      throw new WrappedIoException(e);
    }
  }

  @Override
  public void flush() {
    flushBuffer();
    try {
      this.writer.flush();
    } catch (final IOException e) {
      throw new WrappedIoException(e);
    }
  }

  private void flushBuffer() {
    try {
      this.buffer.flip();
      this.writer.append(this.buffer);
      this.buffer.clear();
    } catch (final IOException e) {
      throw new WrappedIoException(e);
    }
  }

  /**
   * This should be called after each write operation to flush full buffers.
   */
  private void flushBufferIfNeeded() {
    if (!this.buffer.hasRemaining()) {
      flushBuffer();
    }
  }

  @Override
  public void write(final char[] chars) {
    if (chars == null) {
      return;
    }
    write(chars, 0, chars.length);
  }

  @Override
  public void write(final char[] chars, final int offset, final int length) {
    if (chars == null) {
      return;
    }
    final int end = offset + length;
    int i = offset;
    while (i < end) {
      final int writeCount = Math.min(this.buffer.remaining(), end - i);
      this.buffer.put(chars, i, writeCount);
      flushBufferIfNeeded();
      i += writeCount;
    }
  }

  @Override
  public void write(final int c) {
    append((char)c);
  }

  @Override
  public void write(final String string) {
    if (string == null) {
      return;
    }
    append(string, 0, string.length());
  }

  @Override
  public void write(final String string, final int offset, final int length) {
    append(string, offset, offset + length);
  }

}
