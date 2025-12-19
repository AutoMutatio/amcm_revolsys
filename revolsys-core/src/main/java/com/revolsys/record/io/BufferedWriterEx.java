package com.revolsys.record.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.revolsys.exception.Exceptions;
import com.revolsys.exception.WrappedIoException;

public class BufferedWriterEx extends Writer {

  // private static final Method STREAM_ENCODER_CONSTRUCTOR;
  // static {
  // try {
  // STREAM_ENCODER_CONSTRUCTOR = Class.forName("sun.nio.cs.StreamEncoder")
  // .getMethod("forOutputStreamWriter", OutputStream.class, Object.class,
  // Charset.class);
  // } catch (NoSuchMethodException | ClassNotFoundException e) {
  // throw Exceptions.toRuntimeException(e);
  // }
  //
  // }

  private static final int DEFAULT_BUFFER_SIZE = 8192;

  public static BufferedWriterEx forChannel(final WritableByteChannel channel,
    final Charset charset) {
    final var writer = Channels.newWriter(channel, charset);
    return new BufferedWriterEx(writer, DEFAULT_BUFFER_SIZE);
  }

  public static BufferedWriterEx forChannel(final WritableByteChannel channel,
    final Charset charset, final int bufferSize) {
    final var writer = Channels.newWriter(channel, charset);
    return new BufferedWriterEx(writer, bufferSize);
  }

  public static BufferedWriterEx forStream(final OutputStream out) {
    final var writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
    return new BufferedWriterEx(writer, DEFAULT_BUFFER_SIZE);
  }

  public static BufferedWriterEx forStream(final OutputStream out, final Charset charset) {
    final var writer = new OutputStreamWriter(out, charset);
    return new BufferedWriterEx(writer, DEFAULT_BUFFER_SIZE);
  }

  public static BufferedWriterEx forStream(final OutputStream out, final Charset charset,
    final int bufferSize) {
    final var writer = new OutputStreamWriter(out, charset);
    return new BufferedWriterEx(writer, bufferSize);
  }

  public static BufferedWriterEx forStream(final OutputStream out, final int bufferSize) {
    return forStream(out, StandardCharsets.UTF_8, bufferSize);
  }

  public static BufferedWriterEx forWriter(final Writer writer) {
    if (writer instanceof final BufferedWriterEx bufferedWriter) {
      return bufferedWriter;
    }
    return new BufferedWriterEx(writer, DEFAULT_BUFFER_SIZE);
  }

  public static BufferedWriterEx forWriter(final Writer writer, final int bufferSize) {
    if (writer instanceof final BufferedWriterEx bufferedWriter) {
      return bufferedWriter;
    }
    return new BufferedWriterEx(writer, bufferSize);
  }

  // private static Writer newWriter(final OutputStream out, final Charset
  // charset) {
  //
  // try {
  // return (Writer)STREAM_ENCODER_CONSTRUCTOR.invoke(null, out, out, charset);
  // } catch (final IllegalAccessException e) {
  // throw Exceptions.toRuntimeException(e);
  // } catch (final InvocationTargetException e) {
  // throw Exceptions.toRuntimeException(e);
  // }
  // // return new OutputStreamWriter(out, charset);
  // }

  private final CharBuffer buffer;

  private final char[] chars;

  private final Writer writer;

  private boolean closed;

  private BufferedWriterEx(final Writer writer, final int bufferSize) {
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
    final var buffer = this.buffer;
    buffer.put(c);
    if (!buffer.hasRemaining()) {
      flushBuffer();
    }
    return this;
  }

  @Override
  public Writer append(final CharSequence charSequence) {
    try {
      if (charSequence instanceof final CharBuffer buffer) {
        flushBuffer();
        final var writer = this.writer;
        if (writer instanceof OutputStreamWriter) {
          writer.append(buffer);
        } else {
          final int position = buffer.position();
          try {
            writer.append(charSequence);
          } finally {
            buffer.position(position);
          }
        }
        if (!this.buffer.hasRemaining()) {
          flushBuffer();
        }
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
    final var buffer = this.buffer;
    final var chars = this.chars;
    int i = start;
    while (i < end) {
      final int writeCount = Math.min(buffer.remaining(), end - i);
      final int bufferOffset = buffer.position();
      charSequence.getChars(i, i + writeCount, chars, bufferOffset);
      buffer.position(bufferOffset + writeCount);
      if (!buffer.hasRemaining()) {
        flushBuffer();
      }
      i += writeCount;
    }
    return this;
  }

  @Override
  public void close() {
    if (!this.closed) {
      this.closed = true;
      try {
        flushBuffer();
        this.writer.close();
      } catch (final IOException e) {
        throw new WrappedIoException(e);
      }
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

  public void newLine() {
    append('\n');
  }

  @Override
  public void write(final char[] chars) {
    if (chars == null) {
      return;
    }
    final var buffer = this.buffer;
    final int end = chars.length;
    int i = 0;
    while (i < end) {
      final int writeCount = Math.min(buffer.remaining(), end - i);
      buffer.put(chars, i, writeCount);
      if (!buffer.hasRemaining()) {
        flushBuffer();
      }
      i += writeCount;
    }
  }

  @Override
  public void write(final char[] chars, final int offset, final int length) {
    if (chars == null) {
      return;
    }
    final var buffer = this.buffer;
    final int end = offset + length;
    int i = offset;
    while (i < end) {
      final int writeCount = Math.min(buffer.remaining(), end - i);
      buffer.put(chars, i, writeCount);
      if (!buffer.hasRemaining()) {
        flushBuffer();
      }
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
