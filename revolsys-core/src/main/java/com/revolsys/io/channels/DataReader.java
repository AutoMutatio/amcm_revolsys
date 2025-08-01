package com.revolsys.io.channels;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.revolsys.exception.Exceptions;
import com.revolsys.util.BaseCloseable;

interface ByteFilter {
  boolean accept(byte b);
}

public interface DataReader extends BaseCloseable {
  public static ByteFilter WHITESPACE = Character::isWhitespace;

  InputStream asInputStream();

  @Override
  void close();

  default int fillBuffer(final ByteBuffer buffer) {
    try {
      buffer.clear();
      final int size = buffer.remaining();
      int totalReadCount = 0;
      while (totalReadCount < size) {
        final int readCount = read(buffer);
        if (readCount == -1) {
          if (totalReadCount == 0) {
            return -1;
          } else {
            final int bufferPosition = buffer.position();
            buffer.flip();
            return bufferPosition;
          }
        } else {
          totalReadCount += readCount;
        }
      }
      final int bufferPosition = buffer.position();
      buffer.flip();
      return bufferPosition;
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  int getAvailable();

  byte getByte();

  ByteOrder getByteOrder();

  default int getBytes(final byte[] bytes) {
    return getBytes(bytes, 0, bytes.length);
  }

  int getBytes(byte[] bytes, int offset, int byteCount);

  byte[] getBytes(int byteCount);

  byte[] getBytes(long offset, int byteCount);

  double getDouble();

  float getFloat();

  InputStream getInputStream(long offset, int size);

  int getInt();

  long getLong();

  short getShort();

  default String getString(final int byteCount, final Charset charset) {
    final byte[] bytes = getBytes(byteCount);
    int i = 0;
    for (; i < bytes.length; i++) {
      final byte character = bytes[i];
      if (character == 0) {
        return new String(bytes, 0, i, charset);
      }
    }
    return new String(bytes, 0, i, charset);
  }

  default String getStringUtf8ByteCount() {
    final int byteCount = getInt();
    if (byteCount < 0) {
      return null;
    } else if (byteCount == 0) {
      return "";
    } else {
      return getString(byteCount, StandardCharsets.UTF_8);
    }
  }

  default short getUnsignedByte() {
    final byte signedByte = getByte();
    return (short)Byte.toUnsignedInt(signedByte);
  }

  default long getUnsignedInt() {
    final int signedInt = getInt();
    return Integer.toUnsignedLong(signedInt);
  }

  /**
   * Unsigned longs don't actually work channel Java
   *
   * @return
   */
  default long getUnsignedLong() {
    final long signedLong = getLong();
    return signedLong;
  }

  default int getUnsignedShort() {
    final short signedShort = getShort();
    return Short.toUnsignedInt(signedShort);
  }

  default String getUsAsciiString(final int byteCount) {
    return getString(byteCount, StandardCharsets.US_ASCII);
  }

  InputStream getWrapStream();

  boolean isByte(byte expected);

  boolean isByte(char expected);

  default boolean isBytes(final byte[] bytes) {
    for (final byte e : bytes) {
      final int a = read();
      if (a != e) {
        return false;
      }
    }
    return true;
  }

  boolean isClosed();

  default boolean isEof() {
    try {
      final int b = read();
      if (b < 0) {
        return true;
      } else {
        unreadByte((byte)b);
        return false;
      }
    } catch (final RuntimeException e) {
      if (Exceptions.hasCause(e, ClosedChannelException.class)) {
        return true;
      } else {
        throw e;
      }
    }
  }

  default boolean isEol() {
    final int b = read();
    switch (b) {
      case -1:
        return false;
      case '\r': {
        final int b2 = read();
        if (b2 != '\n') {
          unreadByte(b2);
        }
        return true;
      }
      case '\n': {
        return true;
      }
      default:
        unreadByte(b);
        return false;
    }
  }

  boolean isSeekable();

  long position();

  int read();

  int read(byte[] bytes, int offset, int length) throws IOException;

  int read(ByteBuffer buffer);

  void seek(long position);

  void seekEnd(long distance);

  DataReader setByteOrder(ByteOrder byteOrder);

  DataReader setUnreadSize(int unreadSize);

  void skipBytes(int count);

  default void skipComment() {
    skipWhile(b -> b != '\n' && b != '\r');
  }

  default void skipEol() {
    while (true) {
      final int b = read();
      switch (b) {
        case -1:
        break;
        case '\r': {
          final int b2 = getByte();
          if (b2 != '\n') {
            unreadByte(b2);
            return;
          }
          break;
        }
        case '\n': {
          break;
        }
        default:
          unreadByte(b);
          return;
      }
    }
  }

  default boolean skipIfChar(final char c) {
    final int b = read();
    if (c == b) {
      return true;
    } else {
      unreadByte(b);
      return false;
    }
  }

  default void skipOneEol() {
    final int b = read();
    switch (b) {
      case -1:
      break;
      case '\r': {
        final int b2 = read();
        if (b2 != '\n') {
          unreadByte(b2);
        }
        break;
      }
      case '\n': {
        break;
      }
      default: {
        unreadByte(b);
        break;
      }
    }
  }

  default void skipWhile(final ByteFilter c) {
    int b;
    do {
      b = read();
      if (b == -1) {
        return;
      }
    } while (c.accept((byte)b));
    unreadByte(b);
  }

  default boolean skipWhitespace() {
    int count = 0;
    int b;
    do {
      count++;
      b = read();
      if (b == -1) {
        return count > 0;
      }
      // TODO comments
      // if (b == '%') {
      // skipComment();
      // b = getByte();
      // }
    } while (WHITESPACE.accept((byte)b));
    if (b != -1) {
      count--;
      unreadByte(b);
    }
    return count > 0;
  }

  String toFullString();

  void unreadByte(byte b);

  default void unreadByte(final int b) {
    if (b != -1) {
      unreadByte((byte)b);
    }
  }

}
