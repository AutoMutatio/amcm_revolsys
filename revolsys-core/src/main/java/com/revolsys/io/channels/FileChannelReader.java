package com.revolsys.io.channels;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import com.revolsys.exception.Exceptions;
import com.revolsys.io.EndOfFileException;
import com.revolsys.io.SeekableByteChannelInputStream;
import com.revolsys.util.BaseCloseable;

public class FileChannelReader extends AbstractDataReader implements BaseCloseable {

  private final FileChannel channel;

  private final boolean close;

  private long position;

  private long limit = Long.MAX_VALUE;

  public FileChannelReader(final FileChannel channel, final ByteBuffer buffer,
    final boolean close) {
    super(buffer, true);
    this.channel = channel;
    this.close = close;
  }

  @Override
  public void close() {
    if (this.close && !isClosed()) {
      try {
        this.channel.close();
      } catch (final IOException e) {
        throw Exceptions.wrap(e);
      } finally {
        super.close();
      }
    }
  }

  @Override
  public byte[] getBytes(final long offset, final int byteCount) {
    final byte[] bytes = new byte[byteCount];
    try {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      final int count = this.channel.read(buffer, offset);
      if (count == -1) {
        throw new EndOfFileException();
      } else if (count == byteCount) {
        return bytes;
      } else {
        return Arrays.copyOf(bytes, count);
      }
    } catch (final IOException e) {
      throw Exceptions.wrap(e);
    }
  }

  public FileChannel getChannel() {
    return this.channel;
  }

  @Override
  public InputStream getInputStream(final long offset, final int size) {
    return new SeekableByteChannelInputStream(this.channel, offset, size);
  }

  @Override
  public long position() {
    return this.position;
  }

  public ByteBuffer readByteByteBuffer(long offset, final int size) {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(size);
    do {
      try {
        final int count = this.channel.read(buffer, offset);
        if (count == -1) {
          if (buffer.limit() == 0) {
            throw new EndOfFileException();
          }
          break;
        } else {
          offset += count;
        }
      } catch (final IOException e) {
        throw Exceptions.wrap(e);
      }
    } while (buffer.hasRemaining());
    buffer.flip();
    return buffer;
  }

  @Override
  public int readInternal(final ByteBuffer buffer) throws IOException {
    final long remaining = this.limit - this.position;
    if (remaining <= 0) {
      return -1;
    } else if (buffer.limit() > remaining) {
      buffer.limit((int)remaining);
    }
    final int count = this.channel.read(buffer, this.position);
    if (count == -1) {
      return count;
    } else {
      this.position += count;
      return count;
    }
  }

  @Override
  public void seek(final long position) {
    this.position = position;
    afterSeek();
  }

  @Override
  public void seekEnd(final long distance) {
    try {
      this.position = this.channel.size() - distance;
    } catch (final IOException e) {
      throw Exceptions.wrap(e);
    }
  }

  public void setLimit(final long limit) {
    this.limit = limit;
  }
}
