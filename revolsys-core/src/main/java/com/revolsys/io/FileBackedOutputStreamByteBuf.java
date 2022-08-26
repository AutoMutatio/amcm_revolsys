package com.revolsys.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.jeometry.common.exception.Exceptions;

import com.revolsys.reactive.Reactive;
import com.revolsys.reactive.ReactiveByteBuf;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FileBackedOutputStreamByteBuf extends OutputStream implements BaseCloseable {

  public static <T> Mono<T> using(final int bufferSize,
    final Function<FileBackedOutputStreamByteBuf, Mono<T>> action) {
    final Callable<FileBackedOutputStreamByteBuf> supplier = () -> new FileBackedOutputStreamByteBuf(
      bufferSize);
    return Reactive.monoCloseable(supplier, action);
  }

  final ByteBuffer buffer;

  private final byte[] bytes;

  private OutputStream out;

  private boolean closed;

  final int bufferSize;

  private long size = 0;

  Path file;

  private OutputStreamWriter writer;

  public FileBackedOutputStreamByteBuf(final int bufferSize) {
    this.bufferSize = bufferSize;
    this.bytes = new byte[bufferSize];
    this.buffer = ByteBuffer.wrap(this.bytes);
  }

  public Flux<ByteBuf> asByteBufFlux() {
    if (this.file == null) {
      return ReactiveByteBuf.read(this.bytes, 0, (int)this.size);
    } else {
      return ReactiveByteBuf.read(this.file);
    }
  }

  @Override
  public synchronized void close() {
    try {
      if (!this.closed) {
        this.closed = true;
        try {
          if (this.out != null) {
            this.out.close();
          }
        } finally {
          if (this.file != null) {
            Files.deleteIfExists(this.file);
          }
        }
        this.out = null;
      }
    } catch (final IOException e) {
      throw Exceptions.wrap(e);
    }
  }

  public void closeWriter() {
    FileUtil.closeSilent(this.writer);
    this.writer = null;
  }

  public void flip() {
    try {
      closeWriter();
      this.writer = null;
      this.buffer.flip();
      if (this.out != null) {
        this.out.flush();
        this.out.close();
        this.out = null;
      }
    } catch (final IOException e) {
      throw Exceptions.wrap(e);
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    if (!this.closed) {
      if (this.out != null) {
        this.out.flush();
      }
    }
  }

  public long getSize() {
    return this.size;
  }

  public java.io.Writer newWriter() {
    this.writer = new OutputStreamWriter(new IgnoreCloseDelegatingOutputStream(this),
      StandardCharsets.UTF_8);
    return this.writer;
  }

  private void requireFile() throws IOException {
    if (this.file == null) {
      this.file = Files.createTempFile("file", ".bin");
      this.out = new BufferedOutputStream(Files.newOutputStream(this.file));
      this.out.write(this.bytes, 0, this.buffer.position());
    }
  }

  public <T> Mono<T> usingWriter(final Function<? super java.io.Writer, Mono<T>> action) {
    return Mono.using(this::newWriter, action, writer -> closeWriter());
  }

  @Override
  public synchronized void write(final byte[] source, int offset, int length) throws IOException {

    if (this.closed) {
      throw new IOException("Closed");
    } else {
      if (length > 0) {
        final int remaining = this.buffer.remaining();
        if (remaining > 0) {
          final int count = Math.min(remaining, length);
          this.buffer.put(source, offset, count);
          this.size += count;
          length -= count;
          offset += count;
        }
        if (length > 0) {
          requireFile();
          this.out.write(source, offset, length);
          this.size += length;
        }
      }
    }
  }

  @Override
  public synchronized void write(final int b) throws IOException {
    if (this.closed) {
      throw new IOException("Closed");
    } else if (this.file == null) {
      final int remaining = this.buffer.remaining();
      if (remaining > 0) {
        this.buffer.put((byte)b);
      } else {
        requireFile();
        this.out.write(b);

      }
      this.size += 1;
    } else {
      this.out.write(b);
    }
  }

}
