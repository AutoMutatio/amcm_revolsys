package com.revolsys.reactive.chars;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import com.revolsys.io.FileUtil;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.ByteBufFlux;
import reactor.util.context.Context;

public class ByteBufs implements ValueProcessor<ByteBuf> {
  public static Flux<CharBuffer> asCharBuffer(final Publisher<ByteBuf> publisher,
    final Charset charset, final int bufferSize) {
    final Consumer<? super FluxSink<CharBuffer>> handler = sink -> new ByteBufToCharBufferHandler(
      publisher, sink, charset, bufferSize);
    return Flux.create(handler);
  }

  public static BiConsumer<? super ByteBuf, SynchronousSink<CharBuffer>> fluxHundler(
    final Charset charset, final int bufferSize) {
    final byte[] bytes = new byte[bufferSize];
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final CharBuffer chars = CharBuffer.allocate(bufferSize);
    final CharsetDecoder decoder = charset.newDecoder();
    return (byteBuf, sink) -> {
      final int count = byteBuf.readableBytes();
      byteBuf.readBytes(bytes, 0, count);
      byteBuffer.rewind();
      byteBuffer.limit(count);
      decoder.decode(byteBuffer, chars, false);
      chars.flip();
      sink.next(chars);
    };
  }

  public static ByteBufFlux toByteBufFlux(final String value) {
    return ByteBufFlux.fromString(Mono.just(value));
  }

  public static Flux<ByteBuf> write(ByteBufFlux source, OutputStream out) {
    return Flux.create(sink -> {
      OutputStreamSubscriber subscriber = new OutputStreamSubscriber(sink, out);
      sink.onDispose(subscriber);
      source.subscribe(subscriber);
    });
  }

  private static class WritableByteChannelSubscriber extends BaseSubscriber<ByteBuf> {

    private final FluxSink<ByteBuf> sink;

    private final WritableByteChannel channel;

    public WritableByteChannelSubscriber(FluxSink<ByteBuf> sink, WritableByteChannel channel) {
      this.sink = sink;
      this.channel = channel;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      request(1);
    }

    @Override
    protected void hookOnNext(ByteBuf dataBuffer) {
      try {
        ByteBuffer nioBuffer = dataBuffer.nioBuffer();
        while (nioBuffer.hasRemaining()) {
          this.channel.write(nioBuffer);
        }
        this.sink.next(dataBuffer);
        request(1);
      } catch (IOException ex) {
        this.sink.next(dataBuffer);
        this.sink.error(ex);
      }
    }

    @Override
    protected void hookOnError(Throwable throwable) {
      this.sink.error(throwable);
    }

    @Override
    protected void hookOnComplete() {
      this.sink.complete();
    }

    @Override
    public Context currentContext() {
      return Context.of(this.sink.contextView());
    }

  }

  public static Flux<ByteBuf> write(ByteBufFlux source, WritableByteChannel channel) {
    return Flux.create(sink -> {
      WritableByteChannelSubscriber subscriber = new WritableByteChannelSubscriber(sink, channel);
      sink.onDispose(subscriber);
      source.subscribe(subscriber);
    });
  }

  private static class OutputStreamSubscriber extends BaseSubscriber<ByteBuf> {

    private final FluxSink<ByteBuf> sink;

    private final OutputStream out;

    private byte[] buffer = new byte[8192];

    public OutputStreamSubscriber(FluxSink<ByteBuf> sink, OutputStream out) {
      this.sink = sink;
      this.out = out;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      request(1);
    }

    @Override
    protected void hookOnNext(ByteBuf dataBuffer) {
      try {
        while (dataBuffer.isReadable()) {
          int readCount = Math.min(buffer.length, dataBuffer.readableBytes());
          dataBuffer.readBytes(buffer, 0, readCount);
          this.out.write(buffer, 0, readCount);
        }
        this.sink.next(dataBuffer);
        request(1);
      } catch (IOException ex) {
        this.sink.error(ex);
      }
    }

    @Override
    protected void hookOnError(Throwable throwable) {
      this.sink.error(throwable);
    }

    @Override
    protected void hookOnComplete() {
      this.sink.complete();
    }

    @Override
    public Context currentContext() {
      return Context.of(this.sink.contextView());
    }

  }

  private final int bufferSize = 8192;

  private final CharacterProcessor processor;

  private final CharBuffer chars = CharBuffer.allocate(this.bufferSize + 4);

  private final byte[] bytes = new byte[this.bufferSize];

  private final ByteBuffer byteBuffer = ByteBuffer.wrap(this.bytes);

  private final CharsetDecoder decoder;

  public ByteBufs(final Charset charset, final CharacterProcessor processor) {
    this.processor = processor;
    this.decoder = charset.newDecoder();
  }

  @Override
  public void onCancel() {
    this.processor.onCancel();
  }

  @Override
  public void onComplete() {
    this.processor.onComplete();
  }

  @Override
  public void onError(final Throwable e) {
    this.processor.onError(e);
  }

  @Override
  public boolean process(final ByteBuf buffer) {
    while (buffer.isReadable()) {
      final int count = Math.min(buffer.readableBytes(), this.bufferSize);
      buffer.readBytes(this.bytes, 0, count);
      this.byteBuffer.clear();
      this.byteBuffer.limit(count);
      this.chars.clear();
      this.decoder.decode(this.byteBuffer, this.chars, false);
      this.chars.flip();
      this.processor.process(this.chars);
    }
    return true;
  }

  public static Mono<InputStream> inputStream$(ByteBufFlux bytes) {
    return Mono.defer(() -> {
      try {
        InputStream in = toInputStream(bytes);
        return Mono.just(in);
      } catch (IOException e) {
        return Mono.error(e);
      }
    });
  }

  public static InputStream toInputStream(ByteBufFlux bytes) throws IOException {
    PipedOutputStream out = new PipedOutputStream();
    PipedInputStream in = new PipedInputStream(out, 8192);
    write(bytes, out).doOnError(t -> {
      FileUtil.closeSilent(in);
      FileUtil.closeSilent(out);
    }).doOnTerminate(() -> {
      Flux.just(true).doOnNext((v) -> {
        try {
          if (in.available() == 0) {
            FileUtil.closeSilent(out);
          }
        } catch (IOException e) {
        }
      }).repeat(() -> {
        try {
          return in.available() > 0;
        } catch (IOException e) {
          return true;
        }
      }).subscribe();
    }).subscribe();

    return in;
  }
}
