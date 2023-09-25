package com.revolsys.reactive.chars;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

public class ByteBuffers {
  public static Flux<CharBuffer> asCharBuffer(Publisher<ByteBuffer> publisher, Charset charset,
    int bufferSize) {
    final Consumer<? super FluxSink<CharBuffer>> handler = sink -> new ByteBufferToCharBufferHandler(
      publisher, sink, charset, bufferSize);
    return Flux.create(handler);
  }

}
