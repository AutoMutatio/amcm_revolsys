package com.revolsys.reactive.chars;

import java.security.MessageDigest;
import java.util.function.Supplier;

import org.jeometry.coordinatesystem.util.MessageDigestProxy;

import io.netty.buffer.ByteBuf;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

public class DigestByteBufFlux extends FluxOperator<ByteBuf, ByteBuf>
  implements MessageDigestProxy {

  private static final int BUF_SIZE = 8912;

  public static DigestByteBufFlux create(final Flux<ByteBuf> source,
    final Supplier<MessageDigest> digest) {
    return new DigestByteBufFlux(source, digest.get());
  }

  private final MessageDigest digest;

  private final byte[] buf = new byte[BUF_SIZE];

  public DigestByteBufFlux(final Flux<ByteBuf> source, final MessageDigest digest) {
    super(source);
    this.digest = digest;
  }

  @Override
  public MessageDigest getMessageDigest() {
    return this.digest;
  }

  @Override
  public void subscribe(final CoreSubscriber<? super ByteBuf> subscriber) {
    this.source.doOnNext(bytes -> {
      final int size = bytes.readableBytes();
      for (int start = 0; start < size; start += BUF_SIZE) {
        final int len = Math.min(BUF_SIZE, size - start);
        bytes.getBytes(start, this.buf, 0, len);
        this.digest.update(this.buf, 0, len);
      }
    }).subscribe(subscriber);
  }
}
