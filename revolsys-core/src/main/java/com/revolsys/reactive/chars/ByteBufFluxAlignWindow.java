package com.revolsys.reactive.chars;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

import io.netty.buffer.ByteBuf;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

class ByteBufFluxAlignWindow extends FluxOperator<ByteBuf, ByteBuf> {

  private final long windowSize;

  private long size;

  ByteBufFluxAlignWindow(final Flux<? extends ByteBuf> source, final long windowSize) {
    super(source);
    this.windowSize = windowSize;
  }

  @Override
  public void subscribe(final CoreSubscriber<? super ByteBuf> subscription) {
    this.source.concatMap(buf -> {
      final int readableBytes = buf.readableBytes();
      if (readableBytes > this.windowSize - this.size) {
        final ListEx<ByteBuf> buffers = Lists.newArray();
        for (int offset = 0; offset < readableBytes;) {
          int chunkSize;
          if (offset == 0) {
            chunkSize = (int)(this.windowSize - this.size);
          } else {
            chunkSize = (int)Math.min(this.windowSize, readableBytes - offset);
            if (chunkSize < this.windowSize) {
              this.size = chunkSize;
            } else {
              this.size = 0;
            }
          }
          final ByteBuf chunk = buf.retainedSlice(offset, chunkSize);
          buffers.add(chunk);
          offset += chunkSize;
        }
        return Flux.fromIterable(buffers);
      } else {
        this.size += readableBytes;
        if (this.size == this.windowSize) {
          this.size = 0;
        }
        return Flux.just(buf);
      }
    }).subscribe(subscription);
  }
}
