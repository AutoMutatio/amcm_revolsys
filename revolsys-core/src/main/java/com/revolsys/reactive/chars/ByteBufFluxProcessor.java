package com.revolsys.reactive.chars;

import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;

public interface ByteBufFluxProcessor {
  <V> Mono<V> process(ByteBufFlux bytes);
}
