package com.revolsys.reactive.chars;

public interface ValueProcessor<V> extends Processor {
  boolean process(V value);

}
