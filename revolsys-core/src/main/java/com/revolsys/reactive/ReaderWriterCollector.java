package com.revolsys.reactive;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.jeometry.common.exception.Exceptions;

public class ReaderWriterCollector<S, I, T> implements Collector<S, I, T> {
  public static interface ReaderWriter<S, I> {
    public int readWrite(S source, I channel, long position) throws IOException;
  }

  private long position = 0L;

  private final ReaderWriter<S, I> readerWriter;

  private final Supplier<I> supplier;

  private final Function<I, T> closer;

  public ReaderWriterCollector(final ReaderWriter<S, I> readerWriter, final Supplier<I> supplier,
    final Function<I, T> closer) {
    this.readerWriter = readerWriter;
    this.supplier = supplier;
    this.closer = closer;
  }

  @Override
  public BiConsumer<I, S> accumulator() {
    return (channel, source) -> {
      try {
        final int written = this.readerWriter.readWrite(source, channel, this.position);
        this.position += written;
      } catch (final IOException e) {
        throw Exceptions.wrap(e);
      }
    };
  }

  @Override
  public Set<Characteristics> characteristics() {
    return Collections.emptySet();
  }

  @Override
  public BinaryOperator<I> combiner() {
    return null;
  }

  @Override
  public Function<I, T> finisher() {
    return this.closer;
  }

  @Override
  public Supplier<I> supplier() {
    return this.supplier;
  }
}
