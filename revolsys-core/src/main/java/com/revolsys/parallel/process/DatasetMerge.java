package com.revolsys.parallel.process;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.revolsys.exception.Exceptions;
import com.revolsys.util.count.CountTree;

public class DatasetMerge<R, K> implements Cloneable {
  public static class Builder<R2, K2> {
    private final DatasetMerge<R2, K2> merger = new DatasetMerge<>();

    public DatasetMerge<R2, K2> build() {
      return this.merger.clone();
    }

    public Builder<R2, K2> comparator(final Comparator<K2> comparator) {
      this.merger.comparator = comparator;
      return this;
    }

    public Builder<R2, K2> recordToId(final Function<R2, K2> recordToId) {
      this.merger.recordToId = recordToId;
      return this;
    }

    public Builder<R2, K2> sourceRecords(final Iterable<R2> sourceRecords) {
      this.merger.sourceRecords = sourceRecords;
      return this;
    }

    public Builder<R2, K2> targetRecords(final Iterable<R2> targetRecords) {
      this.merger.targetRecords = targetRecords;
      return this;
    }
  }

  private record RecordWithId<R2, K2>(R2 record, K2 id) {

    public int compareTo(final Comparator<K2> comparator, final RecordWithId<R2, K2> other) {
      if (other.hasRecord()) {
        if (hasRecord()) {
          return comparator.compare(this.id, other.id);
        } else {
          return 1;
        }
      } else if (hasRecord()) {
        return -1;
      } else {
        return 0;
      }
    }

    public boolean hasRecord() {
      return this.record != null;
    }
  }

  public static <R2, K2> Builder<R2, K2> builder() {
    return new Builder<>();
  }

  private final RecordWithId<R, K> emptyRecord = new RecordWithId<R, K>(null, null);

  private Comparator<K> comparator;

  private Function<R, K> recordToId;

  private Iterable<R> sourceRecords;

  private Iterable<R> targetRecords;

  private final CountTree counts = new CountTree();

  @SuppressWarnings("unchecked")
  @Override
  protected DatasetMerge<R, K> clone() {
    try {
      return (DatasetMerge<R, K>)super.clone();
    } catch (final CloneNotSupportedException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  private boolean hasMore(final AtomicReference<RecordWithId<R, K>> sourceRef,
    final AtomicReference<RecordWithId<R, K>> targetRef) {
    final var sourceHas = sourceRef.get()
      .hasRecord();
    final var targetHas = targetRef.get()
      .hasRecord();
    return sourceHas || targetHas;
  }

  private RecordWithId<R, K> recordNext(final Iterator<R> iterator, final String countLabel) {
    if (iterator.hasNext()) {
      final var record = iterator.next();
      final var id = this.recordToId.apply(record);
      this.counts.addCount(countLabel);
      return new RecordWithId<R, K>(record, id);
    } else {
      return this.emptyRecord;
    }
  }

  public CountTree run() {
    final var sourceIterator = this.sourceRecords.iterator();
    final var targetIterator = this.targetRecords.iterator();
    final var sourceRef = recordNext(sourceIterator, "source");
    final var targetRef = recordNext(targetIterator, "target");
    while (sourceRef.hasRecord() || targetRef.hasRecord()) {
    }
    return null;
  }

}
