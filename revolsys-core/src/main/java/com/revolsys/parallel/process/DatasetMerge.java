package com.revolsys.parallel.process;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import com.revolsys.exception.Exceptions;
import com.revolsys.parallel.channel.Channel;
import com.revolsys.parallel.channel.store.Buffer;
import com.revolsys.util.concurrent.Concurrent;
import com.revolsys.util.count.CountTree;

public final class DatasetMerge<R, K> implements Cloneable {

  public static class Builder<R2, K2> {
    private final DatasetMerge<R2, K2> merger = new DatasetMerge<>();

    public Builder() {
      // Default NoOp handlers
      deleteRecordHandler(r -> {
      });
      insertRecordHandler(r -> {
      });
      updateRecordHandler(r -> {
      });
    }

    public DatasetMerge<R2, K2> build() {
      Objects.requireNonNull(this.merger.comparator, "comparator");
      Objects.requireNonNull(this.merger.deleteHandler, "deleteHandler");
      Objects.requireNonNull(this.merger.insertHandler, "insertHandler");
      Objects.requireNonNull(this.merger.updateHandler, "updateHandler");
      Objects.requireNonNull(this.merger.recordToId, "recordToId");
      Objects.requireNonNull(this.merger.sourceRecords, "sourceRecords");
      Objects.requireNonNull(this.merger.targetRecords, "targetRecords");
      return this.merger.clone();
    }

    public Builder<R2, K2> comparator(final Comparator<K2> comparator) {
      this.merger.comparator = Objects.requireNonNull(comparator, "comparator");
      return this;
    }

    public Builder<R2, K2> deleteHandler(final Consumer<Channel<R2>> deleteHandler) {
      this.merger.deleteHandler = Objects.requireNonNull(deleteHandler, "deleteHandler");
      return this;
    }

    public Builder<R2, K2> deleteRecordHandler(final Consumer<R2> deleteRecordHandler) {
      Objects.requireNonNull(deleteRecordHandler, "deleteRecordHandler");
      return deleteHandler(channel -> channel.forEach(deleteRecordHandler));
    }

    public Builder<R2, K2> insertHandler(final Consumer<Channel<R2>> insertHandler) {
      this.merger.insertHandler = Objects.requireNonNull(insertHandler, "insertHandler");
      return this;
    }

    public Builder<R2, K2> insertRecordHandler(final Consumer<R2> insertRecordHandler) {
      Objects.requireNonNull(insertRecordHandler, "insertRecordHandler");
      return insertHandler(channel -> channel.forEach(insertRecordHandler));
    }

    public Builder<R2, K2> recordToId(final Function<R2, K2> recordToId) {
      this.merger.recordToId = Objects.requireNonNull(recordToId, "recordToId");
      return this;
    }

    public Builder<R2, K2> sourceRecords(final Iterable<R2> sourceRecords) {
      this.merger.sourceRecords = Objects.requireNonNull(sourceRecords, "sourceRecords");
      return this;
    }

    public Builder<R2, K2> targetRecords(final Iterable<R2> targetRecords) {
      this.merger.targetRecords = Objects.requireNonNull(targetRecords, "targetRecords");
      return this;
    }

    public Builder<R2, K2> updateHandler(
      final Consumer<Channel<SourceTargetRecord<R2>>> updateHandler) {
      this.merger.updateHandler = Objects.requireNonNull(updateHandler, "updateHandler");
      return this;
    }

    public Builder<R2, K2> updateRecordHandler(
      final Consumer<SourceTargetRecord<R2>> updateRecordHandler) {
      Objects.requireNonNull(updateRecordHandler, "updateRecordHandler");
      return updateHandler(channel -> channel.forEach(updateRecordHandler));
    }
  }

  public record SourceTargetRecord<R2>(R2 source, R2 target) {
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

  // private Consumer<ChannelInput<R>>
  private final CountTree counts = new CountTree();

  private Consumer<Channel<R>> insertHandler;

  private Consumer<Channel<R>> deleteHandler;

  private Consumer<Channel<SourceTargetRecord<R>>> updateHandler;

  private final Channel<R> insertChannel = new Channel<R>(new Buffer<>());

  private final Channel<R> deleteChannel = new Channel<R>(new Buffer<>());

  private final Channel<SourceTargetRecord<R>> updateChannel = new Channel<SourceTargetRecord<R>>(
    new Buffer<>());

  @SuppressWarnings("unchecked")
  @Override
  protected DatasetMerge<R, K> clone() {
    try {
      return (DatasetMerge<R, K>)super.clone();
    } catch (final CloneNotSupportedException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  private Void processDelete() {
    try (
      var c = this.deleteChannel.readConnect()) {
      this.deleteHandler.accept(this.deleteChannel);
    }
    return null;
  }

  private Void processInsert() {
    try (
      var c = this.insertChannel.readConnect()) {
      this.insertHandler.accept(this.insertChannel);
    }
    return null;
  }

  private Void processRecords() {
    try (
      var ic = this.insertChannel.writeConnect();
      var uc = this.updateChannel.writeConnect();
      var dc = this.deleteChannel.writeConnect();) {
      final var sourceIterator = this.sourceRecords.iterator();
      final var targetIterator = this.targetRecords.iterator();
      var sourceRef = recordNext(sourceIterator, "source");
      var targetRef = recordNext(targetIterator, "target");
      while (sourceRef.hasRecord() || targetRef.hasRecord()) {
        final int idCompare = targetRef.compareTo(this.comparator, sourceRef);
        if (idCompare > 0) {
          this.counts.addCount("insert");
          final var sourceRecord = sourceRef.record();
          this.insertChannel.write(sourceRecord);
          sourceRef = recordNext(sourceIterator, "source");
        } else if (idCompare < 0) {
          this.counts.addCount("delete");
          final var targetRecord = targetRef.record();
          this.deleteChannel.write(targetRecord);
          targetRef = recordNext(targetIterator, "target");
        } else {
          final var sourceRecord = sourceRef.record();
          final var targetRecord = targetRef.record();
          this.updateChannel.write(new SourceTargetRecord<R>(sourceRecord, targetRecord));
          sourceRef = recordNext(sourceIterator, "source");
          targetRef = recordNext(targetIterator, "target");
        }
      }
    }
    return null;
  }

  private Void processUpdate() {
    try (
      var c = this.updateChannel.readConnect()) {
      this.updateHandler.accept(this.updateChannel);
    }
    return null;
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
    Concurrent.virtual()
      .scope(scope -> {
        scope.fork(this::processRecords);
        scope.fork(this::processDelete);
        scope.fork(this::processInsert);
        scope.fork(this::processUpdate);
      });

    return this.counts;
  }
}
