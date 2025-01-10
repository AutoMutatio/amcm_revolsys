package com.revolsys.parallel.process;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.revolsys.exception.Exceptions;
import com.revolsys.parallel.channel.Channel;
import com.revolsys.parallel.channel.store.Buffer;
import com.revolsys.util.concurrent.Concurrent;
import com.revolsys.util.count.CountTree;

/**
The DatasetMerge process is designed to detect insert, update, and deleted records for two
data sets sorted by an identifier.

<b>NOTE: This will cause unexpected inserts/deletes if the data is not sorted.</b>

<h2>Simple per record callback example</h2>
<pre>
var counts = // Counts of source, target, delete, insert, update records
  DatasetMerge.<JsonObject, Integer> builder()
  .sourceRecords(sourceRecords) // Iterable of sorted sourceRecords
  .targetRecords(targetRecords) // Iterable of sorted sourceRecords
  .recordToId(record -> record.getInteger("id")) // Lambda function to get the id from a record
  .comparator(Integer::compare) // Lambda function to compare two identifiers
  .deleteRecordHandler(deletedRecord -> {
    // Do something with the target record that wasn't in the source
  })
  .insertRecordHandler(actual -> {
    // Do something with the source record that wasn't in the target
  })
  .updateRecordHandler((source, target) -> {
    // Do something to update the target record with the source
    // Best practice is to compare the source and target before doing the database update
  })
  .build() // Create the merger
  .run(); // Process all the source and return the counts
</pre>

<h2>Batching updates in a transaction</h2>
<p>Often it is undesirable from a performance perspective to have either 1 transaction per record
or 1 large transactions for all records. The (delete/insert/update)Handler methods can be used
to get the channel that the delete, insert, update records are sent to. This channel can then be
read in custom loops, for example to batch records into transactions. The following shows an
example of this.</p>

<pre>
var batchSize = 10;
var counts = // Counts of source, target, delete, insert, update records
  DatasetMerge.<JsonObject, Integer> builder()
  .sourceRecords(sourceRecords) // Iterable of sorted sourceRecords
  .targetRecords(targetRecords) // Iterable of sorted sourceRecords
  .recordToId(record -> record.getInteger("id")) // Lambda function to get the id from a record
  .comparator(Integer::compare) // Lambda function to compare two identifiers
  .deleteHandler(deletedChannel -> {
    while (deletedChannel.isOpen()) {
      try (var transaction = ...) {
        try {
          for (int i = 0; i < batchSize; i++) {
            var deletedRecord = deletedChannel.read();
            // Do something with the target record that wasn't in the source
          }
        } catch (ClosedException e) {
          // Ignore exception as it's expected
        }
      }
    }
  })
  .insertHandler(insertedChannel -> {
    while (insertedChannel.isOpen()) {
      try (var transaction = ...) {
        try {
          for (int i = 0; i < batchSize; i++) {
            var insertedRecord = insertedChannel.read();
            // Do something with the source record that wasn't in the target
          }
        } catch (ClosedException e) {
          // Ignore exception as it's expected
        }
      }
    }
  })
    while (updatedChannel.isOpen()) {
      try (var transaction = ...) {
        try {
          for (int i = 0; i < batchSize; i++) {
            var updatedRecords = updatedChannel.read();
            var source = updatedRecords.source();
            var target = updatedRecords.target();
            // Do something to update the target record with the source
            // Best practice is to compare the source and target before doing the database update
          }
        } catch (ClosedException e) {
          // Ignore exception as it's expected
        }
      }
    }
  })
  .build() // Create the merger
  .run(); // Process all the source and return the counts
</pre>
 * @param <R>
 * @param <K>
 */
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

    public Builder<R2, K2> updateRecordHandler(final BiConsumer<R2, R2> updateRecordHandler) {
      Objects.requireNonNull(updateRecordHandler, "updateRecordHandler");
      return updateHandler(channel -> channel.forEach(records -> {
        final var source = records.source();
        final var target = records.target();
        updateRecordHandler.accept(source, target);
      }));
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
    try {
      this.deleteHandler.accept(this.deleteChannel);
    } finally {
      this.deleteChannel.readDisconnect();
    }
    return null;
  }

  private Void processInsert() {
    try {
      this.insertHandler.accept(this.insertChannel);
    } finally {
      this.insertChannel.readDisconnect();
    }
    return null;
  }

  private Void processRecords() {
    try {
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
          this.counts.addCount("update");
          final var sourceRecord = sourceRef.record();
          final var targetRecord = targetRef.record();
          this.updateChannel.write(new SourceTargetRecord<R>(sourceRecord, targetRecord));
          sourceRef = recordNext(sourceIterator, "source");
          targetRef = recordNext(targetIterator, "target");
        }
      }
    } finally {
      this.insertChannel.writeDisconnect();
      this.updateChannel.writeDisconnect();
      this.deleteChannel.writeDisconnect();
    }
    return null;
  }

  private Void processUpdate() {
    try {
      this.updateHandler.accept(this.updateChannel);
    } finally {
      this.updateChannel.readDisconnect();
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
    for (final var channel : Arrays.asList(this.insertChannel, this.updateChannel,
      this.deleteChannel)) {
      channel.writeConnect();
      channel.readConnect();
    }

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
