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
import com.revolsys.util.Debug;
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
  .sourceRecordToId(record -> record.getInteger("id")) // Lambda function to get the id from a source record
  .targetRecordToId(record -> record.getInteger("id")) // Lambda function to get the id from a target record
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
  .sourceRecordToId(record -> record.getInteger("id")) // Lambda function to get the id from a source record
  .targetRecordToId(record -> record.getInteger("id")) // Lambda function to get the id from a target record
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
  .run(); // Process all the source and return the counts
</pre>
 * @param <SR>
 * @param <K>
 */
public final class DatasetMerge<SR, TR, K> implements Cloneable {

  public static class Builder<SR2, TR2, K2> {
    private final DatasetMerge<SR2, TR2, K2> merger = new DatasetMerge<>();

    public Builder() {
      // Default NoOp handlers
      deleteRecordHandler(r -> {
      });
      insertRecordHandler(r -> {
      });
      updateRecordHandler(r -> {
      });
    }

    public Builder<SR2, TR2, K2> comparator(final Comparator<K2> comparator) {
      this.merger.comparator = Objects.requireNonNull(comparator, "comparator");
      return this;
    }

    public Builder<SR2, TR2, K2> deleteHandler(final Consumer<Channel<TR2>> deleteHandler) {
      this.merger.deleteHandler = Objects.requireNonNull(deleteHandler, "deleteHandler");
      return this;
    }

    public Builder<SR2, TR2, K2> deleteRecordHandler(final Consumer<TR2> deleteRecordHandler) {
      Objects.requireNonNull(deleteRecordHandler, "deleteRecordHandler");
      return deleteHandler(channel -> channel.forEach(deleteRecordHandler));
    }

    public Builder<SR2, TR2, K2> insertHandler(final Consumer<Channel<SR2>> insertHandler) {
      this.merger.insertHandler = Objects.requireNonNull(insertHandler, "insertHandler");
      return this;
    }

    public Builder<SR2, TR2, K2> insertRecordHandler(final Consumer<SR2> insertRecordHandler) {
      Objects.requireNonNull(insertRecordHandler, "insertRecordHandler");
      return insertHandler(channel -> channel.forEach(insertRecordHandler));
    }

    public CountTree run() {
      Objects.requireNonNull(this.merger.comparator, "comparator");
      Objects.requireNonNull(this.merger.deleteHandler, "deleteHandler");
      Objects.requireNonNull(this.merger.insertHandler, "insertHandler");
      Objects.requireNonNull(this.merger.updateHandler, "updateHandler");
      Objects.requireNonNull(this.merger.sourceRecordToId, "sourceRecordToId");
      Objects.requireNonNull(this.merger.targetRecordToId, "targetRecordToId");
      Objects.requireNonNull(this.merger.sourceRecords, "sourceRecords");
      Objects.requireNonNull(this.merger.targetRecords, "targetRecords");
      return this.merger.clone().run();
    }

    public Builder<SR2, TR2, K2> sourceRecords(final Iterable<? extends SR2> sourceRecords) {
      this.merger.sourceRecords = Objects.requireNonNull(sourceRecords, "sourceRecords");
      return this;
    }

    public Builder<SR2, TR2, K2> sourceRecordToId(final Function<SR2, K2> sourceRecordToId) {
      this.merger.sourceRecordToId = Objects.requireNonNull(sourceRecordToId, "sourceRecordToId");
      return this;
    }

    public Builder<SR2, TR2, K2> targetRecords(final Iterable<? extends TR2> targetRecords) {
      this.merger.targetRecords = Objects.requireNonNull(targetRecords, "targetRecords");
      return this;
    }

    public Builder<SR2, TR2, K2> targetRecordToId(final Function<TR2, K2> targetRecordToId) {
      this.merger.targetRecordToId = Objects.requireNonNull(targetRecordToId, "targetRecordToId");
      return this;
    }

    public Builder<SR2, TR2, K2> updateHandler(
      final Consumer<Channel<SourceTargetRecord<SR2, TR2>>> updateHandler) {
      this.merger.updateHandler = Objects.requireNonNull(updateHandler, "updateHandler");
      return this;
    }

    public Builder<SR2, TR2, K2> updateRecordHandler(
      final BiConsumer<SR2, TR2> updateRecordHandler) {
      Objects.requireNonNull(updateRecordHandler, "updateRecordHandler");
      return updateHandler(channel -> channel.forEach(records -> {
        final var source = records.source();
        final var target = records.target();
        updateRecordHandler.accept(source, target);
      }));
    }

    public Builder<SR2, TR2, K2> updateRecordHandler(
      final Consumer<SourceTargetRecord<SR2, TR2>> updateRecordHandler) {
      Objects.requireNonNull(updateRecordHandler, "updateRecordHandler");
      return updateHandler(channel -> channel.forEach(updateRecordHandler));
    }
  }

  public static <SR2, TR2, K2> Builder<SR2, TR2, K2> builder() {
    return new Builder<>();
  }

  private final boolean debug = false;

  private Comparator<K> comparator;

  private Function<SR, K> sourceRecordToId;

  private Function<TR, K> targetRecordToId;

  private Iterable<? extends SR> sourceRecords;

  private Iterable<? extends TR> targetRecords;

  private final CountTree counts = new CountTree();

  private Consumer<Channel<SR>> insertHandler;

  private Consumer<Channel<TR>> deleteHandler;

  private Consumer<Channel<SourceTargetRecord<SR, TR>>> updateHandler;

  private final Channel<SR> insertChannel = new Channel<>(new Buffer<>());

  private final Channel<TR> deleteChannel = new Channel<>(new Buffer<>());

  private final Channel<SourceTargetRecord<SR, TR>> updateChannel = new Channel<>(new Buffer<>());

  @SuppressWarnings("unchecked")
  @Override
  protected DatasetMerge<SR, TR, K> clone() {
    try {
      return (DatasetMerge<SR, TR, K>)super.clone();
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
      var sourceRef = recordNext(sourceIterator, this.sourceRecordToId, "source");
      var targetRef = recordNext(targetIterator, this.targetRecordToId, "target");
      while (sourceRef.hasRecord() || targetRef.hasRecord()) {
        if (this.debug) {
          Debug.println(sourceRef.id(), targetRef.id());
        }
        final int idCompare = targetRef.compareTo(this.comparator, sourceRef);
        if (idCompare > 0) {
          this.counts.addCount("insert");
          final var sourceRecord = sourceRef.record();
          this.insertChannel.write(sourceRecord);
          sourceRef = recordNext(sourceIterator, this.sourceRecordToId, "source");
        } else if (idCompare < 0) {
          this.counts.addCount("delete");
          final var targetRecord = targetRef.record();
          this.deleteChannel.write(targetRecord);
          targetRef = recordNext(targetIterator, this.targetRecordToId, "target");
        } else {
          this.counts.addCount("update");
          final var sourceRecord = sourceRef.record();
          final var targetRecord = targetRef.record();
          this.updateChannel.write(new SourceTargetRecord<SR, TR>(sourceRecord, targetRecord));
          sourceRef = recordNext(sourceIterator, this.sourceRecordToId, "source");
          targetRef = recordNext(targetIterator, this.targetRecordToId, "target");
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

  private <R3> RecordWithId<R3, K> recordNext(final Iterator<? extends R3> iterator,
    final Function<R3, K> recordToId, final String countLabel) {
    if (iterator.hasNext()) {
      final var record = iterator.next();
      final var id = recordToId.apply(record);
      this.counts.addCount(countLabel);
      return new RecordWithId<>(record, id);
    } else {
      return RecordWithId.empty();
    }
  }

  public CountTree run() {
    for (final var channel : Arrays.asList(this.insertChannel, this.updateChannel,
      this.deleteChannel)) {
      channel.writeConnect();
      channel.readConnect();
    }

    Concurrent.virtual()
      .parallel(//
        this::processRecords, //
        this::processDelete, //
        this::processInsert, //
        this::processUpdate//
      );

    return this.counts;
  }
}
