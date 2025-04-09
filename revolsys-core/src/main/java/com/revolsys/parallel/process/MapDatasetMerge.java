package com.revolsys.parallel.process;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.revolsys.exception.Exceptions;
import com.revolsys.parallel.channel.Channel;
import com.revolsys.parallel.channel.store.Buffer;
import com.revolsys.util.concurrent.Concurrent;
import com.revolsys.util.count.CountTree;

/**
The MapDatasetMerge process is designed to detect insert, update, and deleted records for two
map data sets with a common key.

<b>NOTE: This will cause unexpected inserts/deletes if the data is not sorted.</b>

<h2>Simple per record callback example</h2>
<pre>
var counts = // Counts of source, target, delete, insert, update records
  DatasetMerge.<JsonObject, Integer> builder()
  .sourceRecordById(sourceRecordById) // Map of source records keyed by the id
  .targetRecordById(targetRecordById) // Map of source records keyed by the id
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
  .sourceRecordById(sourceRecordById) // Map of source records keyed by the id
  .targetRecordById(targetRecordById) // Map of source records keyed by the id
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
 * @param <SR>
 * @param <K>
 */
public final class MapDatasetMerge<SR, TR, K> implements Cloneable {

  public static class Builder<SR2, TR2, K2> {
    private final MapDatasetMerge<SR2, TR2, K2> merger = new MapDatasetMerge<>();

    public Builder() {
      // Default NoOp handlers
      deleteRecordHandler(r -> {
      });
      insertRecordHandler(r -> {
      });
      updateRecordHandler(r -> {
      });
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
      Objects.requireNonNull(this.merger.deleteHandler, "deleteHandler");
      Objects.requireNonNull(this.merger.insertHandler, "insertHandler");
      Objects.requireNonNull(this.merger.updateHandler, "updateHandler");
      Objects.requireNonNull(this.merger.sourceRecordById, "sourceRecordById");
      Objects.requireNonNull(this.merger.targetRecordById, "targetRecordById");
      return this.merger.clone()
        .run();
    }

    public Builder<SR2, TR2, K2> sourceRecordById(final Map<K2, ? extends SR2> sourceRecordById) {
      this.merger.sourceRecordById = Objects.requireNonNull(sourceRecordById, "sourceRecordById");
      return this;
    }

    public Builder<SR2, TR2, K2> targetRecordById(final Map<K2, ? extends TR2> targetRecordById) {
      this.merger.targetRecordById = Objects.requireNonNull(targetRecordById, "targetRecordById");
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

  private Map<K, ? extends SR> sourceRecordById;

  private Map<K, ? extends TR> targetRecordById;

  private final CountTree counts = new CountTree();

  private Consumer<Channel<SR>> insertHandler;

  private Consumer<Channel<TR>> deleteHandler;

  private Consumer<Channel<SourceTargetRecord<SR, TR>>> updateHandler;

  private final Channel<SR> insertChannel = new Channel<>(new Buffer<>());

  private final Channel<TR> deleteChannel = new Channel<>(new Buffer<>());

  private final Channel<SourceTargetRecord<SR, TR>> updateChannel = new Channel<>(new Buffer<>());

  @SuppressWarnings("unchecked")
  @Override
  protected MapDatasetMerge<SR, TR, K> clone() {
    try {
      return (MapDatasetMerge<SR, TR, K>)super.clone();
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
      for (final var sourceEntry : this.sourceRecordById.entrySet()) {
        final var key = sourceEntry.getKey();
        final var sourceRecord = sourceEntry.getValue();
        final var targetRecord = this.targetRecordById.get(key);
        if (targetRecord == null) {
          this.counts.addCount("insert");
          this.insertChannel.write(sourceRecord);
        } else {
          this.counts.addCount("update");
          this.updateChannel.write(new SourceTargetRecord<SR, TR>(sourceRecord, targetRecord));
        }
      }
      for (final var targetEntry : this.targetRecordById.entrySet()) {
        final var key = targetEntry.getKey();
        final var targetRecord = targetEntry.getValue();
        if (!this.sourceRecordById.containsKey(key)) {
          this.counts.addCount("delete");
          this.deleteChannel.write(targetRecord);
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
