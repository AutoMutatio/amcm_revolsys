package com.revolsys.parallel.process;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.parallel.channel.ClosedException;

class DatasetMergeTest {

  private static JsonObject create(final int id) {
    return JsonObject.hash("id", id);
  }

  @Test
  void multipleRecords() {
    final var record1 = create(1);
    final var record2 = create(2);
    final var record3 = create(3);
    final var record4 = create(4);
    final var record5 = create(5);
    final var sourceRecords = Lists.newArray(record1, record3, record5);
    final var targetRecords = Lists.newArray(record2, record3, record4);

    runTest(sourceRecords, targetRecords);
  }

  @Test
  void multipleRecords2() {
    final var record1 = create(1);
    final var record2 = create(2);
    final var record3 = create(3);
    final var record4 = create(4);
    final var record5 = create(5);
    final var sourceRecords = Lists.newArray(record1, record3, record4, record5);
    final var targetRecords = Lists.newArray(record2, record3, record4);

    runTest(sourceRecords, targetRecords);
  }

  private void runTest(final ListEx<JsonObject> sourceRecords,
    final ListEx<JsonObject> targetRecords) {
    final var insertedRecords = sourceRecords.clone();
    insertedRecords.removeAll(targetRecords);
    final var updatedRecords = sourceRecords.clone();
    updatedRecords.retainAll(targetRecords);
    final var deletedRecords = targetRecords.clone();
    deletedRecords.removeAll(sourceRecords);

    runTest(sourceRecords, targetRecords, insertedRecords, updatedRecords, deletedRecords);
    runTestBatch(sourceRecords, targetRecords, insertedRecords, updatedRecords, deletedRecords);
  }

  private void runTest(final ListEx<JsonObject> sourceRecords,
    final ListEx<JsonObject> targetRecords, final ListEx<JsonObject> expectedInsertedRecords,
    final ListEx<JsonObject> expectedUpdatedRecords,
    final ListEx<JsonObject> expectedDeletedRecords) {

    final var deletedRecords = expectedDeletedRecords.clone();
    final var insertedRecords = expectedInsertedRecords.clone();
    final var updatedRecords = expectedUpdatedRecords.clone();
    final var counts = DatasetMerge.<JsonObject, JsonObject, Integer> builder()
      .sourceRecords(sourceRecords)
      .targetRecords(targetRecords)
      .sourceRecordToId(record -> record.getInteger("id"))
      .targetRecordToId(record -> record.getInteger("id"))
      .comparator(Integer::compare)
      .deleteRecordHandler(actual -> {
        final var expected = deletedRecords.removeFirst();
        Assert.assertEquals("deleteRecord", actual, expected);
      })
      .insertRecordHandler(actual -> {
        final var expected = insertedRecords.removeFirst();
        Assert.assertEquals("insertRecord", actual, expected);
      })
      .updateRecordHandler((source, target) -> {
        final var expected = updatedRecords.removeFirst();
        Assert.assertEquals("updateRecords", source, expected);
        Assert.assertEquals("updateRecords", source, target);
      })
      .run();
    Assert.assertEquals("insertRecords.size", 0, insertedRecords.size());
    Assert.assertEquals("deleteRecords.size", 0, deletedRecords.size());
    Assert.assertEquals("updateRecords.size", 0, updatedRecords.size());

    Assert.assertEquals("count.source", counts.getCount("source"), sourceRecords.size());
    Assert.assertEquals("count.target", counts.getCount("target"), targetRecords.size());
    Assert.assertEquals("count.delete", counts.getCount("delete"), expectedDeletedRecords.size());
    Assert.assertEquals("count.insert", counts.getCount("insert"), expectedInsertedRecords.size());
    Assert.assertEquals("count.update", counts.getCount("update"), expectedUpdatedRecords.size());
  }

  private void runTestBatch(final ListEx<JsonObject> sourceRecords,
    final ListEx<JsonObject> targetRecords, final ListEx<JsonObject> expectedInsertedRecords,
    final ListEx<JsonObject> expectedUpdatedRecords,
    final ListEx<JsonObject> expectedDeletedRecords) {

    final var deletedRecords = expectedDeletedRecords.clone();
    final var insertedRecords = expectedInsertedRecords.clone();
    final var updatedRecords = expectedUpdatedRecords.clone();
    final var batchSize = 10;
    final var counts = DatasetMerge.<JsonObject, JsonObject, Integer> builder()
      .sourceRecords(sourceRecords)
      .targetRecords(targetRecords)
      .sourceRecordToId(record -> record.getInteger("id"))
      .targetRecordToId(record -> record.getInteger("id"))
      .comparator(Integer::compare)
      .deleteHandler(deletedChannel -> {
        while (deletedChannel.isOpen()) {
          try {
            for (int i = 0; i < batchSize; i++) {
              final var actual = deletedChannel.read();
              final var expected = deletedRecords.removeFirst();
              Assert.assertEquals("deleteRecord", actual, expected);
            }
          } catch (final ClosedException e) {
          }
        }
      })
      .insertHandler(insertedChannel -> {
        while (insertedChannel.isOpen()) {
          try {
            for (int i = 0; i < batchSize; i++) {
              final var actual = insertedChannel.read();
              final var expected = insertedRecords.removeFirst();
              Assert.assertEquals("insertRecord", actual, expected);
            }
          } catch (final ClosedException e) {
          }
        }
      })
      .updateHandler(updatedChannel -> {
        while (updatedChannel.isOpen()) {
          try {
            for (int i = 0; i < batchSize; i++) {
              final var records = updatedChannel.read();
              final var source = records.source();
              final var target = records.target();
              final var expected = updatedRecords.removeFirst();
              Assert.assertEquals("updateRecords", source, expected);
              Assert.assertEquals("updateRecords", source, target);
            }
          } catch (final ClosedException e) {
          }
        }
      })
      .run(); // Process all the source and return the counts

    Assert.assertEquals("insertRecords.size", 0, insertedRecords.size());
    Assert.assertEquals("deleteRecords.size", 0, deletedRecords.size());
    Assert.assertEquals("updateRecords.size", 0, updatedRecords.size());

    Assert.assertEquals("count.source", counts.getCount("source"), sourceRecords.size());
    Assert.assertEquals("count.target", counts.getCount("target"), targetRecords.size());
    Assert.assertEquals("count.delete", counts.getCount("delete"), expectedDeletedRecords.size());
    Assert.assertEquals("count.insert", counts.getCount("insert"), expectedInsertedRecords.size());
    Assert.assertEquals("count.update", counts.getCount("update"), expectedUpdatedRecords.size());
  }

  @Test
  void source1Record() {
    final var record1 = create(1);
    final var sourceRecords = Lists.newArray(record1);
    final var targetRecords = Lists.<JsonObject> newArray();

    runTest(sourceRecords, targetRecords);
  }

  @Test
  void source2Records() {
    final var record1 = create(1);
    final var record2 = create(2);
    final var sourceRecords = Lists.newArray(record1, record2);
    final var targetRecords = Lists.<JsonObject> newArray();

    runTest(sourceRecords, targetRecords);
  }

  @Test
  void sourceAfterTargetUpdateFirst() {
    final var record1 = create(1);
    final var record2 = create(2);
    final var sourceRecords = Lists.newArray(record1);
    final var targetRecords = Lists.newArray(record1, record2);

    runTest(sourceRecords, targetRecords);
  }

  @Test
  void sourceBeforeTargetNoUpdate() {
    final var record1 = create(1);
    final var record2 = create(2);
    final var record3 = create(3);
    final var record4 = create(4);
    final var sourceRecords = Lists.newArray(record1, record2);
    final var targetRecords = Lists.newArray(record3, record4);

    runTest(sourceRecords, targetRecords);
  }

  @Test
  void sourceBeforeTargetUpdateLast() {
    final var record1 = create(1);
    final var record2 = create(2);
    final var sourceRecords = Lists.newArray(record1, record2);
    final var targetRecords = Lists.newArray(record2);

    runTest(sourceRecords, targetRecords);
  }

  @Test
  void target1Record() {
    final var record1 = create(1);
    final var sourceRecords = Lists.<JsonObject> newArray();
    final var targetRecords = Lists.<JsonObject> newArray(record1);

    runTest(sourceRecords, targetRecords);
  }

  @Test
  void target2Records() {
    final var record1 = create(1);
    final var record2 = create(2);
    final var sourceRecords = Lists.<JsonObject> newArray();
    final var targetRecords = Lists.<JsonObject> newArray(record1, record2);

    runTest(sourceRecords, targetRecords);
  }

  @Test
  void targetBeforeSourceNoUpdate() {
    final var record1 = create(1);
    final var record2 = create(2);
    final var record3 = create(3);
    final var record4 = create(4);
    final var sourceRecords = Lists.newArray(record3, record4);
    final var targetRecords = Lists.newArray(record1, record2);

    runTest(sourceRecords, targetRecords);
  }
}
