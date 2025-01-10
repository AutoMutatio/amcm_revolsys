package com.revolsys.parallel.process;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.list.Lists;
import com.revolsys.parallel.process.DatasetMerge.SourceTargetRecord;

class DatasetMergeTest {

  private static JsonObject create(final int id) {
    return JsonObject.hash("id", id);
  }

  @Test
  void test() {

    final var record1 = create(1);
    final var record2 = create(2);
    final var record3 = create(3);
    final var record5 = create(5);
    final var record4 = create(4);
    final var sourceRecords = Lists.newArray(record1, record3, record5);
    final var targetRecords = Lists.newArray(record2, record3, record4);

    final var insertRecords = Lists.newArray(record1, record5);
    final var updateRecords = Lists.newArray(new SourceTargetRecord<>(record3, record3));
    final var deleteRecords = Lists.newArray(record2, record4);

    DatasetMerge.<JsonObject, Integer> builder()
      .sourceRecords(sourceRecords)
      .targetRecords(targetRecords)
      .recordToId(record -> record.getInteger("id"))
      .comparator(Integer::compare)
      .deleteRecordHandler(actual -> {
        final var expected = deleteRecords.removeFirst();
        Assert.assertEquals("deleteRecord", actual, expected);
      })
      .insertRecordHandler(actual -> {
        final var expected = insertRecords.removeFirst();
        Assert.assertEquals("insertRecord", actual, expected);
      })
      .updateRecordHandler(actual -> {
        final var expected = updateRecords.removeFirst();
        Assert.assertEquals("updateRecords", actual, expected);
      })
      .build()
      .run();
    Assert.assertEquals("insertRecords.size", 0, insertRecords.size());
    Assert.assertEquals("deleteRecords.size", 0, deleteRecords.size());
    Assert.assertEquals("updateRecords.size", 0, updateRecords.size());
  }

}
