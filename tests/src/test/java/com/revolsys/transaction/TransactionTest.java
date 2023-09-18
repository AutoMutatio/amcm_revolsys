package com.revolsys.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;

import org.jeometry.common.io.PathName;
import org.junit.jupiter.api.Test;

import com.revolsys.record.Record;
import com.revolsys.record.io.format.json.JsonObject;
import com.revolsys.record.schema.RecordStore;

public class TransactionTest {
  private static final String FIELD_LABEL = "label";

  private static final String FIELD_ID = "id";

  RecordStore recordStore = RecordStore.newRecordStoreInitialized(JsonObject.hash("connection",
    JsonObject.hash()
      .addValue("url", "jdbc:postgresql://localhost:42032/test")
      .addValue("user", "test")
      .addValue("password", "test")));

  final UUID id = UUID.fromString("b220454e-380c-4b83-96f5-fb390e26fe24");

  String originalLabel = "Original";

  String updatedLabel = "Updated";

  PathName testTable = PathName.newPathName("/test/Test");

  @Test
  void noTransactionSuccess() {
    this.recordStore//
      .newInsertUpdate(this.testTable)
      .search(r -> r.addValue(FIELD_ID, this.id))
      .common(r -> r.addValue(FIELD_LABEL, this.originalLabel))
      .execute();

    {
      final Record r = this.recordStore//
        .newQuery(this.testTable)
        .and(FIELD_ID, this.id)
        .getRecord();
      assertEquals(this.originalLabel, r.getString(FIELD_LABEL));
    }

    this.recordStore//
      .newUpdate(this.testTable)
      .search(r -> r.addValue(FIELD_ID, this.id))
      .common(r -> r.addValue(FIELD_LABEL, this.updatedLabel))
      .execute();

    {
      final Record r1 = this.recordStore//
        .newQuery(this.testTable)
        .and(FIELD_ID, this.id)
        .getRecord();
      assertEquals(this.updatedLabel, r1.getString(FIELD_LABEL));
    }

  }

  @Test
  void transactionSuccess() {
    try (
      Transaction transaction = this.recordStore.newTransaction()) {
      this.recordStore//
        .newInsertUpdate(this.testTable)
        .search(r -> r.addValue(FIELD_ID, this.id))
        .common(r -> r.addValue(FIELD_LABEL, this.originalLabel))
        .execute();

      {
        final Record r = this.recordStore//
          .newQuery(this.testTable)
          .and(FIELD_ID, this.id)
          .getRecord();
        assertEquals(this.originalLabel, r.getString(FIELD_LABEL));
      }

      this.recordStore//
        .newUpdate(this.testTable)
        .search(r -> r.addValue(FIELD_ID, this.id))
        .common(r -> r.addValue(FIELD_LABEL, this.updatedLabel))
        .execute();

      {
        final Record r1 = this.recordStore//
          .newQuery(this.testTable)
          .and(FIELD_ID, this.id)
          .getRecord();
        assertEquals(this.updatedLabel, r1.getString(FIELD_LABEL));
      }
    }
  }
}
