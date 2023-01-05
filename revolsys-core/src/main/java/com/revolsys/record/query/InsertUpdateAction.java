package com.revolsys.record.query;

import com.revolsys.record.Record;

public interface InsertUpdateAction {

  default void insertOrUpdateRecord(final Record record) {
  }

  default void insertRecord(final Record record) {
    insertOrUpdateRecord(record);
  }

  default void updateRecord(final Record record) {
    insertOrUpdateRecord(record);
  }

}
