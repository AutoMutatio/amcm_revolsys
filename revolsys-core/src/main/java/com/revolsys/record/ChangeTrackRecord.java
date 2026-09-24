package com.revolsys.record;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

public interface ChangeTrackRecord extends Record {

  static ChangeTrackRecord of(final Record record) {
    return new ChangeTrackRecordImpl(record);
  }

  default ChangeTrackRecord addValueIfNotModified(final String fieldName, final Object value) {
    if (!hasValue(fieldName) || getState() != RecordState.NEW && !isModified(fieldName)) {
      addValue(fieldName, value);
    }
    return this;
  }

  default ListEx<JsonObject> getModifiedValueList() {
    ListEx<JsonObject> modifiedValues = Lists.empty();
    final int fieldCount = getFieldCount();
    for (int i = 0; i < fieldCount; i++) {
      if (isModified(i)) {
        final String fieldName = getFieldName(i);
        if (modifiedValues.isEmpty()) {
          modifiedValues = Lists.newArray();
        }
        modifiedValues.add(JsonObject.hash()
          .addValue("fieldName", fieldName)
          .addValue("oldValue", getOriginalValue(i))
          .addValue("newValue", getValue(i)));
      }
    }
    return modifiedValues;
  }

  default JsonObject getModifiedValues() {
    JsonObject modifiedValues = JsonObject.EMPTY;
    final int fieldCount = getFieldCount();
    for (int i = 0; i < fieldCount; i++) {
      if (isModified(i)) {
        final String fieldName = getFieldName(i);
        if (modifiedValues.isEmpty()) {
          modifiedValues = JsonObject.hash();
        }
        modifiedValues.addValue(fieldName, getValue(i));
      }
    }
    return modifiedValues;
  }

  <T> T getOriginalValue(int fieldIndex);

  default <T> T getOriginalValue(final String fieldName) {
    final int fieldIndex = getFieldIndex(fieldName);
    return getOriginalValue(fieldIndex);
  }

  boolean isModified(int fieldIndex);

  default boolean isModified(final String fieldName) {
    final int fieldIndex = getFieldIndex(fieldName);
    return isModified(fieldIndex);
  }

  /*
   * Create a shallow clone of this record. The ChangeTrackRecord shouldn't be
   * used after this.
   */
  Record newRecord();
}
