package com.revolsys.record.schema;

import java.util.Set;
import java.util.function.Consumer;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.io.PathName;
import org.jeometry.common.io.PathNameProxy;
import org.springframework.transaction.PlatformTransactionManager;

import com.revolsys.record.Record;
import com.revolsys.record.io.format.json.JsonObject;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transactionable;

public interface TableRecordStoreConnection extends Transactionable, TableRecordStoreFactory {

  Set<String> getGroupNames();

  default RecordDefinition getRecordDefinition(final CharSequence tableName) {
    final AbstractTableRecordStore recordStore = getTableRecordStore(tableName);
    if (recordStore == null) {
      return null;
    }
    return recordStore.getRecordDefinition();
  }

  RecordStore getRecordStore();

  @Override
  default <TRS extends AbstractTableRecordStore> TRS getTableRecordStore(
    final PathNameProxy pathNameProxy) {
    if (pathNameProxy != null) {
      final PathName pathName = pathNameProxy.getPathName();
      return getTableRecordStore(pathName);
    }
    return null;
  }

  @Override
  default PlatformTransactionManager getTransactionManager() {
    final RecordStore recordStore = getRecordStore();
    return recordStore.getTransactionManager();
  }

  default boolean isInGroup(final String name) {
    return getGroupNames().contains(name);
  }

  default Query newQuery(final CharSequence tablePath) {
    final AbstractTableRecordStore recordStore = getTableRecordStore(tablePath);
    return recordStore.newQuery(this);
  }

  default Record newRecord(final CharSequence tablePath) {
    final AbstractTableRecordStore tableRecordStore = getTableRecordStore(tablePath);
    return tableRecordStore.newRecord();
  }

  default Record newRecord(final CharSequence tablePath, final JsonObject json) {
    final AbstractTableRecordStore tableRecordStore = getTableRecordStore(tablePath);
    return tableRecordStore.newRecord(json);
  }

  default Record updateRecord(final CharSequence tablePath, final Identifier id,
    final Consumer<Record> updateAction) {
    final AbstractTableRecordStore tableRecordStore = getTableRecordStore(tablePath);
    return tableRecordStore.updateRecord(this, id, updateAction);
  }

  default Record updateRecord(final CharSequence tablePath, final Identifier id,
    final JsonObject values) {
    final AbstractTableRecordStore tableRecordStore = getTableRecordStore(tablePath);
    return tableRecordStore.updateRecord(this, id, values);
  }

}
