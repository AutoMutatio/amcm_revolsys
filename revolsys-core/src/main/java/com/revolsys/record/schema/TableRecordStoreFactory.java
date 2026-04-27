package com.revolsys.record.schema;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.value.Single;
import com.revolsys.io.PathName;
import com.revolsys.io.PathNameProxy;

public interface TableRecordStoreFactory extends RecordDefinitionFactory {

  <TRS extends AbstractTableRecordStore> TRS getTableRecordStore(CharSequence pathName);

  <TRS extends AbstractTableRecordStore> TRS getTableRecordStore(PathName pathName);

  default <TRS extends AbstractTableRecordStore> TRS getTableRecordStore(
    final PathNameProxy pathNameProxy) {
    if (pathNameProxy != null) {
      final PathName pathName = pathNameProxy.getPathName();
      return getTableRecordStore(pathName);
    }
    return null;
  }

  RecordStoreSchema schema(String schemaName);

  ListEx<RecordStoreSchema> schemas();

  default Single<AbstractTableRecordStore> tableRecordStore(final PathName name) {
    final var recordStore = getTableRecordStore(name);
    return Single.ofNullable(recordStore);
  }

  default Single<AbstractTableRecordStore> tableRecordStore(final String name) {
    final var path = PathName.fromDotSeparated(name);
    return tableRecordStore(path);
  }
}
