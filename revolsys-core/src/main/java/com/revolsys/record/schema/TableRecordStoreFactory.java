package com.revolsys.record.schema;

import com.revolsys.collection.value.Single;
import com.revolsys.io.PathName;
import com.revolsys.io.PathNameProxy;

public interface TableRecordStoreFactory extends RecordDefinitionFactory {

  <TRS extends AbstractTableRecordStore> TRS getTableRecordStore(CharSequence pathName);

  <TRS extends AbstractTableRecordStore> TRS getTableRecordStore(PathNameProxy pathNameProxy);

  default Single<AbstractTableRecordStore> tableRecordStore(final PathName name) {
    final var recordStore = getTableRecordStore(name);
    return Single.ofNullable(recordStore);
  }

  default Single<AbstractTableRecordStore> tableRecordStore(final String name) {
    final var path = PathName.fromDotSeparated(name);
    return tableRecordStore(path);
  }
}
