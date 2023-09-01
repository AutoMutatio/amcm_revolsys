package com.revolsys.record.schema;

import org.jeometry.common.io.PathNameProxy;

public interface TableRecordStoreFactory {

  <TRS extends AbstractTableRecordStore> TRS getTableRecordStore(CharSequence pathName);

  <TRS extends AbstractTableRecordStore> TRS getTableRecordStore(PathNameProxy pathNameProxy);

}
