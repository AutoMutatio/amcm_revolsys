package com.revolsys.record.schema;

import com.revolsys.record.query.TableReference;

public interface TableReferenceFactory {
  <TR extends TableReference> TR getTableReference(CharSequence path);
}
