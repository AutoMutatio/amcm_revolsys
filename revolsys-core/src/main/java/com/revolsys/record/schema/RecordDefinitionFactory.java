package com.revolsys.record.schema;

import com.revolsys.record.query.TableReference;

public interface RecordDefinitionFactory extends TableReferenceFactory {
  <RD extends RecordDefinition> RD getRecordDefinition(CharSequence path);

  @Override
  default <TR extends TableReference> TR getTableReference(final CharSequence path) {
    return getRecordDefinition(path);
  }
}
