package com.revolsys.record.schema;

import com.revolsys.record.query.ColumnReference;

public interface ArrayColumnReference extends ColumnReference {

  ColumnReference arrayElementColumn();
}
