package com.revolsys.record.schema;

import java.sql.PreparedStatement;

import com.revolsys.io.PathName;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.SqlAppendable;

public class ViewRecordDefinition extends RecordDefinitionImpl {

  private final Query viewQuery;

  public ViewRecordDefinition(final RecordStoreSchema schema, final PathName pathName,
    final Query viewQuery) {
    RecordDefinition recordDefinition;
    try (
      var reader = schema.getRecordStore()
        .newQuery()
        .setFrom(viewQuery, "t")
        .setLimit(0)
        .getRecordReader()) {
      reader.open();
      recordDefinition = reader.getRecordDefinition();
      ((RecordDefinitionImpl)recordDefinition).setPathName(pathName);
    }
    super(schema, recordDefinition);
    this.viewQuery = viewQuery;
  }

  @Override
  public void appendFrom(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append('(');
    this.viewQuery.appendSql(statement, recordStore, sql);
    sql.append(") ");
  }

  @Override
  public int appendFromParameters(final int index, final PreparedStatement statement) {
    return this.viewQuery.appendFromParameters(index, statement);
  }
}
