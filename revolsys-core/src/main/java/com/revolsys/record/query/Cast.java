package com.revolsys.record.query;

import com.revolsys.data.type.DataType;
import com.revolsys.record.schema.RecordStore;

public class Cast extends AbstractUnaryQueryValue {
  private final String dataType;

  public Cast(final QueryValue queryValue, final String dataType) {
    super(queryValue);
    this.dataType = dataType;
  }

  @Override
  public void appendDefaultSql(final Query query, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append("CAST(");
    super.appendDefaultSql(query, recordStore, buffer);
    buffer.append(" AS ");
    buffer.append(this.dataType);
    buffer.append(")");
  }

  @Override
  public Cast clone() {
    return (Cast)super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Cast) {
      final Cast condition = (Cast)obj;
      if (DataType.equal(condition.getDataType(), getDataType())) {
        return super.equals(condition);
      }
    }
    return false;
  }

  public String getDataType() {
    return this.dataType;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("CAST(");
    buffer.append(super.toString());
    buffer.append(" AS ");
    buffer.append(this.dataType);
    buffer.append(")");
    return buffer.toString();
  }
}
