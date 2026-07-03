package com.revolsys.record.query;

public class SimpleJoinBuilder extends BaseJoinBuilder<SimpleJoinBuilder> {

  private String fromJoinFieldName;

  private String toJoinFieldName;

  private String joinAlias;

  public String fromJoinFieldName() {
    return this.fromJoinFieldName;
  }

  public SimpleJoinBuilder fromJoinFieldName(final String fromJoinFieldName) {
    ensureEditible();
    this.fromJoinFieldName = fromJoinFieldName;
    return this;
  }

  @Override
  public Join getJoin(final Query query, final TableReferenceProxy fromTable) {
    var join = query.getJoin(this.joinTable, this.joinAlias);
    if (join == null) {
      join = query.join(JoinType.LEFT_OUTER_JOIN)
        .table(this.joinTable)//
        .setAlias(this.joinAlias);
      final var fromJoinColumn = fromTable.getColumn(this.fromJoinFieldName);
      final var toJoinColumn = join.getColumn(this.toJoinFieldName);
      join.on(fromJoinColumn, toJoinColumn);
    }
    return join;
  }

  public String joinAlias() {
    return this.joinAlias;
  }

  public SimpleJoinBuilder joinAlias(final String joinAlias) {
    ensureEditible();
    this.joinAlias = joinAlias;
    return this;
  }

  public String toJoinFieldName() {
    return this.toJoinFieldName;
  }

  public SimpleJoinBuilder toJoinFieldName(final String toJoinFieldName) {
    ensureEditible();
    this.toJoinFieldName = toJoinFieldName;
    return this;
  }
}
