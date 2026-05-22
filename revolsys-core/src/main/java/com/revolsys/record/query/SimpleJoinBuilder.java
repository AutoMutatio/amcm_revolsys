package com.revolsys.record.query;

public class SimpleJoinBuilder extends BaseJoinBuilder<SimpleJoinBuilder> {

  private String fromJoinFieldName;

  private String toJoinFieldName;

  private String joinAlias;

  public String fromJoinFieldName() {
    return fromJoinFieldName;
  }

  public SimpleJoinBuilder fromJoinFieldName(String fromJoinFieldName) {
    ensureEditible();
    this.fromJoinFieldName = fromJoinFieldName;
    return this;
  }

  @Override
  public Join getJoin(final Query query) {
    var join = query.getJoin(joinTable, joinAlias);
    if (join == null) {
      join = query.join(JoinType.LEFT_OUTER_JOIN)
        .table(joinTable)//
        .setAlias(joinAlias);
      final var fromJoinColumn = query.getColumn(fromJoinFieldName);
      final var toJoinColumn = join.getColumn(toJoinFieldName);
      join.on(fromJoinColumn, toJoinColumn);
    }
    return join;
  }

  public String joinAlias() {
    return joinAlias;
  }

  public SimpleJoinBuilder joinAlias(String joinAlias) {
    ensureEditible();
    this.joinAlias = joinAlias;
    return this;
  }

  public String toJoinFieldName() {
    return toJoinFieldName;
  }

  public SimpleJoinBuilder toJoinFieldName(String toJoinFieldName) {
    ensureEditible();
    this.toJoinFieldName = toJoinFieldName;
    return this;
  }
}
