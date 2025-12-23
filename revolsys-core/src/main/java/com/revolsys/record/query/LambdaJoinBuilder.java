package com.revolsys.record.query;

import java.util.function.Function;

public class LambdaJoinBuilder extends BaseJoinBuilder<LambdaJoinBuilder> {

  private Function<Query, Join> joinFunction;

  @Override
  public Join getJoin(final Query query) {
    return joinFunction.apply(query);
  }

  public Function<Query, Join> joinFunction() {
    return joinFunction;
  }

  public LambdaJoinBuilder joinFunction(Function<Query, Join> joinFunction) {
    this.joinFunction = joinFunction;
    return this;
  }

}
