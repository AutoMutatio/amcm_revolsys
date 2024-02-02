package com.revolsys.function;

import java.util.function.Consumer;
import java.util.function.Function;

public interface Lambdaable<SELF extends Lambdaable<SELF>> {

  default SELF consume(final Consumer<SELF> action) {
    @SuppressWarnings("unchecked")
    final SELF self = (SELF)this;
    action.accept(self);
    return self;
  }

  default <V> V mapTo(final Function<SELF, V> action) {
    @SuppressWarnings("unchecked")
    final SELF self = (SELF)this;
    return action.apply(self);
  }

}
