package com.revolsys.function;

@FunctionalInterface
public interface BiFunctionInt<R> {
  R accept(int parameter1, int parameter2);
}
