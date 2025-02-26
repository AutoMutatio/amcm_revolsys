package com.revolsys.function;

@FunctionalInterface
public interface Function5<P1, P2, P3, P4, P5, R> {
  R apply(P1 parameter1, P2 parameter2, P3 parameter3, P4 parameter4, P5 parameter5);
}
