package com.revolsys.record.io.format.json;

import java.util.ArrayDeque;
import java.util.Deque;

public final class JsonStatus {

  private final Deque<JsonState> states = new ArrayDeque<>();

  public int getDepth() {
    return this.states.size();
  }

  public String getLabel() {
    final JsonState state = statePeek();
    return state.getLabel();
  }

  @SuppressWarnings("unchecked")
  public <S extends JsonState> S statePeek() {
    return (S)this.states.peekLast();
  }

  public void statePop() {
    this.states.removeLast();
  }

  public void statePush(final JsonState state) {
    this.states.addLast(state);
  }

  @Override
  public String toString() {
    if (this.states.isEmpty()) {
      return "/";
    } else {
      final StringBuilder s = new StringBuilder();
      for (final JsonState state : this.states) {
        s.append('/');
        state.append(s);
      }
      return s.toString();
    }
  }
}
