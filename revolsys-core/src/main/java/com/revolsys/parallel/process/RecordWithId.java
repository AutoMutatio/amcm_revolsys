package com.revolsys.parallel.process;

import java.util.Comparator;

public record RecordWithId<R2, K2>(R2 record, K2 id) {

  private static final RecordWithId<?, ?> EMPTY = new RecordWithId<>(null, null);

  @SuppressWarnings("unchecked")
  public static <R2, K3> RecordWithId<R2, K3> empty() {
    return (RecordWithId<R2, K3>)EMPTY;
  }

  public int compareTo(final Comparator<K2> comparator, final RecordWithId<?, K2> other) {
    if (other.hasRecord()) {
      if (hasRecord()) {
        return comparator.compare(this.id, other.id);
      } else {
        return 1;
      }
    } else if (hasRecord()) {
      return -1;
    } else {
      return 0;
    }
  }

  public boolean hasRecord() {
    return this.record != null;
  }
}
