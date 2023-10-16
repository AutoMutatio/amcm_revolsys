package org.jeometry.common.collection.collection;

import java.util.Collection;

import org.jeometry.common.data.refresh.Refreshable;

public interface RefreshableCollection<V> extends Refreshable, Collection<V> {

  void clearValue();
}
