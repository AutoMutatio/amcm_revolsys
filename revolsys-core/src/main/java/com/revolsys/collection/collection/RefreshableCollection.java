package com.revolsys.collection.collection;

import java.util.Collection;

import com.revolsys.data.refresh.Refreshable;

public interface RefreshableCollection<V> extends Refreshable, Collection<V> {

  void clearValue();
}
