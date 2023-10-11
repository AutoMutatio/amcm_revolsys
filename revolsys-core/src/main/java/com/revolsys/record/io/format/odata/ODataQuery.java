package com.revolsys.record.io.format.odata;

import java.util.function.Consumer;
import java.util.function.Function;

import org.jeometry.common.json.JsonObject;
import org.jeometry.common.util.Cancellable;

import com.revolsys.record.query.Query;

public class ODataQuery extends Query {

  private final ODataResource resource;

  private int pageLimit = Integer.MAX_VALUE;

  public ODataQuery(final ODataResource resource) {
    this.resource = resource;
  }

  public int forEach(final Cancellable cancellable, final Consumer<JsonObject> action) {
    throw new UnsupportedOperationException();
  }

  public <V> V getFirst(final Function<JsonObject, V> converter) {
    throw new UnsupportedOperationException();
  }

  public void setPageLimit(final int pageLimit) {
    this.pageLimit = Math.max(0, pageLimit);
  }

}
