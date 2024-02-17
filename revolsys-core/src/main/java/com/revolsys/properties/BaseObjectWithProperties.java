package com.revolsys.properties;

import jakarta.annotation.PreDestroy;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.exception.Exceptions;

public class BaseObjectWithProperties implements ObjectWithProperties {
  private MapEx properties = JsonObject.hash();

  private boolean cancelled = false;

  public BaseObjectWithProperties() {
  }

  public void cancel() {
    this.cancelled = true;
  }

  @Override
  protected BaseObjectWithProperties clone() {
    try {
      final BaseObjectWithProperties clone = (BaseObjectWithProperties)super.clone();
      clone.properties = Maps.newLinkedHashEx(this.properties);
      return clone;
    } catch (final CloneNotSupportedException e) {
      return Exceptions.throwUncheckedException(e);
    }
  }

  @Override
  @PreDestroy
  public void close() {
    clearProperties();
  }

  @Override
  public MapEx getProperties() {
    return this.properties;
  }

  public boolean isCancelled() {
    return this.cancelled;
  }

}
