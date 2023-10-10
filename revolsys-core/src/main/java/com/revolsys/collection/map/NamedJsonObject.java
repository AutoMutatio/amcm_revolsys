package com.revolsys.collection.map;

import java.util.Map;

import org.jeometry.common.json.JsonObjectHash;

import com.revolsys.collection.NameProxy;

public class NamedJsonObject extends JsonObjectHash implements NameProxy {
  private static final long serialVersionUID = -874346734708399858L;

  private final String name;

  public NamedJsonObject(final String name) {
    this.name = name;
  }

  public NamedJsonObject(final String name, final Map<String, ? extends Object> map) {
    super(map);
    this.name = name;
  }

  public NamedJsonObject(final String name, final String key, final Object value) {
    this.name = name;
    put(key, value);
  }

  @Override
  public String getName() {
    return this.name;
  }
}
