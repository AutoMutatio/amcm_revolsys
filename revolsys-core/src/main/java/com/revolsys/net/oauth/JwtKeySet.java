package com.revolsys.net.oauth;

import java.util.HashMap;
import java.util.Map;

import com.revolsys.collection.json.JsonObject;

public class JwtKeySet {
  private final Map<String, JwtKey> keyById = new HashMap<>();

  private final JsonObject data;

  public JwtKeySet(final JsonObject data) {
    this.data = data;
  }

  public JwtKey getKey(final String id) {
    var jwtKey = this.keyById.get(id);
    if (jwtKey != null) {
      return jwtKey;
    }
    for (final var key : this.data.<JsonObject> getList("keys")) {
      if (key.equalValue("kid", id)) {
        jwtKey = new JwtKey(key);
        this.keyById.put(id, jwtKey);
        return jwtKey;
      }
    }
    return null;
  }
}
