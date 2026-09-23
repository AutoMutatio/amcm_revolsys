package com.revolsys.record.code;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.list.ListEx;

public class AliasTable {

  private final Map<String, String> valueByAlias = new HashMap<>();

  private final Map<String, Set<String>> aliasesByValue = new HashMap<>();

  public AliasTable addAlias(String value, Collection<String> aliases) {
    value = value.strip();
    @SuppressWarnings("unused")
    final var currentAliases = aliasesByValue.computeIfAbsent(value, v -> new TreeSet<>());
    valueByAlias.put(value.toLowerCase(), value);
    for (final var alias : aliases) {
      final var lowerAlias = alias.strip().toLowerCase();
      currentAliases.add(lowerAlias);
      valueByAlias.put(lowerAlias, value);
    }
    return this;
  }

  public AliasTable addAlias(String value, String... aliases) {
    return addAlias(value, Arrays.asList(aliases));
  }

  public boolean hasValue(String alias) {
    var hasValue = valueByAlias.containsKey(alias);
    if (!hasValue) {
      hasValue = valueByAlias.containsKey(alias.strip().toLowerCase());
    }
    return hasValue;
  }

  public AliasTable loadJsonList(ListEx<JsonObject> config) {
    config.forEach(mapping -> {
      final var value = mapping.getString("value");
      final var aliases = mapping.<String> getList("aliases");
      if (value != null) {
        addAlias(value, aliases);
      }
    });
    return this;
  }

  public String value(String alias) {
    var value = valueByAlias.get(alias);
    if (value == null) {
      value = valueByAlias.get(alias.strip().toLowerCase());
      if (value == null) {
        value = alias.strip();
      }
    }
    return value;
  }
}
