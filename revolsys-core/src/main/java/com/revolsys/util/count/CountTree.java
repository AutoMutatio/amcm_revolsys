package com.revolsys.util.count;

import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.Jsonable;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

public class CountTree implements Jsonable {
  private final Map<String, CountTree> counterByKey = new ConcurrentHashMap<>();

  private final AtomicLong counter = new AtomicLong();

  private final ListEx<String> path;

  public CountTree() {
    this(Lists.newArray());
  }

  private CountTree(final ListEx<String> path) {
    this.path = path;
  }

  public long addCount() {
    return this.counter.incrementAndGet();
  }

  public long addCount(final String... path) {
    var counter = this;
    for (final String key : path) {
      counter = counter.getCounter(key);
    }
    return counter.addCount();
  }

  public void appendJson(final JsonObject json) {
    final var key = this.path.get(this.path.size() - 1);
    final var count = this.counter.get();
    boolean hadChild = false;
    JsonObject subJson;
    if (this.counterByKey.isEmpty()) {
      subJson = JsonObject.EMPTY;
    } else {
      subJson = JsonObject.hash();
      if (count > 0) {
        subJson.addValue("$count", count);
      }
      for (final var node : this.counterByKey.values()) {
        final var size = subJson.size();
        node.appendJson(subJson);
        if (size != subJson.size()) {
          hadChild = true;
        }
      }
    }
    if (hadChild) {
      json.addValue(key, subJson);
    } else {
      json.addValue(key, count);
    }

  }

  public void appendString(final StringBuilder s) {
    if (this.counter.get() > 0) {
      s.append(this.path.join("\t"))
        .append('\t')
        .append(this.counter.get())
        .append('\n');
    }
    for (final var counter : this.counterByKey.values()) {
      counter.appendString(s);
    }
  }

  public long getCount() {
    return this.counter.get();
  }

  public CountTree getCounter(final String key) {
    return this.counterByKey.computeIfAbsent(key, this::newCounter);
  }

  public boolean isEmpty() {
    return this.counter.get() == 0 && this.counterByKey.isEmpty();
  }

  protected CountTree newCounter(final String key) {
    return new CountTree(this.path.clone()
      .addValue(key));
  }

  @Override
  public JsonObject toJson() {
    final var json = JsonObject.hash();
    final var count = this.counter.get();
    if (count > 0) {
      json.addValue("$count", count);
    }
    for (final var key : new TreeSet<>(this.counterByKey.keySet())) {
      final var node = this.counterByKey.get(key);
      if (node != null) {
        node.appendJson(json);
      }
    }
    return json;
  }

  @Override
  public String toString() {
    final var sb = new StringBuilder();
    appendString(sb);
    return sb.toString();
  }
}
