package com.revolsys.util.metrics;

import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.Jsonable;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

public class TimingTree implements Jsonable {
  private final Map<String, TimingTree> childByKey = new ConcurrentHashMap<>();

  private final AtomicLong counter = new AtomicLong();

  private final AtomicLong totalTime = new AtomicLong();

  private final ListEx<String> path;

  public TimingTree() {
    this(Lists.newArray());
  }

  private TimingTree(final ListEx<String> path) {
    this.path = path;
  }

  public void addTime(final long time) {
    this.counter.incrementAndGet();
    this.totalTime.addAndGet(time);
  }

  public void addTime(final long time, final String... path) {
    var counter = this;
    for (final String key : path) {
      counter = counter.getChid(key);
    }
    counter.addTime(time);
  }

  public void appendJson(final JsonObject json) {
    final var key = this.path.get(this.path.size() - 1);
    final var count = this.counter.get();
    boolean hadChild = false;
    JsonObject subJson;
    if (this.childByKey.isEmpty()) {
      subJson = JsonObject.EMPTY;
    } else {
      subJson = JsonObject.hash();
      if (count > 0) {
        subJson.addValue("$count", count);
      }
      for (final var node : this.childByKey.values()) {
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
    for (final var child : this.childByKey.values()) {
      child.appendString(s);
    }
  }

  public TimingTree getChid(final String key) {
    return this.childByKey.computeIfAbsent(key, this::newChild);
  }

  public boolean isEmpty() {
    return this.counter.get() == 0 && this.childByKey.isEmpty();
  }

  private TimingTree newChild(final String key) {
    return new TimingTree(this.path.clone()
      .addValue(key));
  }

  public TimingTree removeChild(final String key) {
    this.childByKey.remove(key);
    return this;
  }

  @Override
  public JsonObject toJson() {
    final var json = JsonObject.hash();
    final var count = this.counter.get();
    if (count > 0) {
      json.addValue("$count", count);
    }
    for (final var key : new TreeSet<>(this.childByKey.keySet())) {
      final var node = this.childByKey.get(key);
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
