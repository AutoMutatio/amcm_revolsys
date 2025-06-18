package com.revolsys.util.count;

import java.lang.ScopedValue.Carrier;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.Jsonable;
import com.revolsys.exception.Exceptions;

public class ProgressTree implements Jsonable {

  public static final ScopedValue<ProgressTree> SCOPED_VALUE = ScopedValue.newInstance();

  public static void child(final String type, final String name,
    final Consumer<ProgressTree> action) {
    ProgressTree tree;
    if (SCOPED_VALUE.isBound()) {
      tree = getTree().addChild(type, name);
    } else {
      tree = new ProgressTree();
    }
    run(tree, action);
  }

  public static void child(final String type, final String name, final Runnable action) {
    ProgressTree tree;
    if (SCOPED_VALUE.isBound()) {
      tree = getTree().addChild(type, name);
    } else {
      tree = new ProgressTree();
    }
    scoped(tree).run(action);
  }

  public static <V> V childTemp(final String type, final String name, final Callable<V> action) {
    count(type);
    try {
      if (name == null) {
        return null;
      } else {
        if (SCOPED_VALUE.isBound()) {
          final var parent = getTree();
          final var tree = parent.addChild(type, name);
          try {
            return scoped(tree).call(action);
          } finally {
            parent.removeChild(tree);
          }
        } else {
          final var tree = new ProgressTree();
          return scoped(tree).call(action);
        }
      }
    } catch (final Exception e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public static void childTemp(final String type, String name, final Runnable action) {
    count(type);
    if (name == null) {
      name = UUID.randomUUID()
        .toString();
    }
    if (SCOPED_VALUE.isBound()) {
      final var parent = getTree();
      final var tree = parent.addChild(type, name);
      try {
        scoped(tree).run(action);
      } finally {
        parent.removeChild(tree);
      }
    } else {
      final var tree = new ProgressTree();
      scoped(tree).run(action);
    }
  }

  public static void count(final String label) {
    if (SCOPED_VALUE.isBound()) {
      getTree().addCount(label);
    }
  }

  private static ProgressTree getTree() {
    return SCOPED_VALUE.get();
  }

  public static void root(final Consumer<ProgressTree> action) {
    final var tree = new ProgressTree();
    run(tree, action);
  }

  public static void root(final Runnable action) {
    final var tree = new ProgressTree();
    scoped(tree).run(action);
  }

  private static void run(final ProgressTree tree, final Consumer<ProgressTree> action) {
    scoped(tree).run(() -> action.accept(tree));
  }

  private static Carrier scoped(final ProgressTree tree) {
    return ScopedValue.where(SCOPED_VALUE, tree);
  }

  private final ConcurrentLinkedQueue<ProgressTree> children = new ConcurrentLinkedQueue<>();

  private final CountTree counts = new CountTree();

  private final String name;

  private final String type;

  private ProgressTree parent;

  public ProgressTree() {
    this.name = "";
    this.type = "";
  }

  private ProgressTree(final ProgressTree parent, final String type, final String name) {
    this.parent = parent;
    this.type = type;
    this.name = name;
  }

  public ProgressTree addChild(final String type, final String name) {
    final var tree = new ProgressTree(this, type, name);
    this.children.add(tree);
    return tree;
  }

  public long addCount(final String label) {
    if (this.parent != null) {
      this.parent.addCount(label);
    }
    return this.counts.addCount(label);
  }

  public JsonObject countsToJson() {
    return this.counts.toJson();
  }

  public void removeChild(final ProgressTree tree) {
    this.children.remove(tree);
  }

  @Override
  public JsonObject toJson() {
    final var result = JsonObject.hash()
      .addValue("$type", this.type);
    if (!this.counts.isEmpty()) {
      result.addValue("$counts", this.counts);
    }
    for (final var child : this.children) {
      final var childJson = child.toJson();
      if (!childJson.isEmpty()) {
        result.addValue(child.name, child);
      }
    }
    result.removeEmptyProperties();
    return result;
  }

  @Override
  public String toString() {
    return this.name;
  }
}
