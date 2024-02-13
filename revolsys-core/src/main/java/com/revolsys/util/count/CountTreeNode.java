package com.revolsys.util.count;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.list.ListEx;
import com.revolsys.record.Record;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.util.Emptyable;

public class CountTreeNode implements Emptyable {

  private final Map<String, CountTreeNode> nodeByKey = new TreeMap<>();

  private long count;

  public CountTreeNode() {
  }

  public long addCount() {
    return ++this.count;
  }

  private long addCount(final int i, final String... nodes) {
    if (i < nodes.length) {
      final var node = getNode(nodes[i]);
      return node.addCount(i + 1, nodes);
    } else {
      return addCount();
    }
  }

  public long addCount(final String... nodes) {
    return addCount(0, nodes);
  }

  public void clear() {
    this.count = 0;
    this.nodeByKey.clear();
  }

  public void forEachCount(final ListEx<String> fieldNames, final Record baseRecord,
    final Consumer<Record> action) {
    for (final var key : this.nodeByKey.keySet()) {
      final var fieldName = fieldNames.get(0);
      final var nodeRecord = baseRecord.clone()
        .addValue(fieldName, key);
      final ListEx<String> subFieldNames = fieldNames.subList(1);
      this.nodeByKey.get(key)
        .forEachCount(subFieldNames, nodeRecord, action);
    }

    if (this.count > 0) {
      final String fieldName;
      if (fieldNames.isEmpty()) {
        fieldName = "count";
      } else {
        fieldName = fieldNames.get(0);
      }
      final var countRecord = baseRecord.clone()
        .addValue(fieldName, this.count);
      action.accept(countRecord);
    }
  }

  public void forEachCount(final RecordDefinitionProxy recordDefinition,
    final Consumer<Record> action) {
    final RecordDefinition rd = recordDefinition.getRecordDefinition();
    final var baseRecord = rd.newRecord();
    forEachCount(rd.getFieldNames(), baseRecord, action);
  }

  public void forEachNode(final ListEx<String> fieldNames, final Record baseRecord,
    final Consumer<Record> action) {
    if (fieldNames.isEmpty()) {
      action.accept(baseRecord);
    } else {
      for (final var key : this.nodeByKey.keySet()) {
        final var fieldName = fieldNames.get(0);
        final var nodeRecord = baseRecord.clone()
          .addValue(fieldName, key);
        final ListEx<String> subFieldNames = fieldNames.subList(1);
        this.nodeByKey.get(key)
          .forEachNode(subFieldNames, nodeRecord, action);
      }
    }

  }

  public void forEachNode(final RecordDefinitionProxy recordDefinition,
    final Consumer<Record> action) {
    final RecordDefinition rd = recordDefinition.getRecordDefinition();
    final var baseRecord = rd.newRecord();
    forEachNode(rd.getFieldNames(), baseRecord, action);
  }

  public long getCount() {
    return this.count;
  }

  private CountTreeNode getNode(final int i, final String... nodes) {
    if (i < nodes.length) {
      final var node = getNode(nodes[i]);
      return node.getNode(i + 1, nodes);
    } else {
      return this;
    }
  }

  public CountTreeNode getNode(final String... nodes) {
    return getNode(0, nodes);
  }

  public CountTreeNode getNode(final String key) {
    if (key == null) {
      return null;
    } else {
      var node = this.nodeByKey.get(key);
      if (node == null) {
        node = new CountTreeNode();
        this.nodeByKey.put(key, node);
      }
      return node;
    }
  }

  public Set<String> getNodeKeys() {
    return this.nodeByKey.keySet();
  }

  @Override
  public boolean isEmpty() {
    return this.nodeByKey.isEmpty() && this.count <= 0;
  }

  public JsonObject toJson() {
    if (isEmpty()) {
      return JsonObject.EMPTY;
    } else {
      final JsonObject json = JsonObject.tree();
      for (final var key : this.nodeByKey.keySet()) {
        final var node = this.nodeByKey.get(key);
        json.addNotEmpty(key, node.toJson());
      }
      return json;
    }
  }
}
