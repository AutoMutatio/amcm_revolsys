package com.revolsys.swing.tree.node;

import java.util.List;
import java.util.function.Supplier;

import javax.swing.Icon;

import com.revolsys.collection.list.Lists;

public interface NodeWithChildren<C> {

  default List<C> children() {
    return Lists.empty();
  }

  default Icon icon() {
    return null;
  }

  default String name() {
    return toString();
  }

  default SupplierChildrenTreeNode toTreeNode() {
    final String name = name();
    final Icon icon = icon();
    final Supplier<List<C>> childrenLoader = this::children;
    return new SupplierChildrenTreeNode(this, name, icon, childrenLoader);
  }
}
