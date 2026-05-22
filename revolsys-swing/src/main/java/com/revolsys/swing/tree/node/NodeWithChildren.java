package com.revolsys.swing.tree.node;

import java.util.List;
import java.util.function.Supplier;

import javax.swing.Icon;

import com.revolsys.collection.list.Lists;
import com.revolsys.swing.menu.MenuFactory;

public interface NodeWithChildren<C> {

  static void initMenu(MenuFactory menu) {
    menu.<NodeWithChildren<?>> addMenuItem("records", "Refresh", "page:refresh",
      NodeWithChildren::refresh, true);
  }

  default List<C> children() {
    return Lists.empty();
  }

  default Icon icon() {
    return null;
  }

  default boolean isAllowsChildren() {
    return true;
  }

  default String name() {
    return toString();
  }

  default void refresh() {
  }

  default SupplierChildrenTreeNode toTreeNode() {
    final var name = name();
    final var icon = icon();
    final Supplier<List<C>> childrenLoader = this::children;
    final var node = new SupplierChildrenTreeNode(this, name, icon, childrenLoader);
    if (!isAllowsChildren()) {
      node.setAllowsChildren(false);
    }
    return node;
  }
}
