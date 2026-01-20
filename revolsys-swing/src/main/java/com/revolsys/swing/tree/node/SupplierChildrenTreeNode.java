package com.revolsys.swing.tree.node;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import javax.swing.Icon;

import com.revolsys.swing.tree.BaseTreeNode;

public class SupplierChildrenTreeNode extends LazyLoadTreeNode {
  private final Supplier<Iterable<?>> childrenLoader;

  public <I extends Iterable<?>> SupplierChildrenTreeNode(Object userData, final String name,
    final Icon icon, final Supplier<I> childrenLoader) {
    super(userData);
    setName(name);
    setIcon(icon);
    this.childrenLoader = (Supplier)childrenLoader;
  }

  public <I extends Iterable<?>> SupplierChildrenTreeNode(final String name, final Icon icon,
    final Supplier<I> childrenLoader) {
    super(name);
    setName(name);
    setIcon(icon);
    this.childrenLoader = (Supplier)childrenLoader;
  }

  @Override
  protected List<BaseTreeNode> loadChildrenDo() {
    final List<BaseTreeNode> nodes = new ArrayList<>();
    final Iterable<?> children = this.childrenLoader.get();
    for (final Object child : children) {
      final BaseTreeNode node = BaseTreeNode.newTreeNode(child);
      if (node != null) {
        nodes.add(node);
      }
    }
    return nodes;
  }
}
