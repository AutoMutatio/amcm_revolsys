package com.revolsys.swing.tree.node;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import com.revolsys.collection.list.Lists;
import com.revolsys.logging.Logs;
import com.revolsys.swing.menu.MenuFactory;
import com.revolsys.swing.parallel.Invoke;
import com.revolsys.swing.tree.BaseTreeNode;
import com.revolsys.swing.tree.TreeNodes;

@SuppressWarnings("serial")
public abstract class LazyLoadTreeNode extends BaseTreeNode {
  public static void addRefreshMenuItem(final MenuFactory menu) {
    TreeNodes.addMenuItem(menu, "default", "Refresh", "arrow_refresh", LazyLoadTreeNode::refresh);
  }

  private boolean loaded = false;

  private final AtomicInteger updateIndicies = new AtomicInteger();

  public LazyLoadTreeNode() {
    this(null);
  }

  public LazyLoadTreeNode(final Object userObject) {
    super(userObject, true);
    setLoading();
  }

  @Override
  protected void closeDo() {
    setLoading();
    super.closeDo();
  }

  @Override
  public void collapseChildren() {
    if (isLoaded()) {
      super.collapseChildren();
    }
  }

  @Override
  public boolean isLoaded() {
    return this.loaded;
  }

  private boolean isLoading(final List<?> newNodes) {
    return newNodes.size() == 1 && newNodes.get(0) instanceof LoadingTreeNode;
  }

  public void loadChildren() {
    if (!isLoaded()) {
      this.loaded = true;
      refresh();
    }
  }

  protected List<BaseTreeNode> loadChildrenDo() {
    return new ArrayList<>();
  }

  protected int nextUpdateIndex() {
    return this.updateIndicies.incrementAndGet();
  }

  @Override
  public void nodeCollapsed(final BaseTreeNode treeNode) {
    super.nodeCollapsed(treeNode);
    if (treeNode != this) {
      nextUpdateIndex();
      setLoading();
    }
  }

  public final void refresh() {
    refreshDo();
  }

  protected void refreshDo() {
    final int updateIndex = nextUpdateIndex();
    Invoke.background("Refresh tree nodes " + getName(), () -> {
      try {
        return loadChildrenDo();
      } catch (final Throwable e) {
        Logs.error(this, "Error refreshing: " + getName(), e);
        return Lists.<BaseTreeNode> empty();
      }
    }, children -> setChildren(updateIndex, children));

  }

  public final void removeNode(final BaseTreeNode node) {
    final var children = this.children;
    if (children != null && isLoaded()) {
      final int index = children.indexOf(node);
      removeNode(index);
    }
  }

  public final void removeNode(final int index) {
    final var children = this.children;
    if (children != null && isLoaded()) {
      if (index > 0 && index < children.size()) {
        final var node = children.remove(index);
        nodeRemoved(index, node);
      }
    }
  }

  private void setChildren(final int updateIndex, final List<BaseTreeNode> newNodes) {
    if (updateIndex == this.updateIndicies.get()) {
      if (this.children == null) {
        newNodes.forEach(this::add);
      } else {
        final boolean oldLoading = isLoading(this.children);
        final boolean newLoading = isLoading(newNodes);
        this.loaded = !newLoading;
        if (oldLoading != newLoading) {
          removeAllChildren();
          newNodes.forEach(this::add);
        } else {
          final var oldNodes = Lists.<BaseTreeNode> newArray();
          oldNodes.addAll((Vector)this.children);
          removeAllChildren();

          for (final var newNode : newNodes) {
            final int index = oldNodes.indexOf(newNode);
            if (index == -1) {
              add(newNode);
            } else {
              final var oldNode = oldNodes.get(index);
              add(oldNode);
            }
          }
        }
      }
      nodeStructureChanged();
    }
  }

  private void setLoading() {
    removeAllChildren();
    add(new LoadingTreeNode());
    this.loaded = false;
  }
}
