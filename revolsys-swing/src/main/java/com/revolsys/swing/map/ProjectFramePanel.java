package com.revolsys.swing.map;

import java.awt.Component;
import java.util.Map;

import javax.swing.Icon;

import org.jeometry.common.collection.map.MapEx;
import org.jeometry.common.util.ObjectWithProperties;

public interface ProjectFramePanel extends ObjectWithProperties {
  void activatePanelComponent(Component component, Map<String, Object> config);

  default void deletePanelComponent(final Component component) {
  }

  Icon getIcon();

  String getName();

  String getPath();

  Component newPanelComponent(MapEx config);
}
