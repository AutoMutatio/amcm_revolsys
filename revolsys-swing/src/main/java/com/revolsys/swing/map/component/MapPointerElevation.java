package com.revolsys.swing.map.component;

import java.awt.Dimension;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;
import java.beans.PropertyChangeListener;
import java.util.Collections;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.border.BevelBorder;

import com.revolsys.geometry.model.Point;
import com.revolsys.logging.Logs;
import com.revolsys.number.Doubles;
import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.parallel.channel.Channel;
import com.revolsys.parallel.channel.store.Overwrite;
import com.revolsys.swing.map.MapPanel;
import com.revolsys.swing.map.Viewport2D;
import com.revolsys.swing.map.layer.Project;
import com.revolsys.swing.map.layer.elevation.ElevationModelLayer;
import com.revolsys.swing.parallel.Invoke;
import com.revolsys.util.concurrent.Concurrent;

public class MapPointerElevation extends JLabel implements MouseMotionListener {

  private static final long serialVersionUID = 1L;

  private final Channel<Point> refreshChannel = new Channel<>(new Overwrite<>());

  private Viewport2D viewport;

  private final Project project;

  private List<ElevationModelLayer> layers = Collections.emptyList();

  private final ReentrantLockEx layerLock = new ReentrantLockEx();

  private int lastX = -1;

  private int lastY = -1;

  public MapPointerElevation(final MapPanel map) {
    this.project = map.getProject();
    this.viewport = map.getViewport();
    map.getMouseOverlay()
      .addMouseMotionListener(this);
    setBorder(
      BorderFactory.createCompoundBorder(BorderFactory.createBevelBorder(BevelBorder.LOWERED),
        BorderFactory.createEmptyBorder(2, 3, 2, 3)));
    setText(" ");

    final int height = getPreferredSize().height;
    setPreferredSize(new Dimension(100, height));
    setVisible(false);
    final PropertyChangeListener listener = e -> {
      try (
        var lock = this.layerLock.lockX()) {
        this.layers = null;
      }
    };
    this.project.addPropertyChangeListener("layers", listener);
    this.project.addPropertyChangeListener("visible", listener);
    Concurrent.virtual("MapGetElevation")
      .start(() -> {
        while (true) {
          try {

            final Point point = this.refreshChannel.read();
            if (point == null) {
              return;
            } else {
              final List<ElevationModelLayer> layers = getLayers();
              if (!layers.isEmpty()) {

                final double elevation = ElevationModelLayer.getElevation(layers, point);
                Invoke.later(() -> {
                  try (
                    var lock = this.layerLock.lockX()) {
                    if (Double.isFinite(elevation) && !getLayers().isEmpty()) {
                      setVisible(true);
                      final String text = Doubles.toString(Doubles.makePrecise(1000, elevation));
                      setText(text);
                    } else {
                      setVisible(false);
                    }
                  }
                });
              }
            }
          } catch (final Throwable e) {
            Logs.error(this, e);
          }
        }
      });
  }

  private List<ElevationModelLayer> getLayers() {
    if (this.layers == null) {
      try (
        var lock = this.layerLock.lockX()) {
        if (this.layers == null) {
          final double scale = this.viewport.getScale();
          this.layers = ElevationModelLayer.getVisibleLayers(this.project, scale);
          final boolean hasLayers = !this.layers.isEmpty();
          Invoke.later(() -> setVisible(hasLayers));
        }
      }
    }
    return this.layers;
  }

  @Override
  public void mouseDragged(final MouseEvent e) {
    mouseMoved(e);
  }

  @Override
  public void mouseMoved(final MouseEvent e) {
    if (!getLayers().isEmpty()) {
      final int x = e.getX();
      final int y = e.getY();
      if (x != this.lastX || y != this.lastY) {
        this.lastX = x;
        this.lastY = y;
        final Point mapLocation = this.viewport.toModelPoint(x, y);
        this.refreshChannel.write(mapLocation);
      }
    }
  }

  @Override
  public void removeNotify() {
    super.removeNotify();
    this.refreshChannel.write(null);
    this.viewport = null;
  }

  @Override
  public String toString() {
    return getText();
  }
}
