package com.revolsys.record.io.format.esri.rest.map;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.revolsys.collection.Parent;
import com.revolsys.collection.map.MapEx;
import com.revolsys.record.io.format.esri.rest.CatalogElement;
import com.revolsys.webservice.WebServiceResource;

public class GroupLayer extends LayerDescription implements Parent<LayerDescription> {
  private List<LayerDescription> layers = new ArrayList<>();

  private Map<String, LayerDescription> layersByName = new HashMap<>();

  public GroupLayer(final ArcGisRestAbstractLayerService service, final CatalogElement parent,
    final MapEx properties) {
    super(service, parent);
    initialize(properties);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C extends WebServiceResource> C getChild(final String name) {
    if (name == null) {
      return null;
    } else {
      refreshIfNeeded();
      return (C)this.layersByName.get(name.toLowerCase());
    }
  }

  @Override
  public List<LayerDescription> getChildren() {
    return getLayers();
  }

  @Override
  public String getIconName() {
    return "folder:table";
  }

  @SuppressWarnings("unchecked")
  public <L extends LayerDescription> L getLayer(final String name) {
    return (L)getChild(name);
  }

  public List<LayerDescription> getLayers() {
    return this.layers;
  }

  @Override
  protected void initialize(final MapEx properties) {
    super.initialize(properties);
    final ArcGisRestAbstractLayerService service = getService();
    final Map<String, LayerDescription> layersByName = new TreeMap<>();
    final Map<String, LayerDescription> layers = new TreeMap<>();
    final Map<String, GroupLayer> groups = new TreeMap<>();
    final List<MapEx> layerDefinitions = properties.getList("subLayers");
    for (final MapEx layerProperties : layerDefinitions) {
      final LayerDescription layer = service.addLayer(this, layersByName, layerProperties);
      if (layer != null) {
        final String layerName = layer.getName();
        if (layer instanceof GroupLayer) {
          final GroupLayer group = (GroupLayer)layer;
          groups.put(layerName, group);
        } else {
          layers.put(layerName, layer);
        }
      }
    }
    final List<LayerDescription> children = new ArrayList<>();
    children.addAll(groups.values());
    children.addAll(layers.values());
    this.layers = Collections.unmodifiableList(children);
    this.layersByName = layersByName;
  }
}
