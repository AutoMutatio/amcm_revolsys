/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.olingo.commons.api.data;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.api.edm.geo.Geospatial.Dimension;
import org.apache.olingo.commons.api.edm.geo.SRID;
import org.apache.olingo.commons.core.edm.EdmPropertyImpl;

import com.revolsys.collection.list.ListEx;
import com.revolsys.data.type.CollectionDataType;
import com.revolsys.data.type.DataType;
import com.revolsys.geometry.coordinatesystem.model.CoordinateSystem;
import com.revolsys.geometry.coordinatesystem.model.GeographicCoordinateSystem;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryCollection;
import com.revolsys.geometry.model.GeometryDataTypes;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.LinearRing;
import com.revolsys.geometry.model.MultiLineString;
import com.revolsys.geometry.model.MultiPoint;
import com.revolsys.geometry.model.MultiPolygon;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.model.Polygon;
import com.revolsys.odata.model.ODataEntityType;
import com.revolsys.record.Record;
import com.revolsys.record.schema.FieldDefinition;

/**
 * Data representation for a single entity.
 */
public class RecordEntity extends Linked implements ODataEntity {

  private final Record record;

  public RecordEntity(final ODataEntityType entityType, final Record record) {
    this.record = record;
    final var recordDefinition = entityType.getRecordDefinition();
    if (recordDefinition != null) {
      final String idFieldName = recordDefinition.getIdFieldName();
      final Object idValue = record.getValue(idFieldName);
      final URI id = entityType.createId(idValue);
      setId(id);
    }

  }

  @Override
  public boolean equals(final Object o) {
    final var entity = (RecordEntity)o;
    return this.record.equals(entity.record);
  }

  @Override
  public ListEx<String> getFieldNames() {
    return this.record.getRecordDefinition()
      .getFieldNames();
  }

  /**
   * Gets property with given name.
   *
   * @param name property name
   * @return property with given name if found, null otherwise
   */
  @Override
  public Property getProperty(final String name) {
    final var field = this.record.getField(name);
    if (field == null) {
      return null;
    } else {
      return newProperty(field);
    }
  }

  @Override
  public Object getValue(final String name) {
    return this.record.getValue(name);
  }

  @Override
  public int hashCode() {
    return this.record.hashCode();
  }

  private Property newProperty(final FieldDefinition field) {
    final var n = field.getName();
    Object value = this.record.getValue(n);
    ValueType valueType = ValueType.PRIMITIVE;
    final DataType dataType = field.getDataType();
    if (dataType instanceof CollectionDataType) {
      valueType = ValueType.COLLECTION_PRIMITIVE;
    } else if (Geometry.class.isAssignableFrom(dataType.getJavaClass())) {
      value = toGeometry(dataType, (Geometry)value);
    }
    return new Property(null, n, valueType, value);
  }

  private Geospatial toGeometry(final DataType dataType, final Geometry geometry) {
    if (geometry == null) {
      return null;
    }
    final CoordinateSystem coordinateSystem = geometry.getCoordinateSystem();
    final Dimension dimension = coordinateSystem instanceof GeographicCoordinateSystem
      ? Dimension.GEOGRAPHY
      : Dimension.GEOMETRY;
    final SRID srid = EdmPropertyImpl.getSrid(geometry);
    final Geospatial geospatial = toGeometry(dimension, srid, geometry);
    if (geospatial != null) {
      if (GeometryDataTypes.MULTI_POINT == dataType) {
        if (geospatial instanceof org.apache.olingo.commons.api.edm.geo.Point) {
          return new org.apache.olingo.commons.api.edm.geo.MultiPoint(dimension, srid,
            Collections.singletonList((org.apache.olingo.commons.api.edm.geo.Point)geospatial));
        }
      } else if (GeometryDataTypes.MULTI_LINE_STRING == dataType) {
        if (geospatial instanceof org.apache.olingo.commons.api.edm.geo.LineString) {
          return new org.apache.olingo.commons.api.edm.geo.MultiLineString(dimension, srid,
            Collections
              .singletonList((org.apache.olingo.commons.api.edm.geo.LineString)geospatial));
        }
      } else if (GeometryDataTypes.MULTI_POLYGON == dataType) {
        if (geospatial instanceof org.apache.olingo.commons.api.edm.geo.Polygon) {
          return new org.apache.olingo.commons.api.edm.geo.MultiPolygon(dimension, srid,
            Collections.singletonList((org.apache.olingo.commons.api.edm.geo.Polygon)geospatial));
        }
      } else if (GeometryDataTypes.GEOMETRY_COLLECTION == dataType) {
        if (!(geospatial instanceof org.apache.olingo.commons.api.edm.geo.GeospatialCollection)) {
          return new org.apache.olingo.commons.api.edm.geo.GeospatialCollection(dimension, srid,
            Collections.singletonList(geospatial));
        }
      }
    }
    return geospatial;
  }

  private Geospatial toGeometry(final Dimension dimension, final SRID srid, final Geometry value) {
    if (value instanceof Point) {
      return toPoint(dimension, srid, (Point)value);
    } else if (value instanceof LineString) {
      return toLineString(dimension, srid, (LineString)value);
    } else if (value instanceof Polygon) {
      return toPolygon(dimension, srid, (Polygon)value);
    } else if (value instanceof MultiPoint) {
      final MultiPoint point = (MultiPoint)value;
      return toMultiPoint(dimension, srid, point);
    } else if (value instanceof MultiLineString) {
      return toMultiLineString(dimension, srid, (MultiLineString)value);
    } else if (value instanceof MultiPolygon) {
      return toMultiPolygon(dimension, srid, (MultiPolygon)value);
    } else if (value instanceof GeometryCollection) {
      return toGeometryCollection(dimension, srid, (GeometryCollection)value);
    }
    return null;
  }

  private org.apache.olingo.commons.api.edm.geo.GeospatialCollection toGeometryCollection(
    final Dimension dimension, final SRID srid, final GeometryCollection geometryCollection) {
    final List<org.apache.olingo.commons.api.edm.geo.Geospatial> geometries = new ArrayList<>();
    for (final Geometry geometry : geometryCollection.geometries()) {
      geometries.add(toGeometry(dimension, srid, geometry));
    }
    return new org.apache.olingo.commons.api.edm.geo.GeospatialCollection(dimension, srid,
      geometries);
  }

  private org.apache.olingo.commons.api.edm.geo.LineString toLineString(final Dimension dimension,
    final SRID srid, final LineString line) {
    final List<org.apache.olingo.commons.api.edm.geo.Point> points = new ArrayList<>();
    final int vertexCount = line.getVertexCount();
    for (int i = 0; i < vertexCount; i++) {
      final org.apache.olingo.commons.api.edm.geo.Point oPoint = new org.apache.olingo.commons.api.edm.geo.Point(
        dimension, srid);
      oPoint.setX(line.getX(i));
      oPoint.setY(line.getY(i));
      oPoint.setZ(line.getZ(i));
      points.add(oPoint);
    }
    return new org.apache.olingo.commons.api.edm.geo.LineString(dimension, srid, points);
  }

  private org.apache.olingo.commons.api.edm.geo.MultiLineString toMultiLineString(
    final Dimension dimension, final SRID srid, final MultiLineString multiLineString) {
    final List<org.apache.olingo.commons.api.edm.geo.LineString> lines = new ArrayList<>();
    for (final LineString line : multiLineString.lineStrings()) {
      lines.add(toLineString(dimension, srid, line));
    }
    return new org.apache.olingo.commons.api.edm.geo.MultiLineString(dimension, srid, lines);
  }

  private org.apache.olingo.commons.api.edm.geo.MultiPoint toMultiPoint(final Dimension dimension,
    final SRID srid, final MultiPoint multiPoint) {
    final List<org.apache.olingo.commons.api.edm.geo.Point> points = new ArrayList<>();
    for (final Point point : multiPoint.points()) {
      points.add(toPoint(dimension, srid, point));
    }
    return new org.apache.olingo.commons.api.edm.geo.MultiPoint(dimension, srid, points);
  }

  private org.apache.olingo.commons.api.edm.geo.MultiPolygon toMultiPolygon(
    final Dimension dimension, final SRID srid, final MultiPolygon multiPolygon) {
    final List<org.apache.olingo.commons.api.edm.geo.Polygon> polygons = new ArrayList<>();
    for (final Polygon polygon : multiPolygon.polygons()) {
      polygons.add(toPolygon(dimension, srid, polygon));
    }
    return new org.apache.olingo.commons.api.edm.geo.MultiPolygon(dimension, srid, polygons);
  }

  private org.apache.olingo.commons.api.edm.geo.Point toPoint(final Dimension dimension,
    final SRID srid, final Point point) {
    final org.apache.olingo.commons.api.edm.geo.Point oPoint = new org.apache.olingo.commons.api.edm.geo.Point(
      dimension, srid);
    oPoint.setX(point.getX());
    oPoint.setY(point.getY());
    oPoint.setZ(point.getZ());
    return oPoint;
  }

  private org.apache.olingo.commons.api.edm.geo.Polygon toPolygon(final Dimension dimension,
    final SRID srid, final Polygon polygon) {
    final List<org.apache.olingo.commons.api.edm.geo.LineString> interiorRings = new ArrayList<>();
    final org.apache.olingo.commons.api.edm.geo.LineString exterior = toLineString(dimension, srid,
      polygon.getShell());
    for (int i = 0; i < polygon.getHoleCount(); i++) {
      final LinearRing ring = polygon.getHole(i);
      interiorRings.add(toLineString(dimension, srid, ring));
    }
    return new org.apache.olingo.commons.api.edm.geo.Polygon(dimension, srid, interiorRings,
      exterior);
  }

  @Override
  public String toString() {
    return this.record.toString();
  }
}
