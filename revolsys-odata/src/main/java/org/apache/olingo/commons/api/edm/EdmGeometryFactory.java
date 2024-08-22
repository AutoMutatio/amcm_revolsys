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
package org.apache.olingo.commons.api.edm;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryCollection;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.MultiLineString;
import com.revolsys.geometry.model.MultiPoint;
import com.revolsys.geometry.model.MultiPolygon;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.model.Polygon;

public class EdmGeometryFactory {

  public static EdmGeometryFactory GEOMETRY = new EdmGeometryFactory(
    ODataGeometryOrGeography.GEOMETRY);

  public static EdmGeometryFactory GEOGRAPHY = new EdmGeometryFactory(
    ODataGeometryOrGeography.GEOGRAPHY);

  private static final Pattern PATTERN = Pattern
    .compile("([a-z]+)'SRID=([0-9]+);([a-zA-Z]+)\\(.*\\)'");

  private static final Pattern COLLECTION_PATTERN = Pattern
    .compile("([a-z]+)'SRID=([0-9]+);Collection\\(([a-zA-Z]+)\\((.*)\\)\\)'");

  public static int getSrid(final String exp) {
    if ("variable".equalsIgnoreCase(exp)) {
      return -1;
    } else {
      return Integer.parseInt(exp);
    }
  }

  private final ODataGeometryOrGeography dimension;

  private EdmGeometryFactory(final ODataGeometryOrGeography dimension) {
    this.dimension = dimension;
  }

  public String collectionToString(final Object value) {
    final var collection = (GeometryCollection)value;
    return toString(collection);
  }

  private Matcher getMatcher(final Pattern pattern, final String value) {
    final Matcher matcher = pattern.matcher(value);
    if (!matcher.matches()) {
      throw new EdmPrimitiveTypeException(
        "The literal '" + value + "' is not a valid " + this.dimension + ".");
    }

    final var dimension = ODataGeometryOrGeography.valueOf(matcher.group(1)
      .toUpperCase());
    if (dimension != this.dimension) {
      throw new EdmPrimitiveTypeException(
        "The literal '" + value + "' should be a " + dimension + ".");
    }

    return matcher;
  }

  public String lineStringToString(final Object value) {
    final var lineString = (LineString)value;
    return toString(lineString);
  }

  public String multiLineStringToString(final Object value) {
    final var multiLineString = (MultiLineString)value;
    return toString(multiLineString);
  }

  public String multiPointToString(final Object value) {
    final var multiPoint = (MultiPoint)value;
    return toString(multiPoint);
  }

  public String multiPolygonToString(final Object value) {
    final var multiPolygon = (MultiPolygon)value;
    return toString(multiPolygon);
  }

  public String pointToString(final Object value) {
    final var point = (Point)value;
    return toString(point);
  }

  public String polygonToString(final Object value) {
    final var polygon = (Polygon)value;
    return toString(polygon);
  }

  public GeometryCollection stringToCollection(final Object value) {
    if (value instanceof final GeometryCollection geometry) {
      return geometry;
    } else {
      final var s = value.toString();
      final Matcher matcher = getMatcher(COLLECTION_PATTERN, s);
      final var srid = getSrid(matcher.group(2));
      return GeometryFactory.floating(srid, 3)
        .geometry(matcher.group(3));
    }
  }

  public LineString stringToLineString(final Object value) {
    if (value instanceof final LineString geometry) {
      return geometry;
    } else {
      final var s = value.toString();
      final Matcher matcher = getMatcher(PATTERN, s);
      final var srid = getSrid(matcher.group(2));
      return GeometryFactory.floating(srid, 3)
        .geometry(matcher.group(3));
    }
  }

  public MultiLineString stringToMultiLineString(final Object value) {
    if (value instanceof final MultiLineString geometry) {
      return geometry;
    } else {
      final var s = value.toString();
      final Matcher matcher = getMatcher(PATTERN, s);
      final var srid = getSrid(matcher.group(2));
      return GeometryFactory.floating(srid, 3)
        .geometry(matcher.group(3));
    }
  }

  public MultiPoint stringToMultiPoint(final Object value) {
    if (value instanceof final MultiPoint geometry) {
      return geometry;
    } else {
      final var s = value.toString();
      final Matcher matcher = getMatcher(PATTERN, s);
      final var srid = getSrid(matcher.group(2));
      return GeometryFactory.floating(srid, 3)
        .geometry(matcher.group(3));
    }
  }

  public MultiPolygon stringToMultiPolygon(final Object value) {
    if (value instanceof final MultiPolygon geometry) {
      return geometry;
    } else {
      final var s = value.toString();
      final Matcher matcher = getMatcher(PATTERN, s);
      final var srid = getSrid(matcher.group(2));
      return GeometryFactory.floating(srid, 3)
        .geometry(matcher.group(3));
    }
  }

  public Point stringToPoint(final Object value) {
    if (value instanceof final Point point) {
      return point;
    } else {
      final var s = value.toString();
      final Matcher matcher = getMatcher(PATTERN, s);
      final var srid = getSrid(matcher.group(2));
      return GeometryFactory.floating(srid, 3)
        .geometry(matcher.group(3));
    }
  }

  public Polygon stringToPolygon(final Object value) {
    if (value instanceof final Polygon geometry) {
      return geometry;
    } else {
      final var s = value.toString();
      final Matcher matcher = getMatcher(PATTERN, s);
      final var srid = getSrid(matcher.group(2));
      return GeometryFactory.floating(srid, 3)
        .geometry(matcher.group(3));
    }
  }

  private String toString(final Geometry geometry) {
    final var sb = new StringBuilder();
    if (geometry.isProjected()) {
      sb.append("geometry");
    } else {
      sb.append("geography");
    }
    return sb.append('\'')
      .append(geometry.toEwkt())
      .append('\'')
      .toString();
  }
}
