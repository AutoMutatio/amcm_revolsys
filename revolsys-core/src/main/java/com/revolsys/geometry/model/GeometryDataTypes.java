package com.revolsys.geometry.model;

import com.revolsys.data.type.DataType;
import com.revolsys.data.type.FunctionDataType;
import com.revolsys.geometry.model.editor.GeometryCollectionImplEditor;
import com.revolsys.geometry.model.editor.LineStringEditor;
import com.revolsys.geometry.model.editor.LinearRingEditor;
import com.revolsys.geometry.model.editor.MultiLineStringEditor;
import com.revolsys.geometry.model.editor.MultiPointEditor;
import com.revolsys.geometry.model.editor.MultiPolygonEditor;
import com.revolsys.geometry.model.editor.PointEditor;
import com.revolsys.geometry.model.editor.PolygonEditor;
import com.revolsys.record.RecordDataType;

public class GeometryDataTypes {

  public static final DataType BOUNDING_BOX = FunctionDataType.builder("boolean", BoundingBox.class)
    .toObjectFunction(BoundingBox::bboxGet)
    .build();

  public static final GeometryDataType<Geometry, GeometryCollectionImplEditor> GEOMETRY = new GeometryDataType<>(
    Geometry.class, Geometry::newGeometry, GeometryCollectionImplEditor::new);

  public static final GeometryDataType<GeometryCollection, GeometryCollectionImplEditor> GEOMETRY_COLLECTION = new GeometryDataType<>(
    GeometryCollection.class, GeometryCollection::newGeometryCollection,
    GeometryCollectionImplEditor::new);

  public static final DataType GEOMETRY_FACTORY = FunctionDataType
    .builder("GeometryFactory", GeometryFactory.class)
    .toObjectFunction(GeometryFactory::newGeometryFactory)
    .build();

  public static final GeometryDataType<LineString, LineStringEditor> LINE_STRING = new GeometryDataType<>(
    LineString.class, LineString::newLineString, LineStringEditor::new);

  public static final GeometryDataType<LinearRing, LinearRingEditor> LINEAR_RING = new GeometryDataType<>(
    LinearRing.class, LinearRing::newLinearRing, LinearRingEditor::new);

  public static final GeometryDataType<MultiLineString, MultiLineStringEditor> MULTI_LINE_STRING = new GeometryDataType<>(
    MultiLineString.class, Lineal::newLineal, MultiLineStringEditor::new);

  public static final GeometryDataType<MultiPoint, MultiPointEditor> MULTI_POINT = new GeometryDataType<>(
    MultiPoint.class, Punctual::newPunctual, MultiPointEditor::new);

  public static final GeometryDataType<MultiPolygon, MultiPolygonEditor> MULTI_POLYGON = new GeometryDataType<>(
    MultiPolygon.class, Polygonal::newPolygonal, MultiPolygonEditor::new);

  public static final GeometryDataType<Point, PointEditor> POINT = new GeometryDataType<>(
    Point.class, Point::newPoint, PointEditor::new);

  public static final GeometryDataType<Polygon, PolygonEditor> POLYGON = new GeometryDataType<>(
    Polygon.class, Polygon::newPolygon, PolygonEditor::new);

  public static final DataType RECORD = new RecordDataType();

  private GeometryDataTypes() {
  }

}
