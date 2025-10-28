package com.revolsys.record.query.functions;

import java.util.List;

import com.revolsys.collection.list.Lists;
import com.revolsys.geometry.model.BoundingBox;
import com.revolsys.geometry.model.BoundingBoxProxy;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.record.query.BinaryCondition;
import com.revolsys.record.query.Column;
import com.revolsys.record.query.ColumnReference;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.Value;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;

public class F {

  public static SimpleFunction arrayAgg(final QueryValue value) {
    return new SimpleFunction("array_agg", value);
  }

  public static BinaryCondition containsAllKeys(final QueryValue left, final QueryValue right) {
    return new BinaryCondition(left, "?&", right);
  }

  public static BinaryCondition containsAnyKeys(final QueryValue left, final QueryValue right) {
    // ?| is escaped to ??| as ? is a placeholder
    return new BinaryCondition(left, "??|", right);
  }

  public static WithinDistance dWithin(final FieldDefinition field, final Geometry geometry,
    final double distance) {
    final Value geometryValue = Value.newValue(field, geometry);
    final Value distanceValue = Value.newValue(distance);
    return new WithinDistance(field, geometryValue, distanceValue);
  }

  public static WithinDistance dWithin(final String name, final Geometry geometry,
    double distance) {
    if (distance < 0) {
      distance = 0;
    }
    final Column column = new Column(name);
    final Value geometryValue = Value.newValue(geometry);
    final Value distanceValue = Value.newValue(distance);
    return new WithinDistance(column, geometryValue, distanceValue);
  }

  public static EnvelopeIntersects envelopeIntersects(final FieldDefinition field,
    final BoundingBox boundingBox) {
    if (field == null) {
      return null;
    } else {
      final Value value = Value.newValue(field, boundingBox);
      return new EnvelopeIntersects(field, value);
    }
  }

  public static EnvelopeIntersects envelopeIntersects(final List<QueryValue> values) {
    final QueryValue left = values.get(0);
    QueryValue right = values.get(1);
    if (!(left instanceof ColumnReference)) {
      throw new IllegalArgumentException(
        "geo.intersections first argument must be a column reference");
    }
    final ColumnReference field = (ColumnReference)left;
    if (right instanceof Value) {
      final Value value = (Value)right;
      final String text = value.getValue()
        .toString();
      final FieldDefinition fieldDefinition = field.getFieldDefinition();
      final GeometryFactory geometryFactory = fieldDefinition.getGeometryFactory();
      final Geometry geometry = geometryFactory.geometry(text);
      right = Value.newValue(fieldDefinition, geometry);
    } else if (right instanceof Column) {
      final Column value = (Column)right;
      final String text = value.getName();
      if (text.startsWith("geometry'")) {
        final FieldDefinition fieldDefinition = field.getFieldDefinition();
        final GeometryFactory geometryFactory = fieldDefinition.getGeometryFactory();
        final Geometry geometry = geometryFactory.geometry(text);
        right = Value.newValue(fieldDefinition, geometry);
      } else {
        throw new IllegalArgumentException(
          "geo.intersections second argument must be a geometry: " + right);
      }
    } else {
      throw new IllegalArgumentException(
        "geo.intersections second argument must be a geometry: " + right);
    }
    return new EnvelopeIntersects(left, right);
  }

  public static EnvelopeIntersects envelopeIntersects(final Query query,
    final BoundingBoxProxy boundingBox) {
    final RecordDefinition recordDefinition = query.getRecordDefinition();
    final FieldDefinition field = recordDefinition.getGeometryField();
    final BoundingBox bbox = boundingBox.getBoundingBox();
    final Value value = Value.newValue(field, bbox, true);
    final EnvelopeIntersects condition = new EnvelopeIntersects(field, value);
    query.and(condition);
    return condition;
  }

  public static EnvelopeIntersects envelopeIntersects(final Query query, final Geometry geometry) {
    final RecordDefinition recordDefinition = query.getRecordDefinition();
    final FieldDefinition field = recordDefinition.getGeometryField();
    final BoundingBox bbox = geometry.getBoundingBox();
    final Value value = Value.newValue(field, bbox);
    final EnvelopeIntersects condition = new EnvelopeIntersects(field, value);
    query.and(condition);
    return condition;
  }

  public static SimpleFunction function(final String name, final QueryValue... args) {
    return new SimpleFunction(name, args);
  }

  public static FunctionMultiArgs greatest(final QueryValue... arguments) {
    return new FunctionMultiArgs("GREATEST", Lists.newArray(arguments));
  }

  public static FunctionMultiArgs greatest(final QueryValue value1, final QueryValue value2) {
    return new FunctionMultiArgs("GREATEST", Lists.newArray(value1, value2));
  }

  public static Lower lower(final QueryValue value) {
    return new Lower(value);
  }

  public static Max max(final String name) {
    final Column column = new Column(name);
    return new Max(column);
  }

  public static Min min(final String name) {
    final Column column = new Column(name);
    return new Min(column);
  }

  public static RegexpReplace regexpReplace(final QueryValue value, final String pattern,
    final String replace) {
    return new RegexpReplace(value, pattern, replace);
  }

  public static RegexpReplace regexpReplace(final QueryValue value, final String pattern,
    final String replace, final String flags) {
    return new RegexpReplace(value, pattern, replace, flags);
  }

  public static ToChar toChar(final QueryValue column, final String format) {
    return new ToChar(column, format);
  }

  public static Upper upper(final FieldDefinition field) {
    return new Upper(field);
  }

  public static Upper upper(final QueryValue value) {
    return new Upper(value);
  }

  public static Upper upper(final String name) {
    final Column column = new Column(name);
    return upper(column);
  }
}
