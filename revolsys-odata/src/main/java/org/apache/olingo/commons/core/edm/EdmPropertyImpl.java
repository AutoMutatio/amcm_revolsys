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
package org.apache.olingo.commons.core.edm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.edm.EdmMapping;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotation;
import org.apache.olingo.commons.api.edm.provider.CsdlMapping;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlConstantExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlConstantExpression.ConstantExpressionType;

import com.revolsys.collection.json.Json;
import com.revolsys.data.type.CollectionDataType;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.geometry.coordinatesystem.model.HorizontalCoordinateSystemProxy;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryDataTypes;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.io.PathName;
import com.revolsys.record.schema.FieldDefinition;

public class EdmPropertyImpl extends AbstractEdmNamed implements EdmProperty {

  private static Map<DataType, EdmPrimitiveTypeKind> EDM_BY_DATA_TYPE = new HashMap<>();

  private static Map<EdmPrimitiveTypeKind, DataType> DATA_TYPE_BY_EDM = new HashMap<>();

  private static Map<String, DataType> DATA_TYPE_BY_EDM_STRING = new HashMap<>();

  private static Map<DataType, EdmPrimitiveTypeKind> GEOMETRY_DATA_TYPE_MAP = new HashMap<>();

  private static Map<DataType, EdmPrimitiveTypeKind> GEOGRAPHY_DATA_TYPE_MAP = new HashMap<>();

  static {
    // addDataType(EdmPrimitiveTypeKind.Binary, DataTypes.BOOLEAN);
    addDataType(EdmPrimitiveTypeKind.Boolean, DataTypes.BOOLEAN);
    addDataType(EdmPrimitiveTypeKind.Byte, DataTypes.UBYTE);
    addDataType(EdmPrimitiveTypeKind.Date, DataTypes.SQL_DATE);
    addDataType(EdmPrimitiveTypeKind.DateTimeOffset, DataTypes.INSTANT, DataTypes.UTIL_DATE,
      DataTypes.DATE_TIME, DataTypes.TIMESTAMP);
    addDataType(EdmPrimitiveTypeKind.Decimal, DataTypes.DECIMAL);
    addDataType(EdmPrimitiveTypeKind.Double, DataTypes.DOUBLE);
    // addDataType(EdmPrimitiveTypeKind.Duration, DataTypes.DOUBLE);
    addDataType(EdmPrimitiveTypeKind.Guid, DataTypes.UUID);
    addDataType(EdmPrimitiveTypeKind.Int16, DataTypes.SHORT);
    addDataType(EdmPrimitiveTypeKind.Int32, DataTypes.INT);
    addDataType(EdmPrimitiveTypeKind.Int64, DataTypes.LONG);
    addDataType(EdmPrimitiveTypeKind.SByte, DataTypes.BYTE);
    addDataType(EdmPrimitiveTypeKind.Single, DataTypes.FLOAT);
    // addDataType(EdmPrimitiveTypeKind.Stream, DataTypes.STRING);
    addDataType(EdmPrimitiveTypeKind.String, DataTypes.STRING);
    addDataType(EdmPrimitiveTypeKind.TimeOfDay, DataTypes.TIME);

    addGeometryDataType(EdmPrimitiveTypeKind.Geometry, EdmPrimitiveTypeKind.Geography,
      GeometryDataTypes.GEOMETRY);
    addGeometryDataType(EdmPrimitiveTypeKind.GeometryCollection,
      EdmPrimitiveTypeKind.GeographyCollection, GeometryDataTypes.GEOMETRY_COLLECTION);
    addGeometryDataType(EdmPrimitiveTypeKind.GeometryPoint, EdmPrimitiveTypeKind.GeographyPoint,
      GeometryDataTypes.POINT);
    addGeometryDataType(EdmPrimitiveTypeKind.GeometryMultiPoint,
      EdmPrimitiveTypeKind.GeographyMultiPoint, GeometryDataTypes.MULTI_POINT);
    addGeometryDataType(EdmPrimitiveTypeKind.GeometryLineString,
      EdmPrimitiveTypeKind.GeographyLineString, GeometryDataTypes.LINE_STRING);
    addGeometryDataType(EdmPrimitiveTypeKind.GeometryMultiLineString,
      EdmPrimitiveTypeKind.GeographyMultiLineString, GeometryDataTypes.MULTI_LINE_STRING);
    addGeometryDataType(EdmPrimitiveTypeKind.GeometryPolygon, EdmPrimitiveTypeKind.GeographyPolygon,
      GeometryDataTypes.POLYGON);
    addGeometryDataType(EdmPrimitiveTypeKind.GeometryMultiPolygon,
      EdmPrimitiveTypeKind.GeographyMultiPolygon, GeometryDataTypes.MULTI_POLYGON);
  }

  private static void addAnnotation(final List<CsdlAnnotation> annotations, final String term,
    final ConstantExpressionType type, final Object value) {
    if (value != null) {
      final var annotation = new CsdlAnnotation().setTerm(term)
        .setExpression(new CsdlConstantExpression(type, value.toString()));
      annotations.add(annotation);
    }
  }

  private static void addDataType(final EdmPrimitiveTypeKind kind, final DataType... dataTypes) {
    addEdmToDataType(kind, dataTypes[0]);

    for (final DataType dataType : dataTypes) {
      EDM_BY_DATA_TYPE.put(dataType, kind);
    }
  }

  private static void addEdmToDataType(final EdmPrimitiveTypeKind kind, final DataType dataType) {
    DATA_TYPE_BY_EDM.put(kind, dataType);
    DATA_TYPE_BY_EDM_STRING.put(kind.getPathName()
      .toDotSeparated(), dataType);
  }

  private static void addGeometryDataType(final EdmPrimitiveTypeKind geometryKind,
    final EdmPrimitiveTypeKind geographyKind, final DataType dataType) {
    addEdmToDataType(geometryKind, dataType);
    addEdmToDataType(geographyKind, dataType);
    GEOMETRY_DATA_TYPE_MAP.put(dataType, geometryKind);
    GEOGRAPHY_DATA_TYPE_MAP.put(dataType, geographyKind);
  }

  public static DataType getDataTypeFromEdm(final String type) {
    return DATA_TYPE_BY_EDM_STRING.getOrDefault(type, DataTypes.STRING);
  }

  public static EdmPrimitiveTypeKind getEdmPrimitiveTypeKind(final DataType dataType,
    final GeometryFactory geometryFactory) {
    final boolean isGeometry = Geometry.class.isAssignableFrom(dataType.getJavaClass());
    if (dataType == Json.JSON_TYPE) {
      return EdmPrimitiveTypeKind.Untyped;
    } else if (dataType instanceof CollectionDataType) {
      final CollectionDataType collectionDataType = (CollectionDataType)dataType;
      final DataType contentType = collectionDataType.getContentType();
      return getEdmPrimitiveTypeKind(contentType, geometryFactory);
    } else if (isGeometry) {
      if (geometryFactory.isGeocentric()) {
        return GEOGRAPHY_DATA_TYPE_MAP.get(dataType);
      } else {
        return GEOMETRY_DATA_TYPE_MAP.get(dataType);
      }
    } else {
      return EDM_BY_DATA_TYPE.getOrDefault(dataType, EdmPrimitiveTypeKind.Untyped);
    }
  }

  public static int getSrid(final HorizontalCoordinateSystemProxy spatial) {
    return spatial.getHorizontalCoordinateSystemId();
  }

  public static <V> V toValue(final EdmPrimitiveTypeKind type, final String text) {
    return DATA_TYPE_BY_EDM.get(type)
      .toObject(text);
  }

  private final EdmTypeInfo typeInfo;

  private final EdmType propertyType;

  private final boolean required;

  private final boolean collection;

  private String defaultValue;

  private Integer maxLength;

  private Integer precision;

  private Integer scale;

  private int srid;

  private final PathName typeName;

  private CsdlMapping mapping;

  private String mimeType;

  private final EdmPrimitiveTypeKind fieldType;

  public EdmPropertyImpl(final Edm edm, final FieldDefinition field) {
    super(edm, field.getName(), null);
    final DataType dataType = field.getDataType();
    final boolean isGeometry = Geometry.class.isAssignableFrom(dataType.getJavaClass());
    final GeometryFactory geometryFactory = field.getGeometryFactory();
    this.fieldType = getEdmPrimitiveTypeKind(dataType, geometryFactory);
    this.required = field.isRequired();
    this.collection = field.isDataTypeCollection();
    final Object defaultValue = field.getDefaultValue();
    if (defaultValue != null) {
      this.defaultValue = defaultValue.toString();
    }
    final int length = field.getLength();
    final int scale = field.getScale();
    if (DataTypes.BYTE.equals(dataType) || DataTypes.SHORT.equals(dataType)
      || DataTypes.INT.equals(dataType) || DataTypes.LONG.equals(dataType)
      || DataTypes.FLOAT.equals(dataType) || DataTypes.DOUBLE.equals(dataType)) {
    } else if (Number.class.isAssignableFrom(dataType.getJavaClass())) {
      if (scale > 0) {
        this.precision = length + scale;
        this.scale = scale;
      } else if (length > 0) {
        this.precision = length;
      }
    } else if (DataTypes.BOOLEAN.equals(dataType)) {
    } else if (length > 0) {
      this.maxLength = length;
    }
    final List<CsdlAnnotation> annotations = new ArrayList<>();

    this.typeName = this.fieldType.getPathName();
    final String type = this.typeName.toDotSeparated();
    this.typeInfo = new EdmTypeInfo.Builder().setEdm(getEdm())
      .setIncludeAnnotations(true)
      .setTypeExpression(type)
      .build();

    this.propertyType = this.typeInfo.getType();

    if (isGeometry) {
      this.srid = getSrid(field);
      final int axisCount = geometryFactory.getAxisCount();
      addAnnotation(annotations, "Geometry.axisCount", ConstantExpressionType.Int, axisCount);
      final double scaleX = geometryFactory.getScaleX();
      if (scaleX != 0) {
        addAnnotation(annotations, "Geometry.scaleX", ConstantExpressionType.Float, scaleX);
      }
      final double scaleY = geometryFactory.getScaleY();
      if (scaleY != 0) {
        addAnnotation(annotations, "Geometry.scaleY", ConstantExpressionType.Float, scaleY);
      }
      final double scaleZ = geometryFactory.getScaleY();
      if (axisCount > 2 && scaleZ != 0) {
        addAnnotation(annotations, "Geometry.scaleZ", ConstantExpressionType.Float, scaleZ);
      }
    }
    field.setProperty("csdlType", type);
    field.setProperty("csdlTypeName", this.typeName);
    field.setProperty("csdlAnnotations", annotations);

    final List<EdmAnnotation> edmAnnotations = new ArrayList<>();
    for (final CsdlAnnotation annotation : annotations) {
      edmAnnotations.add(new EdmAnnotationImpl(getEdm(), annotation));
    }

    setAnnotations(edmAnnotations);
  }

  @Override
  public DataType getDataType() {
    return this.fieldType.getDataType();
  }

  @Override
  public String getDefaultValue() {
    return this.defaultValue;
  }

  public EdmPrimitiveTypeKind getEdmType() {
    return this.fieldType;
  }

  @Override
  public EdmMapping getMapping() {
    return this.mapping;
  }

  @Override
  public Integer getMaxLength() {
    return this.maxLength;
  }

  @Override
  public String getMimeType() {
    return this.mimeType;
  }

  @Override
  public Integer getPrecision() {
    return this.precision;
  }

  @Override
  public Integer getScale() {
    return this.scale;
  }

  @Override
  public String getScaleAsString() {
    return null;
  }

  @Override
  public int getSrid() {
    return this.srid;
  }

  @Override
  public EdmType getType() {
    return this.propertyType;
  }

  @Override
  public EdmType getTypeWithAnnotations() {
    return this.propertyType;
  }

  @Override
  public boolean isCollection() {
    return this.collection;
  }

  @Override
  public boolean isNullable() {
    return !this.required;
  }

  @Override
  public boolean isPrimitive() {
    return this.typeInfo.isPrimitiveType();
  }

}
