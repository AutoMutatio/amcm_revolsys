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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;

import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.data.type.FunctionDataType;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryCollection;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.MultiLineString;
import com.revolsys.geometry.model.MultiPoint;
import com.revolsys.geometry.model.MultiPolygon;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.model.Polygon;
import com.revolsys.io.PathName;

/**
 * Enumeration of all primitive type kinds.
 */
public enum EdmPrimitiveTypeKind implements EdmPrimitiveType {

  Binary(DataTypes.BASE64_URL_BINARY), //
  Boolean(DataTypes.BOOLEAN), //
  Byte(DataTypes.UBYTE), //
  SByte(DataTypes.BYTE), //
  Date(DataTypes.LOCAL_DATE), //
  DateTimeOffset(DataTypes.INSTANT), //
  TimeOfDay(DataTypes.TIME), //
  Duration(new FunctionDataType("Duration", BigDecimal.class,
    EdmPrimitiveTypeKind::stringToDuration, EdmPrimitiveTypeKind::durationToString)), //
  Decimal(DataTypes.DECIMAL), //
  Single(DataTypes.FLOAT), //
  Double(DataTypes.DOUBLE), //
  Guid(DataTypes.UUID), //
  Int16(DataTypes.SHORT), //
  Int32(DataTypes.INT), //
  Int64(DataTypes.LONG), //
  String(DataTypes.STRING), //
  Stream(DataTypes.ANY_URI), //
  Geography(new FunctionDataType("Geography", Geometry.class, v -> {
    throw new EdmPrimitiveTypeException("Not implemented!");
  }, v ->

  {
    throw new EdmPrimitiveTypeException("Not implemented!");
  })), //
  GeographyPoint(new FunctionDataType("GeographyPoint", Point.class,
    EdmGeometryFactory.GEOGRAPHY::stringToPoint, EdmGeometryFactory.GEOGRAPHY::pointToString)), //
  GeographyLineString(new FunctionDataType("GeographyLineString", LineString.class,
    EdmGeometryFactory.GEOGRAPHY::stringToLineString,
    EdmGeometryFactory.GEOGRAPHY::lineStringToString)), //
  GeographyPolygon(new FunctionDataType("GeographyPolygon", Polygon.class,
    EdmGeometryFactory.GEOGRAPHY::stringToPolygon, EdmGeometryFactory.GEOGRAPHY::polygonToString)), //
  GeographyMultiPoint(new FunctionDataType("GeographyMultiPoint", MultiPoint.class,
    EdmGeometryFactory.GEOGRAPHY::stringToMultiPoint,
    EdmGeometryFactory.GEOGRAPHY::multiPointToString)), //
  GeographyMultiLineString(new FunctionDataType("GeographyMultiLineString", MultiLineString.class,
    EdmGeometryFactory.GEOGRAPHY::stringToMultiLineString,
    EdmGeometryFactory.GEOGRAPHY::multiLineStringToString)), //
  GeographyMultiPolygon(new FunctionDataType("GeographyMultiPolygon", MultiPolygon.class,
    EdmGeometryFactory.GEOGRAPHY::stringToMultiPolygon,
    EdmGeometryFactory.GEOGRAPHY::multiPolygonToString)), //
  GeographyCollection(new FunctionDataType("GeographyGeospatialCollection",
    GeometryCollection.class, EdmGeometryFactory.GEOGRAPHY::stringToCollection,
    EdmGeometryFactory.GEOGRAPHY::collectionToString)), //

  Geometry(new FunctionDataType("Geometry", Geometry.class, v -> {
    throw new EdmPrimitiveTypeException("Not implemented!");
  }, v ->

  {
    throw new EdmPrimitiveTypeException("Not implemented!");
  })), //
  GeometryPoint(new FunctionDataType("GeometryPoint", Point.class,
    EdmGeometryFactory.GEOMETRY::stringToPoint, EdmGeometryFactory.GEOMETRY::pointToString)), //
  GeometryLineString(new FunctionDataType("GeometryLineString", LineString.class,
    EdmGeometryFactory.GEOMETRY::stringToLineString,
    EdmGeometryFactory.GEOMETRY::lineStringToString)), //
  GeometryPolygon(new FunctionDataType("GeometryPolygon", Polygon.class,
    EdmGeometryFactory.GEOMETRY::stringToPolygon, EdmGeometryFactory.GEOMETRY::polygonToString)), //
  GeometryMultiPoint(new FunctionDataType("GeometryMultiPoint", MultiPoint.class,
    EdmGeometryFactory.GEOMETRY::stringToMultiPoint,
    EdmGeometryFactory.GEOMETRY::multiPointToString)), //
  GeometryMultiLineString(new FunctionDataType("GeometryMultiLineString", MultiLineString.class,
    EdmGeometryFactory.GEOMETRY::stringToMultiLineString,
    EdmGeometryFactory.GEOMETRY::multiLineStringToString)), //
  GeometryMultiPolygon(new FunctionDataType("GeometryMultiPolygon", MultiPolygon.class,
    EdmGeometryFactory.GEOMETRY::stringToMultiPolygon,
    EdmGeometryFactory.GEOMETRY::multiPolygonToString)), //
  GeometryCollection(new FunctionDataType("GeometryGeospatialCollection", GeometryCollection.class,
    EdmGeometryFactory.GEOMETRY::stringToCollection,
    EdmGeometryFactory.GEOMETRY::collectionToString)), //

  Untyped(DataTypes.OBJECT);

  private static final Pattern PATTERN = Pattern
    .compile("[-+]?P(?:(\\p{Digit}+)D)?(?:T(?:(\\p{Digit}+)H)?(?:(\\p{Digit}+)M)?"
      + "(?:(\\p{Digit}+(?:\\.(?:\\p{Digit}+?)0*)?)S)?)?");

  private static Map<String, EdmPrimitiveTypeKind> VALUES_BY_NAME;

  static {
    final Map<String, EdmPrimitiveTypeKind> valuesByName = new HashMap<>();
    for (final EdmPrimitiveTypeKind value : values()) {
      valuesByName.put(value.name(), value);
    }
    VALUES_BY_NAME = Collections.unmodifiableMap(valuesByName);
  }

  private static String durationToString(final Object value) {

    BigDecimal valueDecimal;
    if (value instanceof BigDecimal) {
      valueDecimal = (BigDecimal)value;
    } else if (value instanceof Byte || value instanceof Short || value instanceof Integer
      || value instanceof Long) {
      valueDecimal = BigDecimal.valueOf(((Number)value).longValue());
    } else if (value instanceof BigInteger) {
      valueDecimal = new BigDecimal((BigInteger)value);
    } else {
      throw new EdmPrimitiveTypeException(
        "The value type " + value.getClass() + " is not supported.");
    }

    final StringBuilder result = new StringBuilder();
    if (valueDecimal.signum() == -1) {
      result.append('-');
      valueDecimal = valueDecimal.negate();
    }
    result.append('P');
    BigInteger seconds = valueDecimal.toBigInteger();
    final BigInteger days = seconds.divide(BigInteger.valueOf(24 * 60 * 60));
    if (!days.equals(BigInteger.ZERO)) {
      result.append(days.toString());
      result.append('D');
    }
    result.append('T');
    seconds = seconds.subtract(days.multiply(BigInteger.valueOf(24 * 60 * 60)));
    final BigInteger hours = seconds.divide(BigInteger.valueOf(60 * 60));
    if (!hours.equals(BigInteger.ZERO)) {
      result.append(hours.toString());
      result.append('H');
    }
    seconds = seconds.subtract(hours.multiply(BigInteger.valueOf(60 * 60)));
    final BigInteger minutes = seconds.divide(BigInteger.valueOf(60));
    if (!minutes.equals(BigInteger.ZERO)) {
      result.append(minutes.toString());
      result.append('M');
    }
    result.append(valueDecimal.remainder(BigDecimal.valueOf(60))
      .toPlainString());
    result.append('S');

    return result.toString();
  }

  /**
   * Get a type kind by name.
   *
   * @param name The name.
   * @return The type kind or <tt>null</tt> if it does not exist.
   */
  public static EdmPrimitiveTypeKind getByName(final String name) {
    return VALUES_BY_NAME.get(name);
  }

  private static Object stringToDuration(final Object value) throws EdmPrimitiveTypeException {
    if (value instanceof final BigDecimal number) {
      return number;
    } else {
      final var s = value.toString();
      final Matcher matcher = PATTERN.matcher(s);
      if (!matcher.matches() || matcher.group(1) == null && matcher.group(2) == null
        && matcher.group(3) == null && matcher.group(4) == null) {
        throw new EdmPrimitiveTypeException("The literal '" + value + "' has illegal content.");
      }

      BigDecimal result = (matcher.group(1) == null ? BigDecimal.ZERO
        : new BigDecimal(matcher.group(1)).multiply(BigDecimal.valueOf(24 * 60 * 60)))
        .add(matcher.group(2) == null ? BigDecimal.ZERO
          : new BigDecimal(matcher.group(2)).multiply(BigDecimal.valueOf(60 * 60)))
        .add(matcher.group(3) == null ? BigDecimal.ZERO
          : new BigDecimal(matcher.group(3)).multiply(BigDecimal.valueOf(60)))
        .add(matcher.group(4) == null ? BigDecimal.ZERO : new BigDecimal(matcher.group(4)));

      result = s.charAt(0) == '-' ? result.negate() : result;

      return result;
    }
  }

  /**
   * Gets the {@link EdmPrimitiveTypeKind} from a full-qualified type name.
   * @param pathName full-qualified type name
   * @return {@link EdmPrimitiveTypeKind} object
   */
  public static EdmPrimitiveTypeKind valueOfFQN(final PathName pathName) {
    if (EdmPrimitiveType.EDM_NAMESPACE.equals(pathName.getParent())) {
      return valueOf(pathName.getName());
    } else {
      throw new IllegalArgumentException(pathName + " does not look like an EDM primitive type.");
    }
  }

  /**
   * Gets the {@link EdmPrimitiveTypeKind} from a full type expression (like <code>Edm.Int32</code>).
   * @param fqn String containing a full-qualified type name
   * @return {@link EdmPrimitiveTypeKind} object
   */
  public static EdmPrimitiveTypeKind valueOfFQN(final String fqn) {
    if (!fqn.startsWith(EdmPrimitiveType.EDM_NAMESPACE + ".")) {
      throw new IllegalArgumentException(fqn + " does not look like an Edm primitive type");
    }

    return valueOf(fqn.substring(4));
  }

  private final DataType dataType;

  private final PathName pathName;

  private final boolean geo;

  private EdmPrimitiveTypeKind(final DataType dataType) {
    this.pathName = EdmPrimitiveType.EDM_NAMESPACE.newChild(name());
    this.dataType = dataType;
    this.geo = Geometry.class.isAssignableFrom(getDefaultType());
  }

  private String fromUri(final String literal, final String prefix, final String suffix) {
    if (literal.startsWith(prefix) && literal.endsWith(suffix)) {
      return literal.substring(prefix.length(), literal.length() - suffix.length());
    } else {
      throw new EdmPrimitiveTypeException("The literal '" + literal + "' has illegal content.");
    }
  }

  @Override
  public String fromUriLiteral(final String literal) throws EdmPrimitiveTypeException {
    if (literal == null) {
      return null;
    } else {
      return switch (this) {
        case Binary -> fromUri(literal, "binary'", "'");
        case Duration -> fromUri(literal, "duration'", "'");
        case String -> fromUri(literal, "'", "'").replace("''", "'");
        default -> literal;
      };
    }
  }

  @Override
  public DataType getDataType() {
    return this.dataType;
  }

  @Override
  public Class<?> getDefaultType() {
    return this.dataType.getJavaClass();
  }

  public EdmPrimitiveType getInstance() {
    return this;
  }

  @Override
  public EdmTypeKind getKind() {
    return EdmTypeKind.PRIMITIVE;
  }

  @Override
  public String getName() {
    return name();
  }

  @Override
  public PathName getNamespace() {
    return EdmPrimitiveType.EDM_NAMESPACE;
  }

  /**
   * Returns the {@link PathName} for this type kind.
   *
   * @return {@link PathName}
   */
  @Override
  public PathName getPathName() {
    return this.pathName;
  }

  @Override
  public boolean isCompatible(final EdmPrimitiveType primitiveType) {
    if (primitiveType instanceof final EdmPrimitiveTypeKind kind) {
      return isCompatible(kind);
    }
    return false;
  }

  public boolean isCompatible(final EdmPrimitiveTypeKind primitiveType) {
    if (primitiveType == this) {
      return true;
    }
    return switch (this) {
      case Decimal:
      case Double: {
        switch (primitiveType) {
          case Byte:
          case SByte:
          case Int16:
          case Int32:
          case Int64:
          case Single:
          case Double: {
            yield true;
          }
          default:
          yield false;
        }
      }
      case Int16: {
        switch (primitiveType) {
          case Byte:
          case SByte:
          case Int16: {
            yield true;
          }
          default:
          yield false;
        }
      }
      case Int32: {
        switch (primitiveType) {
          case Byte:
          case SByte:
          case Int16:
          case Int64: {
            yield true;
          }
          default:
          yield false;
        }
      }
      case Int64: {
        switch (primitiveType) {
          case Byte:
          case SByte:
          case Int16:
          case Int32:
          case Int64: {
            yield true;
          }
          default:
          yield false;
        }
      }
      case Single: {
        switch (primitiveType) {
          case Byte:
          case SByte:
          case Int16:
          case Int32:
          case Int64:
          case Single: {
            yield true;
          }
          default:
          yield false;
        }
      }
      case Untyped:
      yield true;
      default:
        throw new IllegalArgumentException("Unexpected value: " + primitiveType);
    };
  }

  @Override
  public boolean isGeospatial() {
    return this.geo;
  }

  @Override
  public boolean isPrimitive() {
    return true;
  }

  @Override
  public String toUriLiteral(final String literal) {
    if (literal == null) {
      return null;
    } else {
      return switch (this) {
        case Binary: {
          yield new StringBuilder("binary'").append(literal)
            .append('\'')
            .toString();
        }
        case Duration: {
          yield new StringBuilder("duration'").append(literal)
            .append('\'')
            .toString();
        }
        case String: {
          final int length = literal.length();

          final StringBuilder uriLiteral = new StringBuilder(length + 2);
          uriLiteral.append('\'');
          for (int i = 0; i < length; i++) {
            final char c = literal.charAt(i);
            if (c == '\'') {
              uriLiteral.append(c);
            }
            uriLiteral.append(c);
          }
          uriLiteral.append('\'');
          yield uriLiteral.toString();
        }
        default: {
          yield literal;
        }
      };
    }
  }

  @Override
  public boolean validate(final String value, final Boolean isNullable, final Integer maxLength,
    final Integer precision, final Integer scale) {
    try {
      this.dataType.toObject(value);
    } catch (final RuntimeException e) {
      return false;
    }
    return true;
  }

  @Override
  public Object valueOfString(final String value, final Boolean isNullable, final Integer maxLength,
    final Integer precision, final Integer scale) throws EdmPrimitiveTypeException {
    return this.dataType.toObject(value);
  }

  @Override
  public String valueToString(final Object value) throws EdmPrimitiveTypeException {
    return this.dataType.toString(value);
  }

}
