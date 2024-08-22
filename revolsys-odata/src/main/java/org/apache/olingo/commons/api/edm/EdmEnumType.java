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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.edm.provider.CsdlEnumMember;
import org.apache.olingo.commons.api.edm.provider.CsdlEnumType;
import org.apache.olingo.commons.core.edm.Edm;
import org.apache.olingo.commons.core.edm.EdmMemberImpl;
import org.apache.olingo.commons.core.edm.EdmTypeImpl;

import com.revolsys.data.type.DataType;
import com.revolsys.io.PathName;

/**
 * An EdmEnumType represents a set of related values.
 */
public class EdmEnumType extends EdmTypeImpl implements EdmPrimitiveType {

  private EdmPrimitiveType underlyingType;

  private final CsdlEnumType enumType;

  private final PathName enumName;

  private List<String> memberNames;

  private Map<String, EdmMember> membersMap;

  public EdmEnumType(final Edm edm, final PathName enumName, final CsdlEnumType enumType) {
    super(edm, enumName, EdmTypeKind.ENUM, enumType);

    if (enumType.getUnderlyingType() == null) {
      this.underlyingType = EdmPrimitiveTypeKind.Int32;
    } else {
      final EdmPrimitiveTypeKind underlyingTypeKind = EdmPrimitiveTypeKind
        .valueOfFQN(enumType.getUnderlyingType());
      if (underlyingTypeKind == EdmPrimitiveTypeKind.Byte
        || underlyingTypeKind == EdmPrimitiveTypeKind.SByte
        || underlyingTypeKind == EdmPrimitiveTypeKind.Int16
        || underlyingTypeKind == EdmPrimitiveTypeKind.Int32
        || underlyingTypeKind == EdmPrimitiveTypeKind.Int64) {
        this.underlyingType = underlyingTypeKind;
      } else {
        throw new EdmException("Not allowed as underlying type: " + underlyingTypeKind);
      }
    }

    this.enumType = enumType;
    this.enumName = enumName;
  }

  private String constructEnumValue(final long value) throws EdmPrimitiveTypeException {
    long remaining = value;
    final StringBuilder result = new StringBuilder();

    final boolean flags = isFlags();
    long memberValue = -1;
    for (final EdmMember member : getMembers()) {
      memberValue = member.getValue() == null ? memberValue + 1 : Long.parseLong(member.getValue());
      if (flags) {
        if ((memberValue & remaining) == memberValue) {
          if (result.length() > 0) {
            result.append(',');
          }
          result.append(member.getName());
          remaining ^= memberValue;
        }
      } else {
        if (value == memberValue) {
          return member.getName();
        }
      }
    }

    if (remaining != 0) {
      throw new EdmPrimitiveTypeException("The value '" + value + "' is not valid.");
    }
    return result.toString();
  }

  /**
   * Creates the map from member names to member objects,
   * preserving the order for the case of implicit value assignments.
   */
  private void createEdmMembers() {
    final Map<String, EdmMember> membersMapLocal = new LinkedHashMap<>();
    final List<String> memberNamesLocal = new ArrayList<>();
    if (this.enumType.getMembers() != null) {
      for (final CsdlEnumMember member : this.enumType.getMembers()) {
        membersMapLocal.put(member.getName(), new EdmMemberImpl(getEdm(), member));
        memberNamesLocal.add(member.getName());
      }

      this.membersMap = membersMapLocal;
      this.memberNames = memberNamesLocal;
    }
  }

  @Override
  public boolean equals(final Object obj) {
    return obj != null && (obj == this
      || obj instanceof EdmEnumType && getPathName().equals(((EdmEnumType)obj).getPathName()));
  }

  @Override
  public String fromUriLiteral(final String literal) throws EdmPrimitiveTypeException {
    if (literal == null) {
      return null;
    } else {
      final String uriPrefix = this.enumName.toString() + "'";
      final String uriSuffix = "'";
      if (literal.length() >= uriPrefix.length() + uriSuffix.length()
        && literal.startsWith(uriPrefix) && literal.endsWith(uriSuffix)) {
        // This is the positive case where the literal is prefixed with the full
        // qualified name of the enum type
        return literal.substring(uriPrefix.length(), literal.length() - uriSuffix.length());
      } else {
        // This case will be called if the prefix might be an alias
        if (literal.endsWith(uriSuffix)) {
          final int indexSingleQuote = literal.indexOf('\'');
          final String fqn = literal.substring(0, indexSingleQuote);
          PathName typeFqn = null;
          try {
            typeFqn = PathName.fromDotSeparated(fqn);
          } catch (final IllegalArgumentException e) {
            throw new EdmPrimitiveTypeException(
              "The literal '" + literal + "' has illegal content.", e);
          }
          /*
           * Get itself. This will also resolve a possible alias. If we had an
           * easier way to query the edm for an alias we could use this here.
           * But since there is no such method we try to get the enum type based
           * on a possible alias qualified name. This way the edm will resolve
           * the alias for us. Also in a positive case the type is already
           * cached so the EdmProvider should not be called.
           */
          final EdmEnumType prospect = getEdm().getEnumType(typeFqn);
          if (prospect != null && this.enumName.equals(prospect.getPathName())
            && literal.length() >= fqn.length() + 2) {
            return literal.substring(fqn.length() + 1, literal.length() - 1);
          }
        }
      }
    }
    throw new EdmPrimitiveTypeException("The literal '" + literal + "' has illegal content.");
  }

  @Override
  public DataType getDataType() {
    return getUnderlyingType().getDataType();
  }

  @Override
  public Class<?> getDefaultType() {
    return getUnderlyingType().getDefaultType();
  }

  public EdmMember getMember(final String name) {
    if (this.membersMap == null) {
      createEdmMembers();
    }
    return this.membersMap.get(name);
  }

  public List<String> getMemberNames() {
    if (this.memberNames == null) {
      createEdmMembers();
    }
    return Collections.unmodifiableList(this.memberNames);
  }

  private Collection<EdmMember> getMembers() {
    if (this.membersMap == null) {
      createEdmMembers();
    }
    return this.membersMap.values();
  }

  public EdmPrimitiveType getUnderlyingType() {
    return this.underlyingType;
  }

  @Override
  public int hashCode() {
    return getPathName().toString()
      .hashCode();
  }

  @Override
  public boolean isCompatible(final EdmPrimitiveType primitiveType) {
    return equals(primitiveType);
  }

  @Override
  public boolean isEnum() {
    return true;
  }

  public boolean isFlags() {
    return this.enumType.isFlags();
  }

  private Long parseEnumValue(final String value) throws EdmPrimitiveTypeException {
    Long result = null;
    for (final String memberValue : value.split(",", isFlags() ? -1 : 1)) {
      Long memberValueLong = null;
      long count = 0;
      for (final EdmMember member : getMembers()) {
        count++;
        if (memberValue.equals(member.getName()) || memberValue.equals(member.getValue())) {
          memberValueLong = member.getValue() == null ? count - 1 : Long.decode(member.getValue());
        }
      }
      if (memberValueLong == null) {
        throw new EdmPrimitiveTypeException("The literal '" + value + "' has illegal content.");
      }
      result = result == null ? memberValueLong : result | memberValueLong;
    }
    return result;
  }

  @Override
  public String toUriLiteral(final String literal) {
    if (literal == null) {
      return null;
    } else {
      return this.enumName.toString() + "'" + literal + "'";
    }
  }

  @Override
  public boolean validate(final String value, final Boolean isNullable, final Integer maxLength,
    final Integer precision, final Integer scale) {

    try {
      valueOfString(value, isNullable, maxLength, precision, scale);
      return true;
    } catch (final EdmPrimitiveTypeException e) {
      return false;
    }
  }

  @Override
  public Object valueOfString(final String value, final Boolean isNullable, final Integer maxLength,
    final Integer precision, final Integer scale) throws EdmPrimitiveTypeException {

    if (value == null) {
      if (isNullable != null && !isNullable) {
        throw new EdmPrimitiveTypeException("The literal 'null' is not allowed.");
      }
      return null;
    }

    try {
      return getUnderlyingType().getDataType()
        .toObject(parseEnumValue(value));
    } catch (final IllegalArgumentException e) {
      throw new EdmPrimitiveTypeException(
        "The literal '" + value + "' cannot be converted to value type " + getDataType() + ".", e);
    } catch (final ClassCastException e) {
      throw new EdmPrimitiveTypeException("The value type " + getDataType() + " is not supported.",
        e);
    }
  }

  @Override
  public String valueToString(final Object value) throws EdmPrimitiveTypeException {
    if (value instanceof Byte || value instanceof Short || value instanceof Integer
      || value instanceof Long) {
      return constructEnumValue(((Number)value).longValue());
    } else {
      throw new EdmPrimitiveTypeException(
        "The value type " + value.getClass() + " is not supported.");
    }
  }
}
