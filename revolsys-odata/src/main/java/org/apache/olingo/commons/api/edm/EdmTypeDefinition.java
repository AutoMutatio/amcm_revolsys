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

import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.edm.provider.CsdlTypeDefinition;
import org.apache.olingo.commons.core.edm.Edm;
import org.apache.olingo.commons.core.edm.EdmTypeImpl;

import com.revolsys.data.type.DataType;
import com.revolsys.io.PathName;

/**
 * An {@link EdmTypeDefinition} defines a specialization of one of the possible primitive types.
 * <br/>
 * For more information on primitive types refer to {@link EdmPrimitiveType}.
 */
public class EdmTypeDefinition extends EdmTypeImpl implements EdmPrimitiveType, EdmAnnotatable {

  private final CsdlTypeDefinition typeDefinition;

  private EdmPrimitiveType edmPrimitiveTypeInstance;

  public EdmTypeDefinition(final Edm edm, final PathName typeDefinitionName,
    final CsdlTypeDefinition typeDefinition) {
    super(edm, typeDefinitionName, EdmTypeKind.DEFINITION, typeDefinition);
    this.typeDefinition = typeDefinition;
  }

  @Override
  public String fromUriLiteral(final String literal) throws EdmPrimitiveTypeException {
    return getUnderlyingType().fromUriLiteral(literal);
  }

  @Override
  public DataType getDataType() {
    return getUnderlyingType().getDataType();
  }

  @Override
  public Class<?> getDefaultType() {
    return getUnderlyingType().getDefaultType();
  }

  public Integer getMaxLength() {
    return this.typeDefinition.getMaxLength();
  }

  public Integer getPrecision() {
    return this.typeDefinition.getPrecision();
  }

  public Integer getScale() {
    return this.typeDefinition.getScale();
  }

  public int getSrid() {
    return this.typeDefinition.getSrid();
  }

  public EdmPrimitiveType getUnderlyingType() {
    if (this.edmPrimitiveTypeInstance == null) {
      try {
        if (this.typeDefinition.getUnderlyingType() == null) {
          throw new EdmException("Underlying Type for type definition: " + this.typeName.toString()
            + " must not be null.");
        }
        this.edmPrimitiveTypeInstance = EdmPrimitiveTypeKind
          .valueOfFQN(this.typeDefinition.getUnderlyingType());
      } catch (final IllegalArgumentException e) {
        throw new EdmException(
          "Invalid underlying type: " + this.typeDefinition.getUnderlyingType(), e);
      }
    }
    return this.edmPrimitiveTypeInstance;
  }

  @Override
  public boolean isCompatible(final EdmPrimitiveType primitiveType) {
    return this == primitiveType || getUnderlyingType().isCompatible(primitiveType);
  }

  @Override
  public String toUriLiteral(final String literal) {
    return getUnderlyingType().toUriLiteral(literal);
  }

  @Override
  public boolean validate(final String value, final Boolean isNullable, final Integer maxLength,
    final Integer precision, final Integer scale) {
    return getUnderlyingType().validate(value, isNullable,
      maxLength == null ? getMaxLength() : maxLength,
      precision == null ? getPrecision() : precision, scale == null ? getScale() : scale);
  }

  @Override
  public Object valueOfString(final String value, final Boolean isNullable, final Integer maxLength,
    final Integer precision, final Integer scale) throws EdmPrimitiveTypeException {
    return getUnderlyingType().valueOfString(value, isNullable,
      maxLength == null ? getMaxLength() : maxLength,
      precision == null ? getPrecision() : precision, scale == null ? getScale() : scale);
  }

  @Override
  public String valueToString(final Object value) throws EdmPrimitiveTypeException {
    return getUnderlyingType().valueToString(value);
  }
}
