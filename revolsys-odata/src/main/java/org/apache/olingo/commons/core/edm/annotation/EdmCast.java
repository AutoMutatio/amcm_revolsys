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
package org.apache.olingo.commons.core.edm.annotation;

import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.annotation.EdmExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlCast;
import org.apache.olingo.commons.core.edm.Edm;
import org.apache.olingo.commons.core.edm.EdmTypeInfo;

public class EdmCast extends AbstractEdmAnnotatableDynamicExpression {

  private final CsdlCast cast;

  private EdmExpression value;

  private EdmType type;

  public EdmCast(final Edm edm, final CsdlCast csdlExp) {
    super(edm, "Cast", csdlExp);
    this.cast = csdlExp;
  }

  @Override
  public EdmExpressionType getExpressionType() {
    return EdmExpressionType.Cast;
  }

  public Integer getMaxLength() {
    return this.cast.getMaxLength();
  }

  public Integer getPrecision() {
    return this.cast.getPrecision();
  }

  public Integer getScale() {
    return this.cast.getScale();
  }

  public int getSrid() {
    return this.cast.getSrid();
  }

  public EdmType getType() {
    if (this.type == null) {
      if (this.cast.getType() == null) {
        throw new EdmException("Must specify a type for a Cast expression.");
      }
      final EdmTypeInfo typeInfo = new EdmTypeInfo.Builder().setEdm(this.edm)
        .setTypeExpression(this.cast.getType())
        .build();
      this.type = typeInfo.getType();
    }
    return this.type;
  }

  public EdmExpression getValue() {
    if (this.value == null) {
      if (this.cast.getValue() == null) {
        throw new EdmException("Cast expressions require an expression value.");
      }
      this.value = this.cast.getValue()
        .toEdm(this.edm);
    }
    return this.value;
  }
}
