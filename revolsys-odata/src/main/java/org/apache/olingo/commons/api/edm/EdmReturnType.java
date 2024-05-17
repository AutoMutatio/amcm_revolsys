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

import org.apache.olingo.commons.api.edm.provider.CsdlReturnType;
import org.apache.olingo.commons.core.edm.Edm;
import org.apache.olingo.commons.core.edm.EdmTypeInfo;

/**
 * An {@link EdmReturnType} of an {@link EdmOperation}.
 */
public class EdmReturnType implements EdmTyped {

  private final CsdlReturnType returnType;

  private final Edm edm;

  private EdmType typeImpl;

  public EdmReturnType(final Edm edm, final CsdlReturnType returnType) {
    this.edm = edm;
    this.returnType = returnType;
  }

  public Integer getMaxLength() {
    return this.returnType.getMaxLength();
  }

  public Integer getPrecision() {
    return this.returnType.getPrecision();
  }

  public Integer getScale() {
    return this.returnType.getScale();
  }

  public int getSrid() {
    return this.returnType.getSrid();
  }

  @Override
  public EdmType getType() {
    if (this.typeImpl == null) {
      if (this.returnType.getType() == null) {
        throw new EdmException("Return types must hava a full qualified type.");
      }
      this.typeImpl = new EdmTypeInfo.Builder().setEdm(this.edm)
        .setTypeExpression(this.returnType.getType())
        .build()
        .getType();
      if (this.typeImpl == null) {
        throw new EdmException("Cannot find type with name: " + this.returnType.getType());
      }
    }

    return this.typeImpl;
  }

  @Override
  public boolean isCollection() {
    return this.returnType.isCollection();
  }

  public boolean isNullable() {
    return this.returnType.isNullable();
  }

  @Override
  public String toString() {
    return this.returnType.toString();
  }
}
