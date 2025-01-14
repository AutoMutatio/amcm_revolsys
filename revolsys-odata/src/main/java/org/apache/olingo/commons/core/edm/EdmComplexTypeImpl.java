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

import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;

import com.revolsys.io.PathName;

public class EdmComplexTypeImpl extends AbstractEdmStructuredType implements EdmComplexType {

  public EdmComplexTypeImpl(final Edm edm, final PathName name, final CsdlComplexType complexType) {
    super(edm, name, EdmTypeKind.COMPLEX, complexType);
  }

  @Override
  protected EdmStructuredType buildBaseType(final PathName baseTypeName) {
    EdmComplexType baseType = null;
    if (baseTypeName != null) {
      baseType = getEdm().getComplexType(baseTypeName);
      if (baseType == null) {
        throw new EdmException(
          "Can't find base type with name: " + baseTypeName + " for complex type: " + getName());
      }
    }
    return baseType;
  }

  @Override
  protected void checkBaseType() {
    if (this.baseTypeName != null && this.baseType == null) {
      this.baseType = buildBaseType(this.baseTypeName);
    }
  }

  @Override
  public EdmComplexType getBaseType() {
    checkBaseType();
    return (EdmComplexType)this.baseType;
  }
}
