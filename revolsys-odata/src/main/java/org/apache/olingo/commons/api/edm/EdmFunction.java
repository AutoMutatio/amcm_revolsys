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
import org.apache.olingo.commons.api.edm.provider.CsdlFunction;
import org.apache.olingo.commons.core.edm.AbstractEdmOperation;
import org.apache.olingo.commons.core.edm.Edm;

import com.revolsys.io.PathName;

/**
 * An EdmFunction as described in the OData specification
 */
public class EdmFunction extends AbstractEdmOperation<CsdlFunction> {

  private final CsdlFunction function;

  public EdmFunction(final Edm edm, final PathName name, final CsdlFunction function) {
    super(edm, name, function, EdmTypeKind.FUNCTION);
    this.function = function;
  }

  public CsdlFunction getFunction() {
    return this.function;
  }

  @Override
  public EdmReturnType getReturnType() {
    final EdmReturnType returnType = super.getReturnType();
    if (returnType == null) {
      throw new EdmException("ReturnType for a function must not be null");
    }
    return returnType;
  }

  public boolean isComposable() {
    return this.function.isComposable();
  }

}
