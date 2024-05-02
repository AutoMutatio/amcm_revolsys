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

import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotatable;

import com.revolsys.io.PathName;

public class EdmTypeImpl extends AbstractEdmNamed implements EdmType {

  protected final PathName typeName;

  protected final EdmTypeKind kind;

  public EdmTypeImpl(final Edm edm, final PathName typeName, final EdmTypeKind kind,
    final CsdlAnnotatable annotatable) {
    super(edm, typeName.getName(), annotatable);
    this.typeName = typeName;
    this.kind = kind;
  }

  @Override
  public PathName getPathName() {
    return this.typeName;
  }

  @Override
  public EdmTypeKind getKind() {
    return this.kind;
  }

  @Override
  public PathName getNamespace() {
    return this.typeName.getParent();
  }
}
