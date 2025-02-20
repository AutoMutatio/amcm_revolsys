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
package org.apache.olingo.commons.api.edm.provider.annotation;

import org.apache.olingo.commons.api.edm.annotation.EdmExpression;
import org.apache.olingo.commons.api.edm.provider.CsdlAbstractEdmItem;
import org.apache.olingo.commons.core.edm.Edm;

public abstract class CsdlExpression implements CsdlAbstractEdmItem {

  /**
   * Casts the expression to {@link CsdlConstantExpression}.
   * @return Constant Expression
   */
  public CsdlConstantExpression asConstant() {
    return isConstant() ? (CsdlConstantExpression)this : null;
  }

  /**
   * Cast the expression to {@link CsdlDynamicExpression}.
   * @return Dynamic Expression
   */
  public CsdlDynamicExpression asDynamic() {
    return isDynamic() ? (CsdlDynamicExpression)this : null;
  }

  /**
   * Return true if the expression is constant
   * @return true if the expression is constant
   */
  public boolean isConstant() {
    return this instanceof CsdlConstantExpression;
  }

  /**
   * Returns true if the expression is dynamic.
   * @return true if the expression is dynamic
   */
  public boolean isDynamic() {
    return this instanceof CsdlDynamicExpression;
  }

  public abstract EdmExpression toEdm(Edm edm);
}
