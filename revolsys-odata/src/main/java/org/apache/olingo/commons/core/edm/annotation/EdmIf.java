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
import org.apache.olingo.commons.api.edm.annotation.EdmExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlIf;
import org.apache.olingo.commons.core.edm.Edm;

public class EdmIf extends AbstractEdmAnnotatableDynamicExpression {

  private EdmExpression guard;

  private EdmExpression _then;

  private EdmExpression _else;

  private final CsdlIf csdlExp;

  public EdmIf(final Edm edm, final CsdlIf csdlExp) {
    super(edm, "If", csdlExp);
    this.csdlExp = csdlExp;
  }

  public EdmExpression getElse() {
    // The else clause might be null in certain conditions so we can`t evaluate
    // this here.
    if (this._else == null && this.csdlExp.getElse() != null) {
      this._else = this.csdlExp.getElse()
        .toEdm(this.edm);
    }
    return this._else;
  }

  @Override
  public EdmExpressionType getExpressionType() {
    return EdmExpressionType.If;
  }

  public EdmExpression getGuard() {
    if (this.guard == null) {
      if (this.csdlExp.getGuard() == null) {
        throw new EdmException("Guard clause of an if expression must not be null");
      }
      this.guard = this.csdlExp.getGuard()
        .toEdm(this.edm);
    }
    return this.guard;
  }

  public EdmExpression getThen() {
    if (this._then == null) {
      if (this.csdlExp.getThen() == null) {
        throw new EdmException("Then clause of an if expression must not be null");
      }
      this._then = this.csdlExp.getThen()
        .toEdm(this.edm);
    }
    return this._then;
  }
}
