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
package org.apache.olingo.server.core.uri.queryoption.expression;

import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor;
import org.apache.olingo.server.api.uri.queryoption.expression.Unary;
import org.apache.olingo.server.api.uri.queryoption.expression.UnaryOperatorKind;

public class UnaryImpl implements Unary {

  private final UnaryOperatorKind operator;

  private final Expression expression;

  private final EdmType type;

  public UnaryImpl(final UnaryOperatorKind operator, final Expression expression,
    final EdmType type) {
    this.operator = operator;
    this.expression = expression;
    this.type = type;
  }

  @Override
  public <T> T accept(final ExpressionVisitor<T> visitor)
    throws ExpressionVisitException, ODataApplicationException {
    final T operand = this.expression.accept(visitor);
    return visitor.visitUnaryOperator(this.operator, operand);
  }

  @Override
  public Expression getOperand() {
    return this.expression;
  }

  @Override
  public UnaryOperatorKind getOperator() {
    return this.operator;
  }

  public EdmType getType() {
    return this.type;
  }

  @Override
  public String toString() {
    return "{" + this.operator.name() + " " + this.expression + '}';
  }
}
