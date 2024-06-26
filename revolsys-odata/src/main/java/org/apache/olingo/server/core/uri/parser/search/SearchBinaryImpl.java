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
package org.apache.olingo.server.core.uri.parser.search;

import org.apache.olingo.server.api.uri.queryoption.search.SearchBinary;
import org.apache.olingo.server.api.uri.queryoption.search.SearchBinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.search.SearchExpression;

public class SearchBinaryImpl extends SearchExpressionImpl implements SearchBinary {

  private final SearchBinaryOperatorKind operator;

  private final SearchExpression left;

  private final SearchExpression right;

  public SearchBinaryImpl(final SearchExpression left, final SearchBinaryOperatorKind operator,
    final SearchExpression right) {
    this.left = left;
    this.operator = operator;
    this.right = right;
  }

  @Override
  public SearchExpression getLeftOperand() {
    return this.left;
  }

  @Override
  public SearchBinaryOperatorKind getOperator() {
    return this.operator;
  }

  @Override
  public SearchExpression getRightOperand() {
    return this.right;
  }

  @Override
  public String toString() {
    return "{" + this.left + " " + this.operator.name() + " " + this.right + '}';
  }
}
