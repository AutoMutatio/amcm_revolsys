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

import org.apache.olingo.server.api.uri.queryoption.search.SearchTerm;
import org.apache.olingo.server.api.uri.queryoption.search.SearchUnary;
import org.apache.olingo.server.api.uri.queryoption.search.SearchUnaryOperatorKind;

public class SearchUnaryImpl extends SearchExpressionImpl implements SearchUnary {
  private final SearchTerm operand;

  public SearchUnaryImpl(final SearchTerm operand) {
    this.operand = operand;
  }

  @Override
  public SearchTerm getOperand() {
    return this.operand;
  }

  @Override
  public SearchUnaryOperatorKind getOperator() {
    return SearchUnaryOperatorKind.NOT;
  }

  @Override
  public String toString() {
    return "{" + getOperator().name() + " " + this.operand + '}';
  }
}
