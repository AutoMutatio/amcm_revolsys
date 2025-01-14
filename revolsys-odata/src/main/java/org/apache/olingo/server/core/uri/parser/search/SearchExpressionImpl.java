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
import org.apache.olingo.server.api.uri.queryoption.search.SearchExpression;
import org.apache.olingo.server.api.uri.queryoption.search.SearchTerm;
import org.apache.olingo.server.api.uri.queryoption.search.SearchUnary;

public abstract class SearchExpressionImpl implements SearchExpression {

  @Override
  public SearchBinary asSearchBinary() {
    return isSearchBinary() ? (SearchBinary)this : null;
  }

  @Override
  public SearchTerm asSearchTerm() {
    return isSearchTerm() ? (SearchTerm)this : null;
  }

  @Override
  public SearchUnary asSearchUnary() {
    return isSearchUnary() ? (SearchUnary)this : null;
  }

  @Override
  public boolean isSearchBinary() {
    return this instanceof SearchBinary;
  }

  @Override
  public boolean isSearchTerm() {
    return this instanceof SearchTerm;
  }

  @Override
  public boolean isSearchUnary() {
    return this instanceof SearchUnary;
  }

}
