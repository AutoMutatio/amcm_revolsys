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
package org.apache.olingo.server.core.uri.queryoption;

import org.apache.olingo.server.api.uri.queryoption.SystemQueryOption;
import org.apache.olingo.server.api.uri.queryoption.SystemQueryOptionKind;

public abstract class SystemQueryOptionImpl extends QueryOptionImpl implements SystemQueryOption {

  private SystemQueryOptionKind kind;

  @Override
  public SystemQueryOptionKind getKind() {
    return this.kind;
  }

  @Override
  public String getName() {
    return this.kind.toString();
  }

  protected void setKind(final SystemQueryOptionKind kind) {
    this.kind = kind;
  }

  @Override
  public String toString() {
    return this.kind + "=" + super.toString();
  }
}
