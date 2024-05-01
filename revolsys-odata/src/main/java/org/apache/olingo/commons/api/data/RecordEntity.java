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
package org.apache.olingo.commons.api.data;

import java.net.URI;

import org.apache.olingo.commons.api.edm.provider.CsdlStructuralType;

import com.revolsys.collection.list.ListEx;
import com.revolsys.record.Record;
import com.revolsys.record.schema.FieldDefinition;

/**
 * Data representation for a single entity.
 */
public class RecordEntity extends Linked implements ODataEntity {

  private final Record record;

  private final CsdlStructuralType structuralType;

  public RecordEntity(final CsdlStructuralType type, final Record record) {
    this.structuralType = type;
    this.record = record;
    if (type != null) {
      final var recordDefinition = type.getRecordDefinition();
      if (recordDefinition != null) {
        final String idFieldName = recordDefinition.getIdFieldName();
        if (idFieldName != null) {
          final Object idValue = record.getValue(idFieldName);
          final URI id = type.createId(idValue);
          setId(id);
        }
      }
    }
  }

  @Override
  public boolean equals(final Object o) {
    final var entity = (RecordEntity)o;
    return this.record.equals(entity.record);
  }

  public FieldDefinition getField(final String name) {
    return this.record.getField(name);
  }

  @Override
  public ListEx<String> getFieldNames() {
    return this.record.getRecordDefinition()
      .getFieldNames();
  }

  @Override
  public CsdlStructuralType getStructuralType() {
    return this.structuralType;
  }

  @Override
  public <V> V getValue(final String name) {
    return this.record.getValue(name);
  }

  @Override
  public int hashCode() {
    return this.record.hashCode();
  }

  @Override
  public String toString() {
    return this.record.toString();
  }
}
