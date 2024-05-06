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
package org.apache.olingo.commons.api.edm.provider;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.data.RecordEntity;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.core.edm.Edm;

import com.revolsys.io.PathName;
import com.revolsys.record.Record;
import com.revolsys.record.RecordState;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.record.schema.RecordDefinitionProxy;

/**
 * The type Csdl structural type.
 */
public abstract class CsdlStructuralType<SELF extends CsdlStructuralType<SELF>>
  implements CsdlAbstractEdmItem, CsdlNamed, CsdlAnnotatable, RecordDefinitionProxy {

  /**
   * The Name.
   */
  protected String name;

  /**
   * The Is open type.
   */
  protected boolean isOpenType = false;

  /**
   * The Base type.
   */
  protected PathName baseType;

  /**
   * The Is abstract.
   */
  protected boolean isAbstract;

  /**
   * The Navigation properties.
   */
  protected List<CsdlNavigationProperty> navigationProperties = new ArrayList<>();

  private RecordDefinition recordDefinition = new RecordDefinitionImpl();

  /**
   * The Annotations.
   */
  protected List<CsdlAnnotation> annotations = new ArrayList<>();

  public URI createId(final Object id) {
    final StringBuilder idBuilder = new StringBuilder(getName()).append('(');

    if (id == null) {
      return null;
    } else {
      if (id instanceof Number) {
        idBuilder.append(id);
      } else {
        final String idString = URLEncoder.encode(id.toString(), StandardCharsets.UTF_8);
        idBuilder //
          .append('\'')
          .append(idString)
          .append('\'');
      }
      idBuilder.append(')');
      final String idUrl = idBuilder.toString();

      try {
        return new URI(idUrl);
      } catch (final URISyntaxException e) {
        throw new ODataRuntimeException("Unable to create id for entity: " + idUrl, e);
      }
    }
  }

  @Override
  public List<CsdlAnnotation> getAnnotations() {
    return this.annotations;
  }

  /**
   * Gets base type.
   *
   * @return the base type
   */
  public String getBaseType() {
    if (this.baseType != null) {
      return this.baseType.toDotSeparated();
    }
    return null;
  }

  /**
   * Gets base type fQN.
   *
   * @return the base type fQN
   */
  public PathName getBaseTypePathName() {
    return this.baseType;
  }

  /**
   * Gets property.
   *
   * @param name the name
   * @return the property
   */
  public FieldDefinition getField(final String name) {
    for (final var field : getFields()) {
      if (name.equals(field.getName())) {
        return field;
      }
    }
    return null;
  }

  public List<CsdlAnnotation> getFieldAnnotations(final String name) {
    return Edm.getAnnotations(getField(name));
  }

  /**
   * Gets properties.
   *
   * @return the properties
   */
  public List<FieldDefinition> getFields() {
    return this.recordDefinition.getFields();
  }

  @Override
  public String getName() {
    return this.name;
  }

  /**
   * Gets navigation properties.
   *
   * @return the navigation properties
   */
  public List<CsdlNavigationProperty> getNavigationProperties() {
    return this.navigationProperties;
  }

  /**
   * Gets navigation property.
   *
   * @param name the name
   * @return the navigation property
   */
  public CsdlNavigationProperty getNavigationProperty(final String name) {
    return getOneByName(name, this.navigationProperties);
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    return this.recordDefinition;
  }

  /**
   * Is abstract.
   *
   * @return the boolean
   */
  public boolean isAbstract() {
    return this.isAbstract;
  }

  /**
   * Is open type.
   *
   * @return the boolean
   */
  public boolean isOpenType() {
    return this.isOpenType;
  }

  public RecordEntity newEntity(final Record record) {
    return new RecordEntity(this, record);
  }

  public Record newRecord(final Record values) {
    final var recordDefinition = getRecordDefinition();
    final var record = recordDefinition.newRecord();
    record.setState(RecordState.INITIALIZING);
    record.setValuesAll(values);
    record.setState(RecordState.PERSISTED);
    return record;
  }

  @SuppressWarnings("unchecked")
  public SELF self() {
    return (SELF)this;
  }

  /**
   * Sets abstract.
   *
   * @param isAbstract the is abstract
   * @return the abstract
   */
  public SELF setAbstract(final boolean isAbstract) {
    this.isAbstract = isAbstract;
    return self();
  }

  /**
   * Sets a list of annotations
   * @param annotations list of annotations
   * @return this instance
   */
  public SELF setAnnotations(final List<CsdlAnnotation> annotations) {
    this.annotations = annotations;
    return self();
  }

  /**
   * Sets base type.
   *
   * @param baseType the base type
   * @return the base type
   */
  public SELF setBaseType(final PathName baseType) {
    this.baseType = baseType;
    return self();
  }

  /**
   * Sets base type.
   *
   * @param baseType the base type
   * @return the base type
   */
  public SELF setBaseType(final String baseType) {
    this.baseType = PathName.fromDotSeparated(baseType);
    return self();
  }

  /**
   * Sets properties.
   *
   * @param fields the properties
   * @return the properties
   */
  public SELF setFields(final List<FieldDefinition> fields) {
    for (final var field : fields) {
      ((RecordDefinitionImpl)this.recordDefinition).addField(field);
    }
    return self();
  }

  /**
   * Sets name.
   *
   * @param name the name
   * @return the name
   */
  public SELF setName(final String name) {
    this.name = name;
    ((RecordDefinitionImpl)this.recordDefinition).setPathName(name);
    return self();
  }

  /**
   * Sets navigation properties.
   *
   * @param navigationProperties the navigation properties
   * @return the navigation properties
   */
  public SELF setNavigationProperties(final List<CsdlNavigationProperty> navigationProperties) {
    this.navigationProperties = navigationProperties;
    return self();
  }

  /**
   * Sets open type.
   *
   * @param isOpenType the is open type
   * @return the open type
   */
  public SELF setOpenType(final boolean isOpenType) {
    this.isOpenType = isOpenType;
    return self();
  }

  protected void setRecordDefinition(final RecordDefinition recordDefinition) {
    this.recordDefinition = recordDefinition;
  }
}
