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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * The type Csdl operation.
 */
public abstract class CsdlOperation<SELF extends CsdlOperation<SELF>>
  implements CsdlAbstractEdmItem, CsdlNamed, CsdlAnnotatable {

  /**
   * The Name.
   */
  private String name;

  private String namespace;

  /**
   * The Is bound.
   */
  private boolean isBound = false;

  /**
   * The Entity set path.
   */
  private String entitySetPath;

  /**
   * The Parameters.
   */
  private List<CsdlParameter> parameters = new ArrayList<>();

  /**
   * The Return type.
   */
  private CsdlReturnType returnType;

  /**
   * The Annotations.
   */
  private List<CsdlAnnotation> annotations = new ArrayList<>();

  public SELF addParameter(final String name, final Consumer<CsdlParameter> configurer) {
    final var parameter = new CsdlParameter();
    parameter.setName(name);
    configurer.accept(parameter);
    this.parameters.add(parameter);
    return self();
  }

  @Override
  public List<CsdlAnnotation> getAnnotations() {
    return this.annotations;
  }

  /**
   * Gets entity set path.
   *
   * @return the entity set path
   */
  public String getEntitySetPath() {
    return this.entitySetPath;
  }

  @Override
  public String getName() {
    return this.name;
  }

  public String getNamespace() {
    return this.namespace;
  }

  /**
   * Gets parameter.
   *
   * @param name the name
   * @return the parameter
   */
  public CsdlParameter getParameter(final String name) {
    return getOneByName(name, getParameters());
  }

  /**
   * Gets parameters.
   *
   * @return the parameters
   */
  public List<CsdlParameter> getParameters() {
    return this.parameters;
  }

  /**
   * Gets return type.
   *
   * @return the return type
   */
  public CsdlReturnType getReturnType() {
    return this.returnType;
  }

  /**
   * Is bound.
   *
   * @return the boolean
   */
  public boolean isBound() {
    return this.isBound;
  }

  @SuppressWarnings("unchecked")
  public SELF self() {
    return (SELF)this;
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
   * Sets as bound operation.
   *
   * @param isBound the is bound
   * @return the bound
   */
  public SELF setBound(final boolean isBound) {
    this.isBound = isBound;
    return self();
  }

  /**
   * Sets entity set path.
   *
   * @param entitySetPath the entity set path
   * @return the entity set path
   */
  public SELF setEntitySetPath(final String entitySetPath) {
    this.entitySetPath = entitySetPath;
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
    return self();
  }

  public SELF setNamespace(final String namespace) {
    this.namespace = namespace;
    return self();
  }

  /**
   * Sets parameters.
   *
   * @param parameters the parameters
   * @return the parameters
   */
  public SELF setParameters(final List<CsdlParameter> parameters) {
    this.parameters = parameters;
    return self();
  }

  public SELF setReturnType(final Consumer<CsdlReturnType> configurer) {
    this.returnType = new CsdlReturnType();
    configurer.accept(this.returnType);
    return self();
  }

  /**
   * Sets return type.
   *
   * @param returnType the return type
   * @return the return type
   */
  public SELF setReturnType(final CsdlReturnType returnType) {
    this.returnType = returnType;
    return self();
  }
}
