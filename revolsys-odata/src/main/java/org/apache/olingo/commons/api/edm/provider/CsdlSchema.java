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

import com.revolsys.io.PathName;
import com.revolsys.record.schema.RecordDefinitionBuilder;

/**
 * The type Csdl schema.
 */
public class CsdlSchema implements CsdlAbstractEdmItem, CsdlAnnotatable {

  private PathName namespace;

  private PathName alias;

  private List<CsdlEnumType> enumTypes = new ArrayList<>();

  private List<CsdlTypeDefinition> typeDefinitions = new ArrayList<>();

  private List<CsdlEntityType> entityTypes = new ArrayList<>();

  private List<CsdlComplexType> complexTypes = new ArrayList<>();

  private List<CsdlAction> actions = new ArrayList<>();

  private List<CsdlFunction> functions = new ArrayList<>();

  private CsdlEntityContainer entityContainer;

  private List<CsdlTerm> terms = new ArrayList<>();

  private List<CsdlAnnotations> annotationGroups = new ArrayList<>();

  private List<CsdlAnnotation> annotations = new ArrayList<>();

  protected void addEntityType(final CsdlEntityType entityType) {
    this.entityTypes.add(entityType);
    this.entityTypes.sort((a, b) -> a.getName()
      .compareToIgnoreCase(b.getName()));
  }

  public void addEntityType(final String name, final Consumer<RecordDefinitionBuilder> configurer) {
    final var builder = new RecordDefinitionBuilder(this.namespace.newChild(name));
    configurer.accept(builder);

    final var entityType = new CsdlEntityType(builder.getRecordDefinition());
    entityType.setName(name);
    addEntityType(entityType);
  }

  /**
   * Gets actions.
   *
   * @return the actions
   */
  public List<CsdlAction> getActions() {
    return this.actions;
  }

  /**
   * All actions with the given name
   * @param name the name
   * @return a list of actions
   */
  public List<CsdlAction> getActions(final String name) {
    return getAllByName(name, getActions());
  }

  /**
   * Gets alias.
   *
   * @return the alias
   */
  public PathName getAlias() {
    return this.alias;
  }

  /**
   * Gets annotation.
   *
   * @param term the term
   * @return the annotation
   */
  public CsdlAnnotation getAnnotation(final String term) {
    CsdlAnnotation result = null;
    for (final CsdlAnnotation annot : getAnnotations()) {
      if (term.equals(annot.getTerm())) {
        result = annot;
      }
    }
    return result;
  }

  /**
   * Gets annotation group.
   *
   * @param target the target
   * @return the annotation group
   */
  public CsdlAnnotations getAnnotationGroup(final String target, final String qualifier) {
    CsdlAnnotations result = null;
    for (final CsdlAnnotations annots : getAnnotationGroups()) {
      if (target.equals(annots.getTarget()) && (qualifier == annots.getQualifier()
        || qualifier != null && qualifier.equals(annots.getQualifier()))) {
        result = annots;
      }
    }
    return result;
  }

  /**
   * Gets annotation groups.
   *
   * @return the annotation groups
   */
  public List<CsdlAnnotations> getAnnotationGroups() {
    return this.annotationGroups;
  }

  @Override
  public List<CsdlAnnotation> getAnnotations() {
    return this.annotations;
  }

  /**
   * Gets complex type.
   *
   * @param name the name
   * @return the complex type
   */
  public CsdlComplexType getComplexType(final String name) {
    return getOneByName(name, getComplexTypes());
  }

  /**
   * Gets complex types.
   *
   * @return the complex types
   */
  public List<CsdlComplexType> getComplexTypes() {
    return this.complexTypes;
  }

  /**
   * Gets entity container.
   *
   * @return the entity container
   */
  public CsdlEntityContainer getEntityContainer() {
    return this.entityContainer;
  }

  /**
   * Gets entity type.
   *
   * @param name the name
   * @return the entity type
   */
  public CsdlEntityType getEntityType(final String name) {
    return getOneByName(name, getEntityTypes());
  }

  /**
   * Gets entity types.
   *
   * @return the entity types
   */
  public List<CsdlEntityType> getEntityTypes() {
    return this.entityTypes;
  }

  /**
   * Gets enum type.
   *
   * @param name the name
   * @return the enum type
   */
  public CsdlEnumType getEnumType(final String name) {
    return getOneByName(name, getEnumTypes());
  }

  /**
   * Gets enum types.
   *
   * @return the enum types
   */
  public List<CsdlEnumType> getEnumTypes() {
    return this.enumTypes;
  }

  /**
   * Gets functions.
   *
   * @return the functions
   */
  public List<CsdlFunction> getFunctions() {
    return this.functions;
  }

  /**
   * All functions with the given name
   * @param name the name
   * @return a list of functions
   */
  public List<CsdlFunction> getFunctions(final String name) {
    return getAllByName(name, getFunctions());
  }

  /**
   * Gets namespace.
   *
   * @return the namespace
   */
  public PathName getNamespace() {
    return this.namespace;
  }

  /**
   * Gets term.
   *
   * @param name the name
   * @return the term
   */
  public CsdlTerm getTerm(final String name) {
    return getOneByName(name, getTerms());
  }

  /**
   * Gets terms.
   *
   * @return the terms
   */
  public List<CsdlTerm> getTerms() {
    return this.terms;
  }

  /**
   * Gets type definition.
   *
   * @param name the name
   * @return the type definition
   */
  public CsdlTypeDefinition getTypeDefinition(final String name) {
    return getOneByName(name, getTypeDefinitions());
  }

  /**
   * Gets type definitions.
   *
   * @return the type definitions
   */
  public List<CsdlTypeDefinition> getTypeDefinitions() {
    return this.typeDefinitions;
  }

  /**
   * Sets actions.
   *
   * @param actions the actions
   * @return the actions
   */
  public CsdlSchema setActions(final List<CsdlAction> actions) {
    this.actions = actions;
    return this;
  }

  /**
   * Sets alias.
   *
   * @param alias the alias
   * @return the alias
   */
  public CsdlSchema setAlias(final PathName alias) {
    this.alias = alias;
    return this;
  }

  /**
   * Sets a list of annotations
   * @param annotations list of annotations
   * @return this instance
   */
  public CsdlSchema setAnnotations(final List<CsdlAnnotation> annotations) {
    this.annotations = annotations;
    return this;
  }

  /**
   * Sets a list of annotations
   * @param annotationGroups list of annotations
   * @return this instance
   */
  public CsdlSchema setAnnotationsGroup(final List<CsdlAnnotations> annotationGroups) {
    this.annotationGroups = annotationGroups;
    return this;
  }

  /**
   * Sets complex types.
   *
   * @param complexTypes the complex types
   * @return the complex types
   */
  public CsdlSchema setComplexTypes(final List<CsdlComplexType> complexTypes) {
    this.complexTypes = complexTypes;
    return this;
  }

  /**
   * Sets entity container.
   *
   * @param entityContainer the entity container
   * @return the entity container
   */
  public CsdlSchema setEntityContainer(final CsdlEntityContainer entityContainer) {
    this.entityContainer = entityContainer;
    return this;
  }

  /**
   * Sets entity types.
   *
   * @param entityTypes the entity types
   * @return the entity types
   */
  public CsdlSchema setEntityTypes(final List<CsdlEntityType> entityTypes) {
    this.entityTypes = entityTypes;
    return this;
  }

  /**
   * Sets enum types.
   *
   * @param enumTypes the enum types
   * @return the enum types
   */
  public CsdlSchema setEnumTypes(final List<CsdlEnumType> enumTypes) {
    this.enumTypes = enumTypes;
    return this;
  }

  /**
   * Sets functions.
   *
   * @param functions the functions
   * @return the functions
   */
  public CsdlSchema setFunctions(final List<CsdlFunction> functions) {
    this.functions = functions;
    return this;
  }

  /**
   * Sets namespace.
   *
   * @param namespace the namespace
   * @return the namespace
   */
  public CsdlSchema setNamespace(final PathName namespace) {
    this.namespace = namespace;
    return this;
  }

  /**
   * Sets terms.
   *
   * @param terms the terms
   * @return the terms
   */
  public CsdlSchema setTerms(final List<CsdlTerm> terms) {
    this.terms = terms;
    return this;
  }

  /**
   * Sets type definitions.
   *
   * @param typeDefinitions the type definitions
   * @return the type definitions
   */
  public CsdlSchema setTypeDefinitions(final List<CsdlTypeDefinition> typeDefinitions) {
    this.typeDefinitions = typeDefinitions;
    return this;
  }
}
