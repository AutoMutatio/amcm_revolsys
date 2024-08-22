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
package org.apache.olingo.commons.core.edm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmAction;
import org.apache.olingo.commons.api.edm.EdmAnnotations;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmEnumType;
import org.apache.olingo.commons.api.edm.EdmException;
import org.apache.olingo.commons.api.edm.EdmFunction;
import org.apache.olingo.commons.api.edm.EdmParameter;
import org.apache.olingo.commons.api.edm.EdmSchema;
import org.apache.olingo.commons.api.edm.EdmTerm;
import org.apache.olingo.commons.api.edm.EdmTypeDefinition;
import org.apache.olingo.commons.api.edm.provider.CsdlAction;
import org.apache.olingo.commons.api.edm.provider.CsdlAliasInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotation;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotations;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlEnumType;
import org.apache.olingo.commons.api.edm.provider.CsdlFunction;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlOperation;
import org.apache.olingo.commons.api.edm.provider.CsdlParameter;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.edm.provider.CsdlStructuralType;
import org.apache.olingo.commons.api.edm.provider.CsdlTerm;
import org.apache.olingo.commons.api.edm.provider.CsdlTypeDefinition;
import org.apache.olingo.commons.api.ex.ODataException;
import org.apache.olingo.server.core.uri.parser.Parser;

import com.revolsys.collection.map.Maps;
import com.revolsys.collection.value.ValueHolder;
import com.revolsys.io.PathName;
import com.revolsys.record.schema.FieldDefinition;

/**
 * Entity Data Model (EDM)
 * <br/>
 * Interface representing a Entity Data Model as described in the Conceptual Schema Definition.
 */
public class Edm {

  public static List<CsdlAnnotation> getAnnotations(final FieldDefinition field) {
    return field.getProperty("csdlAnnotations");
  }

  public static String getType(final FieldDefinition field) {
    return field.getProperty("csdlType");
  }

  public static PathName getTypeName(final FieldDefinition field) {
    return field.getProperty("csdlTypeName");
  }

  private final Parser parser = new Parser(this);

  protected Map<PathName, EdmSchema> schemas;

  protected List<EdmSchema> schemaList;

  private boolean isEntityDerivedFromES;

  private boolean isComplexDerivedFromES;

  private boolean isPreviousES;

  private final Map<PathName, EdmEntityContainer> entityContainers = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<PathName, EdmEnumType> enumTypes = Collections.synchronizedMap(new HashMap<>());

  private final Map<PathName, EdmTypeDefinition> typeDefinitions = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<PathName, EdmEntityType> entityTypes = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<PathName, EdmComplexType> complexTypes = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<PathName, EdmAction> unboundActions = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<PathName, List<EdmFunction>> unboundFunctionsByName = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<FunctionMapKey, EdmFunction> unboundFunctionsByKey = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<ActionMapKey, EdmAction> boundActions = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<FunctionMapKey, EdmFunction> boundFunctions = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<PathName, EdmTerm> terms = Collections.synchronizedMap(new HashMap<>());

  private final Map<TargetQualifierMapKey, EdmAnnotations> annotationGroups = Collections
    .synchronizedMap(new HashMap<>());

  private final ValueHolder<Map<PathName, PathName>> aliasToNamespaceInfo = ValueHolder
    .lazy(this::initAliasToNamespaceInfo);

  private final Map<PathName, EdmEntityType> entityTypesWithAnnotations = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<PathName, EdmEntityType> entityTypesDerivedFromES = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<PathName, EdmComplexType> complexTypesWithAnnotations = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<PathName, EdmComplexType> complexTypesDerivedFromES = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<String, List<CsdlAnnotation>> annotationMap = new HashMap<>();

  private final CsdlEdmProvider provider;

  private final Map<PathName, List<CsdlAction>> actionsMap = Collections
    .synchronizedMap(new HashMap<>());

  private final Map<PathName, List<CsdlFunction>> functionsMap = Maps
    .lazy(this::getProviderFunction);

  private List<CsdlSchema> termSchemaDefinition = new ArrayList<>();

  public Edm(final CsdlEdmProvider provider) {
    this.provider = provider;
  }

  public Edm(final CsdlEdmProvider provider, final List<CsdlSchema> termSchemaDefinition) {
    this.provider = provider;
    this.termSchemaDefinition = termSchemaDefinition;
    populateAnnotationMap();
  }

  /**
   * @param csdlEntityContainer
   * @param annotations
   */
  private void addAnnotationsOnEntityContainer(final CsdlEntityContainer csdlEntityContainer,
    final List<CsdlAnnotation> annotations) {
    if (null != annotations) {
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(csdlEntityContainer.getAnnotations(), annotation)) {
          csdlEntityContainer.getAnnotations()
            .add(annotation);
        }
      }
    }
  }

  /**
   * @param enumType
   * @param annotations
   */
  private void addAnnotationsOnEnumTypes(final CsdlEnumType enumType,
    final List<CsdlAnnotation> annotations) {
    if (null != annotations) {
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(enumType.getAnnotations(), annotation)) {
          enumType.getAnnotations()
            .add(annotation);
        }
      }
    }
  }

  /**
   * Adds annotations to navigation properties of entity and complex types
   * @param structuralType
   * @param navProperty
   * @param navPropAnnotations
   */
  private void addAnnotationsOnNavProperties(final CsdlStructuralType<?> structuralType,
    final CsdlNavigationProperty navProperty, final List<CsdlAnnotation> navPropAnnotations) {
    if (null != navPropAnnotations && !navPropAnnotations.isEmpty()) {
      for (final CsdlAnnotation annotation : navPropAnnotations) {
        if (!compareAnnotations(structuralType.getNavigationProperty(navProperty.getName())
          .getAnnotations(), annotation)) {
          structuralType.getNavigationProperty(navProperty.getName())
            .getAnnotations()
            .add(annotation);
        }
      }
    }
  }

  /**
   * Adds annotations to properties of entity type and complex type
   * @param structuralType
   * @param property
   * @param propAnnotations
   */
  private void addAnnotationsOnPropertiesOfStructuralType(
    final CsdlStructuralType<?> structuralType, final String name,
    final List<CsdlAnnotation> propAnnotations) {
    if (null != propAnnotations && !propAnnotations.isEmpty()) {
      for (final CsdlAnnotation annotation : propAnnotations) {
        if (!compareAnnotations(structuralType.getFieldAnnotations(name), annotation)) {
          structuralType.getFieldAnnotations(name)
            .add(annotation);
        }
      }
    }
  }

  /**
   * Add annoations to entity types and complex types
   * @param entityType
   * @param annotations
   */
  private void addAnnotationsOnStructuralType(final CsdlStructuralType<?> structuralType,
    final List<CsdlAnnotation> annotations) {
    if (null != annotations && !annotations.isEmpty()) {
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(structuralType.getAnnotations(), annotation)) {
          structuralType.getAnnotations()
            .add(annotation);
        }
      }
    }
  }

  /**
   * @param typeDefinition
   * @param annotations
   */
  private void addAnnotationsOnTypeDefinitions(final CsdlTypeDefinition typeDefinition,
    final List<CsdlAnnotation> annotations) {
    if (null != annotations) {
      for (final CsdlAnnotation annotation : annotations) {
        if (!compareAnnotations(typeDefinition.getAnnotations(), annotation)) {
          typeDefinition.getAnnotations()
            .add(annotation);
        }
      }
    }
  }

  /** Adds annotations to action
   * @param operation
   * @param annotationsOnAlias
   */
  private void addAnnotationsToOperations(final CsdlOperation<?> operation,
    final List<CsdlAnnotation> annotations) {
    for (final CsdlAnnotation annotation : annotations) {
      if (!compareAnnotations(operation.getAnnotations(), annotation)) {
        operation.getAnnotations()
          .add(annotation);
      }
    }
  }

  /** Adds annotations to action parameters
   * @param operation
   * @param actionName
   * @param annotations
   */
  private void addAnnotationsToParamsOfOperations(final CsdlOperation<?> operation,
    final PathName actionName) {
    final List<CsdlParameter> parameters = operation.getParameters();
    for (final CsdlParameter parameter : parameters) {
      final List<CsdlAnnotation> annotsToParams = getAnnotationsMap()
        .get(actionName.toDotSeparated() + "/" + parameter.getName());
      if (null != annotsToParams && !annotsToParams.isEmpty()) {
        for (final CsdlAnnotation annotation : annotsToParams) {
          if (!compareAnnotations(operation.getParameter(parameter.getName())
            .getAnnotations(), annotation)) {
            operation.getParameter(parameter.getName())
              .getAnnotations()
              .add(annotation);
          }
        }
      }
      final var aliasName = getAliasInfo(actionName.getParent());
      final List<CsdlAnnotation> annotsToParamsOnAlias = getAnnotationsMap()
        .get(aliasName.toDotSeparated() + "." + actionName.getName() + "/" + parameter.getName());
      if (null != annotsToParamsOnAlias && !annotsToParamsOnAlias.isEmpty()) {
        for (final CsdlAnnotation annotation : annotsToParamsOnAlias) {
          if (!compareAnnotations(operation.getParameter(parameter.getName())
            .getAnnotations(), annotation)) {
            operation.getParameter(parameter.getName())
              .getAnnotations()
              .add(annotation);
          }
        }
      }
    }
  }

  public void addEntityContainerAnnotations(final CsdlEntityContainer csdlEntityContainer,
    final PathName containerName) {
    final var aliasName = getAliasInfo(containerName.getParent());
    final List<CsdlAnnotation> annotations = getAnnotationsMap()
      .get(containerName.toDotSeparated());
    final List<CsdlAnnotation> annotationsOnAlias = getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + containerName.getName());
    addAnnotationsOnEntityContainer(csdlEntityContainer, annotations);
    addAnnotationsOnEntityContainer(csdlEntityContainer, annotationsOnAlias);
  }

  public void addEnumTypeAnnotations(final CsdlEnumType enumType, final PathName enumName) {
    final var aliasName = getAliasInfo(enumName.getParent());
    final List<CsdlAnnotation> annotations = getAnnotationsMap().get(enumName.toDotSeparated());
    final List<CsdlAnnotation> annotationsOnAlias = getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + enumName.getName());
    addAnnotationsOnEnumTypes(enumType, annotations);
    addAnnotationsOnEnumTypes(enumType, annotationsOnAlias);
  }

  public void addOperationsAnnotations(final CsdlOperation<?> operation,
    final PathName actionName) {
    final var aliasName = getAliasInfo(actionName.getParent());
    final List<CsdlAnnotation> annotations = getAnnotationsMap().get(actionName.toDotSeparated());
    final List<CsdlAnnotation> annotationsOnAlias = getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + actionName.getName());
    if (null != annotations) {
      addAnnotationsToOperations(operation, annotations);
    }
    if (null != annotationsOnAlias) {
      addAnnotationsToOperations(operation, annotationsOnAlias);
    }
    addAnnotationsToParamsOfOperations(operation, actionName);
  }

  /**
   * Add the annotations defined in an external file to the property/
   * navigation property and the entity
   * @param structuralType
   * @param typeName
   * @param csdlEntityContainer
   */
  public void addStructuralTypeAnnotations(final CsdlStructuralType<?> structuralType,
    final PathName typeName, final CsdlEntityContainer csdlEntityContainer) {
    updateAnnotationsOnStructuralProperties(structuralType, typeName, csdlEntityContainer);
    updateAnnotationsOnStructuralNavProperties(structuralType, typeName, csdlEntityContainer);
  }

  public void addTypeDefnAnnotations(final CsdlTypeDefinition typeDefinition,
    final PathName typeDefinitionName) {
    final var aliasName = getAliasInfo(typeDefinitionName.getParent());
    final List<CsdlAnnotation> annotations = getAnnotationsMap()
      .get(typeDefinitionName.toDotSeparated());
    final List<CsdlAnnotation> annotationsOnAlias = getAnnotationsMap()
      .get(aliasName.toDotSeparated() + "." + typeDefinitionName.getName());
    addAnnotationsOnTypeDefinitions(typeDefinition, annotations);
    addAnnotationsOnTypeDefinitions(typeDefinition, annotationsOnAlias);
  }

  public void cacheAction(final PathName actionName, final EdmAction action) {
    if (action.isBound()) {
      final ActionMapKey key = new ActionMapKey(actionName,
        action.getBindingParameterTypePathName(), action.isBindingParameterTypeCollection());
      this.boundActions.put(key, action);
    } else {
      this.unboundActions.put(actionName, action);
    }
  }

  public void cacheAliasNamespaceInfo(final PathName alias, final PathName namespace) {
    this.aliasToNamespaceInfo.getValue()
      .put(alias, namespace);
  }

  public void cacheAnnotationGroup(final PathName targetName,
    final EdmAnnotations annotationsGroup) {
    final TargetQualifierMapKey key = new TargetQualifierMapKey(targetName,
      annotationsGroup.getQualifier());
    this.annotationGroups.put(key, annotationsGroup);
  }

  public void cacheComplexType(final PathName compelxTypeName, final EdmComplexType complexType) {
    this.complexTypes.put(compelxTypeName, complexType);
  }

  public void cacheEntityContainer(final PathName containerFQN,
    final EdmEntityContainer container) {
    this.entityContainers.put(containerFQN, container);
  }

  public void cacheEntityType(final PathName entityTypeName, final EdmEntityType entityType) {
    this.entityTypes.put(entityTypeName, entityType);
  }

  public void cacheEnumType(final PathName enumName, final EdmEnumType enumType) {
    this.enumTypes.put(enumName, enumType);
  }

  public void cacheFunction(final PathName functionName, final EdmFunction function) {
    final FunctionMapKey key = new FunctionMapKey(functionName,
      function.getBindingParameterTypePathName(), function.isBindingParameterTypeCollection(),
      function.getParameterNames());

    if (function.isBound()) {
      this.boundFunctions.put(key, function);
    } else {
      if (!this.unboundFunctionsByName.containsKey(functionName)) {
        this.unboundFunctionsByName.put(functionName, new ArrayList<>());
      }
      this.unboundFunctionsByName.get(functionName)
        .add(function);

      this.unboundFunctionsByKey.put(key, function);
    }
  }

  public void cacheTerm(final PathName termName, final EdmTerm term) {
    this.terms.put(termName, term);
  }

  public void cacheTypeDefinition(final PathName typeDefName, final EdmTypeDefinition typeDef) {
    this.typeDefinitions.put(typeDefName, typeDef);
  }

  private boolean compareAnnotations(final List<CsdlAnnotation> annotations,
    final CsdlAnnotation annotation) {
    for (final CsdlAnnotation annot : annotations) {
      if (annot.equals(annotation)) {
        return true;
      }
    }
    return false;
  }

  protected EdmAnnotations createAnnotationGroup(final PathName targetName,
    final String qualifier) {
    try {
      CsdlAnnotations providerGroup = this.provider.getAnnotationsGroup(targetName, qualifier);
      if (null == providerGroup) {
        for (final CsdlSchema schema : this.termSchemaDefinition) {
          providerGroup = schema.getAnnotationGroup(targetName.toDotSeparated(), qualifier);
          break;
        }
      }
      if (providerGroup != null) {
        return new EdmAnnotationsImpl(this, providerGroup);
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  public EdmAction createBoundAction(final PathName actionName,
    final PathName bindingParameterTypeName, final Boolean isBindingParameterCollection) {

    try {
      List<CsdlAction> actions = this.actionsMap.get(actionName);
      if (actions == null) {
        actions = this.provider.getActions(actionName);
        if (actions == null) {
          return null;
        } else {
          this.actionsMap.put(actionName, actions);
        }
      }
      // Search for bound action where binding parameter matches
      for (final CsdlAction action : actions) {
        if (action.isBound()) {
          final List<CsdlParameter> parameters = action.getParameters();
          final CsdlParameter parameter = parameters.get(0);
          if ((bindingParameterTypeName.equals(parameter.getTypePathName())
            || isEntityPreviousTypeCompatibleToBindingParam(bindingParameterTypeName, parameter)
            || isComplexPreviousTypeCompatibleToBindingParam(bindingParameterTypeName, parameter,
              isBindingParameterCollection))
            && isBindingParameterCollection.booleanValue() == parameter.isCollection()) {
            addOperationsAnnotations(action, actionName);
            return new EdmActionImpl(this, actionName, action);
          }

        }
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  public EdmFunction createBoundFunction(final PathName functionName,
    final PathName bindingParameterTypeName, final Boolean isBindingParameterCollection,
    final List<String> parameterNames) {

    try {
      final List<CsdlFunction> functions = this.functionsMap.get(functionName);

      final List<String> parameterNamesCopy = parameterNames == null
        ? Collections.<String> emptyList()
        : parameterNames;
      for (final CsdlFunction function : functions) {
        if (function.isBound()) {
          final List<CsdlParameter> providerParameters = function.getParameters();
          if (providerParameters == null || providerParameters.isEmpty()) {
            throw new EdmException("No parameter specified for bound function: " + functionName);
          }
          final CsdlParameter bindingParameter = providerParameters.get(0);
          if ((bindingParameterTypeName.equals(bindingParameter.getTypePathName())
            || isEntityPreviousTypeCompatibleToBindingParam(bindingParameterTypeName,
              bindingParameter)
            || isComplexPreviousTypeCompatibleToBindingParam(bindingParameterTypeName,
              bindingParameter, isBindingParameterCollection))
            && isBindingParameterCollection.booleanValue() == bindingParameter.isCollection()
            && parameterNamesCopy.size() == providerParameters.size() - 1) {

            final List<String> providerParameterNames = new ArrayList<>();
            for (int i = 1; i < providerParameters.size(); i++) {
              providerParameterNames.add(providerParameters.get(i)
                .getName());
            }
            if (parameterNamesCopy.containsAll(providerParameterNames)) {
              addOperationsAnnotations(function, functionName);
              return new EdmFunction(this, functionName, function);
            }
          }
        }
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  public EdmComplexType createComplexType(final PathName complexTypeName) {
    try {
      final CsdlComplexType complexType = this.provider.getComplexType(complexTypeName);
      if (complexType != null) {
        final List<CsdlAnnotation> annotations = getAnnotationsMap()
          .get(complexTypeName.toDotSeparated());
        if (null != annotations && !annotations.isEmpty()) {
          addAnnotationsOnStructuralType(complexType, annotations);
        }
        final var aliasName = getAliasInfo(complexTypeName.getParent());
        final List<CsdlAnnotation> annotationsOnAlias = getAnnotationsMap()
          .get(aliasName.toDotSeparated() + "." + complexTypeName.getName());
        if (null != annotationsOnAlias && !annotationsOnAlias.isEmpty()) {
          addAnnotationsOnStructuralType(complexType, annotationsOnAlias);
        }

        if (!isComplexDerivedFromES()) {
          addStructuralTypeAnnotations(complexType, complexTypeName,
            this.provider.getEntityContainer());
        }
        return new EdmComplexTypeImpl(this, complexTypeName, complexType);
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  public EdmEntityContainer createEntityContainer(final PathName containerName) {
    try {
      final CsdlEntityContainerInfo entityContainerInfo = this.provider
        .getEntityContainerInfo(containerName);
      if (entityContainerInfo != null) {
        final CsdlEntityContainer entityContainer = this.provider.getEntityContainer();
        addEntityContainerAnnotations(entityContainer, entityContainerInfo.getContainerName());
        return new EdmEntityContainer(this, this.provider, entityContainerInfo.getContainerName(),
          entityContainer);
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  public EdmEntityType createEntityType(final PathName entityTypeName) {
    try {
      final var entityType = this.provider.getEntityType(entityTypeName);
      if (entityType != null) {
        final List<CsdlAnnotation> annotations = getAnnotationsMap()
          .get(entityTypeName.toDotSeparated());
        final var aliasName = getAliasInfo(entityTypeName.getParent());
        final List<CsdlAnnotation> annotationsOnAlias = getAnnotationsMap()
          .get(aliasName.toDotSeparated() + "." + entityTypeName.getName());
        addAnnotationsOnStructuralType(entityType, annotations);
        addAnnotationsOnStructuralType(entityType, annotationsOnAlias);

        if (!isEntityDerivedFromES()) {
          addStructuralTypeAnnotations(entityType, entityTypeName,
            this.provider.getEntityContainer());
        }
        return new EdmEntityType(this, entityTypeName, entityType);
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  public EdmEnumType createEnumType(final PathName enumName) {
    try {
      final CsdlEnumType enumType = this.provider.getEnumType(enumName);
      if (enumType != null) {
        addEnumTypeAnnotations(enumType, enumName);
        return new EdmEnumType(this, enumName, enumType);
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  protected Map<PathName, EdmSchema> createSchemas() {
    try {
      final Map<PathName, EdmSchema> providerSchemas = new LinkedHashMap<>();
      final List<CsdlSchema> localSchemas = this.provider.getSchemas();
      if (localSchemas != null) {
        for (final CsdlSchema schema : localSchemas) {
          providerSchemas.put(schema.getNamespace(),
            new EdmSchemaImpl(this, this.provider, schema));
        }
      }
      for (final CsdlSchema termSchemaDefn : this.termSchemaDefinition) {
        providerSchemas.put(termSchemaDefn.getNamespace(),
          new EdmSchemaImpl(this, this.provider, termSchemaDefn));
      }
      return providerSchemas;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  protected EdmTerm createTerm(final PathName termName) {
    try {
      final CsdlTerm providerTerm = this.provider.getTerm(termName);
      if (providerTerm != null) {
        return new EdmTermImpl(this, termName.getParent(), providerTerm);
      } else {
        for (final CsdlSchema schema : this.termSchemaDefinition) {
          if (schema.getNamespace()
            .equalsIgnoreCase(termName.getParent())
            || null != schema.getAlias() && schema.getAlias()
              .equalsIgnoreCase(termName.getParent())) {
            final List<CsdlTerm> terms = schema.getTerms();
            for (final CsdlTerm term : terms) {
              if (term.getName()
                .equals(termName.getName())) {
                return new EdmTermImpl(this, termName.getParent(), term);
              }
            }
          }
        }
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  public EdmTypeDefinition createTypeDefinition(final PathName typeDefinitionName) {
    try {
      final CsdlTypeDefinition typeDefinition = this.provider.getTypeDefinition(typeDefinitionName);
      if (typeDefinition != null) {
        addTypeDefnAnnotations(typeDefinition, typeDefinitionName);
        return new EdmTypeDefinition(this, typeDefinitionName, typeDefinition);
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  protected EdmAction createUnboundAction(final PathName actionName) {
    try {
      List<CsdlAction> actions = this.actionsMap.get(actionName);
      if (actions == null) {
        actions = this.provider.getActions(actionName);
        if (actions == null) {
          return null;
        } else {
          this.actionsMap.put(actionName, actions);
        }
      }
      // Search for first unbound action
      for (final CsdlAction action : actions) {
        if (!action.isBound()) {
          addOperationsAnnotations(action, actionName);
          return new EdmActionImpl(this, actionName, action);
        }
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  protected EdmFunction createUnboundFunction(final PathName functionName,
    final List<String> parameterNames) {
    try {
      List<CsdlFunction> functions = this.functionsMap.get(functionName);
      if (functions == null) {
        functions = getProviderFunction(functionName);
        if (functions == null) {
          return null;
        } else {
          this.functionsMap.put(functionName, functions);
        }
      }

      final List<String> parameterNamesCopy = parameterNames == null
        ? Collections.<String> emptyList()
        : parameterNames;
      for (final CsdlFunction function : functions) {
        if (!function.isBound()) {
          List<CsdlParameter> providerParameters = function.getParameters();
          if (providerParameters == null) {
            providerParameters = Collections.emptyList();
          }
          if (parameterNamesCopy.size() == providerParameters.size()) {
            final List<String> functionParameterNames = new ArrayList<>();
            for (final CsdlParameter parameter : providerParameters) {
              functionParameterNames.add(parameter.getName());
            }

            if (parameterNamesCopy.containsAll(functionParameterNames)) {
              addOperationsAnnotations(function, functionName);
              addAnnotationsToParamsOfOperations(function, functionName);
              return new EdmFunction(this, functionName, function);
            }
          }
        }
      }
      return null;
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  protected List<EdmFunction> createUnboundFunctions(final PathName functionName) {
    final List<EdmFunction> result = new ArrayList<>();

    try {
      List<CsdlFunction> functions = this.functionsMap.get(functionName);
      if (functions == null) {
        functions = getProviderFunction(functionName);
        if (functions != null) {
          this.functionsMap.put(functionName, functions);
        }
      }
      if (functions != null) {
        for (final CsdlFunction function : functions) {
          if (!function.isBound()) {
            addOperationsAnnotations(function, functionName);
            result.add(new EdmFunction(this, functionName, function));
          }
        }
      }
    } catch (final ODataException e) {
      throw new EdmException(e);
    }

    return result;
  }

  /**
   * @param schema
   */
  private void fetchAnnotationsInMetadataAndExternalFile(final CsdlSchema schema) {
    final List<CsdlAnnotations> annotationGrps = schema.getAnnotationGroups();
    for (final CsdlAnnotations annotationGrp : annotationGrps) {
      if (!getAnnotationsMap().containsKey(annotationGrp.getTarget())) {
        getAnnotationsMap().put(annotationGrp.getTarget(), annotationGrp.getAnnotations());
      } else {
        final List<CsdlAnnotation> annotations = getAnnotationsMap().get(annotationGrp.getTarget());
        final List<CsdlAnnotation> newAnnotations = new ArrayList<>();
        for (final CsdlAnnotation annotation : annotationGrp.getAnnotations()) {
          if (!compareAnnotations(annotations, annotation)) {
            newAnnotations.add(annotation);
          }
        }
        if (!newAnnotations.isEmpty()) {
          getAnnotationsMap().get(annotationGrp.getTarget())
            .addAll(newAnnotations);
        }
      }
    }
  }

  /**
   * Get alias name given the namespace from the alias info
   * @param namespace
   * @return
   */
  private PathName getAliasInfo(final PathName namespace) {
    final var aliasInfos = this.provider.getAliasInfos();
    for (final CsdlAliasInfo aliasInfo : aliasInfos) {
      if (null != aliasInfo.getParent() && aliasInfo.getParent()
        .equalsIgnoreCase(namespace)) {
        return aliasInfo.getAlias();
      }
    }
    return PathName.ROOT;
  }

  public EdmAnnotations getAnnotationGroup(final PathName targetName, final String qualifier) {
    final var fqn = resolvePossibleAlias(targetName);
    final TargetQualifierMapKey key = new TargetQualifierMapKey(fqn, qualifier);
    EdmAnnotations _annotations = this.annotationGroups.get(key);
    if (_annotations == null) {
      _annotations = createAnnotationGroup(fqn, qualifier);
      if (_annotations != null) {
        this.annotationGroups.put(key, _annotations);
      }
    }
    return _annotations;
  }

  public Map<String, List<CsdlAnnotation>> getAnnotationsMap() {
    return this.annotationMap;
  }

  public EdmAction getBoundAction(final PathName actionName,
    final PathName bindingParameterTypeName, final Boolean isBindingParameterCollection) {

    final var actionFqn = resolvePossibleAlias(actionName);
    final var bindingParameterTypeFqn = resolvePossibleAlias(bindingParameterTypeName);
    final ActionMapKey key = new ActionMapKey(actionFqn, bindingParameterTypeFqn,
      isBindingParameterCollection);
    EdmAction action = this.boundActions.get(key);
    if (action == null) {
      action = createBoundAction(actionFqn, bindingParameterTypeFqn, isBindingParameterCollection);
      if (action != null) {
        this.boundActions.put(key, action);
      }
    }

    return action;
  }

  public EdmAction getBoundActionWithBindingType(final PathName bindingParameterTypeName,
    final Boolean isBindingParameterCollection) {
    for (final EdmSchema schema : getSchemas()) {
      for (final EdmAction action : schema.getActions()) {
        if (action.isBound()) {
          final EdmParameter bindingParameter = action.getParameter(action.getParameterNames()
            .get(0));
          if (bindingParameter.getType()
            .getPathName()
            .equals(bindingParameterTypeName)
            && bindingParameter.isCollection() == isBindingParameterCollection) {
            return action;
          }
        }
      }
    }
    return null;
  }

  public EdmFunction getBoundFunction(final PathName functionName,
    final PathName bindingParameterTypeName, final Boolean isBindingParameterCollection,
    final List<String> parameterNames) {

    final var functionFqn = resolvePossibleAlias(functionName);
    final var bindingParameterTypeFqn = resolvePossibleAlias(bindingParameterTypeName);
    final FunctionMapKey key = new FunctionMapKey(functionFqn, bindingParameterTypeFqn,
      isBindingParameterCollection, parameterNames);
    EdmFunction function = this.boundFunctions.get(key);
    if (function == null) {
      function = createBoundFunction(functionFqn, bindingParameterTypeFqn,
        isBindingParameterCollection, parameterNames);
      if (function != null) {
        this.boundFunctions.put(key, function);
      }
    }

    return function;
  }

  public List<EdmFunction> getBoundFunctionsWithBindingType(final PathName bindingParameterTypeName,
    final Boolean isBindingParameterCollection) {
    final List<EdmFunction> functions = new ArrayList<>();
    for (final EdmSchema schema : getSchemas()) {
      for (final EdmFunction function : schema.getFunctions()) {
        if (function.isBound()) {
          final EdmParameter bindingParameter = function.getParameter(function.getParameterNames()
            .get(0));
          if (bindingParameter.getType()
            .getPathName()
            .equals(bindingParameterTypeName)
            && bindingParameter.isCollection() == isBindingParameterCollection) {
            functions.add(function);
          }
        }
      }
    }
    return functions;
  }

  public EdmComplexType getComplexType(final PathName namespaceOrAliasFQN) {
    final var fqn = resolvePossibleAlias(namespaceOrAliasFQN);
    EdmComplexType complexType = this.complexTypes.get(fqn);
    if (complexType == null) {
      complexType = createComplexType(fqn);
      if (complexType != null) {
        this.complexTypes.put(fqn, complexType);
      }
    }
    return complexType;
  }

  public EdmComplexType getComplexTypeWithAnnotations(final PathName namespaceOrAliasFQN) {
    final var fqn = resolvePossibleAlias(namespaceOrAliasFQN);
    EdmComplexType complexType = this.complexTypesWithAnnotations.get(fqn);
    if (complexType == null) {
      complexType = createComplexType(fqn);
      if (complexType != null) {
        this.complexTypesWithAnnotations.put(fqn, complexType);
      }
    }
    setIsPreviousES(false);
    return complexType;
  }

  protected EdmComplexType getComplexTypeWithAnnotations(final PathName namespaceOrAliasFQN,
    final boolean isComplexDerivedFromES) {
    this.isComplexDerivedFromES = isComplexDerivedFromES;
    final var fqn = resolvePossibleAlias(namespaceOrAliasFQN);
    if (!isPreviousES() && getEntityContainer() != null) {
      getEntityContainer().getEntitySetsWithAnnotations();
    }
    EdmComplexType complexType = this.complexTypesDerivedFromES.get(fqn);
    if (complexType == null) {
      complexType = createComplexType(fqn);
      if (complexType != null) {
        this.complexTypesDerivedFromES.put(fqn, complexType);
      }
    }
    this.isComplexDerivedFromES = false;
    return complexType;
  }

  public EdmEntityContainer getEntityContainer() {
    return getEntityContainer(null);
  }

  public EdmEntityContainer getEntityContainer(final PathName namespaceOrAliasFQN) {
    final var fqn = resolvePossibleAlias(namespaceOrAliasFQN);
    EdmEntityContainer container = this.entityContainers.get(fqn);
    if (container == null) {
      container = createEntityContainer(fqn);
      if (container != null) {
        this.entityContainers.put(fqn, container);
        if (fqn == null) {
          this.entityContainers.put(container.getNamespace()
            .newChild(container.getName()), container);
        }
      }
    }
    return container;
  }

  public EdmEntityType getEntityType(final PathName namespaceOrAliasFQN) {
    final var pathName = resolvePossibleAlias(namespaceOrAliasFQN);
    EdmEntityType entityType = this.entityTypes.get(pathName);
    if (entityType == null) {
      entityType = createEntityType(pathName);
      if (entityType != null) {
        this.entityTypes.put(pathName, entityType);
      }
    }
    return entityType;
  }

  public EdmEntityType getEntityTypeWithAnnotations(final PathName namespaceOrAliasFQN) {
    final var fqn = resolvePossibleAlias(namespaceOrAliasFQN);
    EdmEntityType entityType = this.entityTypesWithAnnotations.get(fqn);
    if (entityType == null) {
      entityType = createEntityType(fqn);
      if (entityType != null) {
        this.entityTypesWithAnnotations.put(fqn, entityType);
      }
    }
    setIsPreviousES(false);
    return entityType;
  }

  protected EdmEntityType getEntityTypeWithAnnotations(final PathName namespaceOrAliasFQN,
    final boolean isEntityDerivedFromES) {
    this.isEntityDerivedFromES = isEntityDerivedFromES;
    final var fqn = resolvePossibleAlias(namespaceOrAliasFQN);
    if (!isPreviousES() && getEntityContainer() != null) {
      getEntityContainer().getEntitySetsWithAnnotations();
    }
    EdmEntityType entityType = this.entityTypesDerivedFromES.get(fqn);
    if (entityType == null) {
      entityType = createEntityType(fqn);
      if (entityType != null) {
        this.entityTypesDerivedFromES.put(fqn, entityType);
      }
    }
    this.isEntityDerivedFromES = false;
    return entityType;
  }

  public EdmEnumType getEnumType(final PathName namespaceOrAliasFQN) {
    final var fqn = resolvePossibleAlias(namespaceOrAliasFQN);
    EdmEnumType enumType = this.enumTypes.get(fqn);
    if (enumType == null) {
      enumType = createEnumType(fqn);
      if (enumType != null) {
        this.enumTypes.put(fqn, enumType);
      }
    }
    return enumType;
  }

  public Parser getParser() {
    return this.parser;
  }

  private List<CsdlFunction> getProviderFunction(final PathName name) {
    return this.provider.getFunctions(name);
  }

  public EdmSchema getSchema(final PathName namespace) {
    if (this.schemas == null) {
      initSchemas();
    }

    EdmSchema schema = this.schemas.get(namespace);
    if (schema == null) {
      schema = this.schemas.get(this.aliasToNamespaceInfo.getValue()
        .get(namespace));
    }
    return schema;
  }

  public List<EdmSchema> getSchemas() {
    if (this.schemaList == null) {
      initSchemas();
    }
    return this.schemaList;
  }

  public EdmTerm getTerm(final PathName termName) {
    final var fqn = resolvePossibleAlias(termName);
    EdmTerm term = this.terms.get(fqn);
    if (term == null) {
      term = createTerm(fqn);
      if (term != null) {
        this.terms.put(fqn, term);
      }
    }
    return term;
  }

  public List<CsdlSchema> getTermSchemaDefinitions() {
    return this.termSchemaDefinition;
  }

  public EdmTypeDefinition getTypeDefinition(final PathName namespaceOrAliasFQN) {
    final var fqn = resolvePossibleAlias(namespaceOrAliasFQN);
    EdmTypeDefinition typeDefinition = this.typeDefinitions.get(fqn);
    if (typeDefinition == null) {
      typeDefinition = createTypeDefinition(fqn);
      if (typeDefinition != null) {
        this.typeDefinitions.put(fqn, typeDefinition);
      }
    }
    return typeDefinition;
  }

  public EdmAction getUnboundAction(final PathName actionName) {
    final var fqn = resolvePossibleAlias(actionName);
    EdmAction action = this.unboundActions.get(fqn);
    if (action == null) {
      action = createUnboundAction(fqn);
      if (action != null) {
        this.unboundActions.put(actionName, action);
      }
    }

    return action;
  }

  public EdmFunction getUnboundFunction(final PathName functionName,
    final List<String> parameterNames) {
    final var functionFqn = resolvePossibleAlias(functionName);

    final FunctionMapKey key = new FunctionMapKey(functionFqn, null, null, parameterNames);
    EdmFunction function = this.unboundFunctionsByKey.get(key);
    if (function == null) {
      function = createUnboundFunction(functionFqn, parameterNames);
      if (function != null) {
        this.unboundFunctionsByKey.put(key, function);
      }
    }

    return function;
  }

  public List<EdmFunction> getUnboundFunctions(final PathName functionName) {
    final var functionFqn = resolvePossibleAlias(functionName);

    List<EdmFunction> functions = this.unboundFunctionsByName.get(functionFqn);
    if (functions == null) {
      functions = createUnboundFunctions(functionFqn);
      if (functions != null) {
        this.unboundFunctionsByName.put(functionFqn, functions);

        for (final EdmFunction unbound : functions) {
          final FunctionMapKey key = new FunctionMapKey(unbound.getNamespace()
            .newChild(unbound.getName()), unbound.getBindingParameterTypePathName(),
            unbound.isBindingParameterTypeCollection(), unbound.getParameterNames());
          this.unboundFunctionsByKey.put(key, unbound);
        }
      }
    }

    return functions;
  }

  private Map<PathName, PathName> initAliasToNamespaceInfo() {
    final Map<PathName, PathName> aliasToNamespaceInfos = new HashMap<>();
    final List<CsdlAliasInfo> aliasInfos = this.provider.getAliasInfos();
    if (aliasInfos != null) {
      for (final CsdlAliasInfo info : aliasInfos) {
        aliasToNamespaceInfos.put(info.getAlias(), info.getParent());
      }
    }
    return aliasToNamespaceInfos;
  }

  private void initSchemas() {
    final Map<PathName, EdmSchema> localSchemas = createSchemas();
    this.schemas = Collections.synchronizedMap(localSchemas);

    this.schemaList = Collections.unmodifiableList(new ArrayList<>(this.schemas.values()));
  }

  protected boolean isComplexDerivedFromES() {
    return this.isComplexDerivedFromES;
  }

  /**
   * @param bindingParameterTypeName
   * @param parameter
   * @param isBindingParameterCollection
   * @return
   * @throws ODataException
   */
  private boolean isComplexPreviousTypeCompatibleToBindingParam(
    final PathName bindingParameterTypeName, final CsdlParameter parameter,
    final Boolean isBindingParameterCollection) throws ODataException {
    final CsdlComplexType complexType = this.provider.getComplexType(bindingParameterTypeName);
    if (this.provider.getEntityType(parameter.getTypePathName()) == null) {
      return false;
    }
    final var properties = this.provider.getEntityType(parameter.getTypePathName())
      .getFields();
    for (final var property : properties) {
      final String paramPropertyTypeName = Edm.getTypeName(property)
        .toString();
      if (complexType != null && complexType.getBaseType() != null
        && complexType.getBaseTypePathName()
          .toString()
          .equals(paramPropertyTypeName)
        || paramPropertyTypeName.equals(bindingParameterTypeName.toDotSeparated())
          && isBindingParameterCollection.booleanValue() == property.isDataTypeCollection()) {
        return true;
      }
    }
    return false;
  }

  protected boolean isEntityDerivedFromES() {
    return this.isEntityDerivedFromES;
  }

  /**
   * @param bindingParameterTypeName
   * @param parameter
   * @return
   * @throws ODataException
   */
  private boolean isEntityPreviousTypeCompatibleToBindingParam(
    final PathName bindingParameterTypeName, final CsdlParameter parameter) throws ODataException {
    return this.provider.getEntityType(bindingParameterTypeName) != null
      && this.provider.getEntityType(bindingParameterTypeName)
        .getBaseTypePathName() != null
      && this.provider.getEntityType(bindingParameterTypeName)
        .getBaseTypePathName()
        .equals(parameter.getTypePathName());
  }

  protected boolean isPreviousES() {
    return this.isPreviousES;
  }

  /**
   * Populates a map of String (annotation target) and List of CsdlAnnotations
   * Reads both term definition schema (external schema) and
   * provider schema (actual metadata file)
   */
  private void populateAnnotationMap() {
    for (final CsdlSchema schema : this.termSchemaDefinition) {
      fetchAnnotationsInMetadataAndExternalFile(schema);
    }
    try {
      if (null != this.provider.getSchemas()) {
        for (final CsdlSchema schema : this.provider.getSchemas()) {
          fetchAnnotationsInMetadataAndExternalFile(schema);
        }
      }
    } catch (final ODataException e) {
      throw new EdmException(e);
    }
  }

  /**
   * Remove the annotations added to navigation properties
   * of a complex type loaded via entity set path
   * @param structuralType
   * @param typeName
   * @param csdlEntityContainer
   * @param navProperties
   * @param entitySets
   */
  private void removeAnnotationsAddedToCTNavPropFromES(final CsdlStructuralType<?> structuralType,
    final PathName typeName, final CsdlEntityContainer csdlEntityContainer,
    final List<CsdlNavigationProperty> navProperties, final List<CsdlEntitySet> entitySets) {
    String containerName;
    PathName schemaName;
    String complexPropName;
    for (final CsdlEntitySet entitySet : entitySets) {
      try {
        final CsdlEntityType entType = this.provider.getEntityType(entitySet.getTypePathName());
        final var fields = null != entType ? entType.getFields() : new ArrayList<FieldDefinition>();
        for (final var field : fields) {
          if (Edm.getTypeName(field)
            .equals(typeName)) {
            complexPropName = field.getName();
            containerName = csdlEntityContainer.getName();
            schemaName = typeName.getParent();
            for (final CsdlNavigationProperty navProperty : navProperties) {
              final List<CsdlAnnotation> annotPropDerivedFromES = getAnnotationsMap()
                .get(schemaName + "." + containerName + "/" + entitySet.getName() + "/"
                  + complexPropName + "/" + navProperty.getName());
              removeAnnotationsOnNavPropDerivedFromEntitySet(structuralType, navProperty,
                annotPropDerivedFromES);
              var aliasName = getAliasInfo(schemaName);
              final List<CsdlAnnotation> annotPropDerivedFromESOnAlias = getAnnotationsMap()
                .get(aliasName.toDotSeparated() + "." + containerName + "/" + entitySet.getName()
                  + "/" + complexPropName + "/" + navProperty.getName());
              removeAnnotationsOnNavPropDerivedFromEntitySet(structuralType, navProperty,
                annotPropDerivedFromESOnAlias);

              final List<CsdlAnnotation> propAnnotations = getAnnotationsMap()
                .get(typeName.toDotSeparated() + "/" + navProperty.getName());
              addAnnotationsOnNavProperties(structuralType, navProperty, propAnnotations);
              aliasName = getAliasInfo(typeName.getParent());
              final List<CsdlAnnotation> propAnnotationsOnAlias = getAnnotationsMap()
                .get(aliasName.toDotSeparated() + "." + typeName.getName() + "/"
                  + navProperty.getName());
              addAnnotationsOnNavProperties(structuralType, navProperty, propAnnotationsOnAlias);
            }
          }
        }
      } catch (final ODataException e) {
        throw new EdmException(e);
      }
    }
    for (final CsdlNavigationProperty navProperty : structuralType.getNavigationProperties()) {
      final List<CsdlAnnotation> propAnnotations = getAnnotationsMap()
        .get(typeName.toDotSeparated() + "/" + navProperty.getName());
      addAnnotationsOnNavProperties(structuralType, navProperty, propAnnotations);
      final var aliasName = getAliasInfo(typeName.getParent());
      final List<CsdlAnnotation> propAnnotationsOnAlias = getAnnotationsMap()
        .get(aliasName.toDotSeparated() + "." + typeName.getName() + "/" + navProperty.getName());
      addAnnotationsOnNavProperties(structuralType, navProperty, propAnnotationsOnAlias);
    }
  }

  /**
   * Removes the annotation added on complex type property via Entity Set
   * @param structuralType
   * @param typeName
   * @param csdlEntityContainer
   * @param properties
   * @param entitySets
   */
  private void removeAnnotationsAddedToCTTypePropFromES(final CsdlStructuralType<?> structuralType,
    final PathName typeName, final CsdlEntityContainer csdlEntityContainer,
    final List<FieldDefinition> properties, final List<CsdlEntitySet> entitySets) {
    String containerName;
    PathName schemaName;
    String complexPropName;
    for (final CsdlEntitySet entitySet : entitySets) {
      try {
        final CsdlEntityType entType = this.provider.getEntityType(entitySet.getTypePathName());
        final var entTypeProperties = null != entType ? entType.getFields()
          : new ArrayList<FieldDefinition>();
        for (final var entTypeProperty : entTypeProperties) {
          if (null != getType(entTypeProperty)
            && getType(entTypeProperty).endsWith("." + structuralType.getName())) {
            complexPropName = entTypeProperty.getName();
            containerName = csdlEntityContainer.getName();
            schemaName = typeName.getParent();
            for (final var property : properties) {
              final var name = property.getName();
              final List<CsdlAnnotation> annotPropDerivedFromES = getAnnotationsMap()
                .get(schemaName + "." + containerName + "/" + entitySet.getName() + "/"
                  + complexPropName + "/" + name);
              removeAnnotationsOnPropDerivedFromEntitySet(structuralType, property,
                annotPropDerivedFromES);
              var aliasName = getAliasInfo(schemaName);
              final List<CsdlAnnotation> annotPropDerivedFromESOnAlias = getAnnotationsMap()
                .get(aliasName.toDotSeparated() + "." + containerName + "/" + entitySet.getName()
                  + "/" + complexPropName + "/" + name);
              removeAnnotationsOnPropDerivedFromEntitySet(structuralType, property,
                annotPropDerivedFromESOnAlias);

              final List<CsdlAnnotation> propAnnotations = getAnnotationsMap()
                .get(typeName.toDotSeparated() + "/" + name);
              addAnnotationsOnPropertiesOfStructuralType(structuralType, name, propAnnotations);
              aliasName = getAliasInfo(typeName.getParent());
              final List<CsdlAnnotation> propAnnotationsOnAlias = getAnnotationsMap()
                .get(typeName.getName() + "/" + name);
              addAnnotationsOnPropertiesOfStructuralType(structuralType, name,
                propAnnotationsOnAlias);
            }
          }
        }
      } catch (final ODataException e) {
        throw new EdmException(e);
      }
    }
  }

  /**
   * Removes the annotations added to properties of structural types
   * if annotation was added before via EntitySet path
   * @param structuralType
   * @param navProperty
   * @param annotPropDerivedFromES
   */
  private void removeAnnotationsOnNavPropDerivedFromEntitySet(
    final CsdlStructuralType<?> structuralType, final CsdlNavigationProperty navProperty,
    final List<CsdlAnnotation> annotPropDerivedFromES) {
    if (null != annotPropDerivedFromES && !annotPropDerivedFromES.isEmpty()) {
      for (final CsdlAnnotation annotation : annotPropDerivedFromES) {
        final List<CsdlAnnotation> propAnnot = structuralType
          .getNavigationProperty(navProperty.getName())
          .getAnnotations();
        if (propAnnot.contains(annotation)) {
          propAnnot.remove(annotation);
        }
      }
    }
  }

  /**
   * Removes the annotations added to properties of entity type when added via entity set
   * @param structuralType
   * @param property
   * @param annotPropDerivedFromESOnAlias
   */
  private void removeAnnotationsOnPropDerivedFromEntitySet(
    final CsdlStructuralType<?> structuralType, final FieldDefinition property,
    final List<CsdlAnnotation> annotPropDerivedFromES) {
    if (null != annotPropDerivedFromES && !annotPropDerivedFromES.isEmpty()) {
      for (final CsdlAnnotation annotation : annotPropDerivedFromES) {
        final List<CsdlAnnotation> propAnnot = structuralType
          .getFieldAnnotations(property.getName());
        if (propAnnot.contains(annotation)) {
          propAnnot.remove(annotation);
        }
      }
    }
  }

  private PathName resolvePossibleAlias(final PathName namespaceOrAliasFQN) {
    if (namespaceOrAliasFQN != null) {
      final PathName namespace = this.aliasToNamespaceInfo.getValue()
        .get(namespaceOrAliasFQN.getParent());
      // If not contained in info it must be a namespace
      if (namespace == null) {
        return namespaceOrAliasFQN;
      } else {
        return namespace.newChild(namespaceOrAliasFQN.getName());
      }
    }
    return null;
  }

  public void setIsPreviousES(final boolean isPreviousES) {
    this.isPreviousES = isPreviousES;
  }

  /** Check if annotations are added on navigation properties of a structural type
   * @param structuralType
   * @param typeName
   * @param csdlEntityContainer
   * @param isNavPropAnnotationsCleared
   * @param annotationGrp
   */
  private void updateAnnotationsOnStructuralNavProperties(
    final CsdlStructuralType<?> structuralType, final PathName typeName,
    final CsdlEntityContainer csdlEntityContainer) {
    final List<CsdlNavigationProperty> navProperties = structuralType.getNavigationProperties();
    String containerName = null;
    PathName schemaName = null;
    String entitySetName = null;
    final List<CsdlEntitySet> entitySets = csdlEntityContainer != null
      ? csdlEntityContainer.getEntitySets()
      : new ArrayList<>();
    if (structuralType instanceof CsdlComplexType) {
      removeAnnotationsAddedToCTNavPropFromES(structuralType, typeName, csdlEntityContainer,
        navProperties, entitySets);
    } else {
      for (final CsdlEntitySet entitySet : entitySets) {
        entitySetName = entitySet.getName();
        final String entityTypeName = entitySet.getTypePathName()
          .toDotSeparated();
        if (null != entityTypeName && entityTypeName.equalsIgnoreCase(typeName.toDotSeparated())) {
          containerName = csdlEntityContainer.getName();
          schemaName = typeName.getParent();
          break;
        }
      }
      for (final CsdlNavigationProperty navProperty : navProperties) {
        final List<CsdlAnnotation> annotPropDerivedFromES = getAnnotationsMap().get(
          schemaName + "." + containerName + "/" + entitySetName + "/" + navProperty.getName());
        removeAnnotationsOnNavPropDerivedFromEntitySet(structuralType, navProperty,
          annotPropDerivedFromES);
        PathName aliasName = getAliasInfo(schemaName);
        final List<CsdlAnnotation> annotPropDerivedFromESOnAlias = getAnnotationsMap()
          .get(aliasName + "." + containerName + "/" + entitySetName + "/" + navProperty.getName());
        removeAnnotationsOnNavPropDerivedFromEntitySet(structuralType, navProperty,
          annotPropDerivedFromESOnAlias);

        final List<CsdlAnnotation> navPropAnnotations = getAnnotationsMap()
          .get(typeName.toDotSeparated() + "/" + navProperty.getName());
        addAnnotationsOnNavProperties(structuralType, navProperty, navPropAnnotations);
        aliasName = getAliasInfo(typeName.getParent());
        final List<CsdlAnnotation> navPropAnnotationsOnAlias = getAnnotationsMap()
          .get(aliasName + "." + typeName.getName() + "/" + navProperty.getName());
        addAnnotationsOnNavProperties(structuralType, navProperty, navPropAnnotationsOnAlias);
      }
    }
  }

  /** Check if annotations are added on properties of a structural type
   * @param structuralType
   * @param typeName
   * @param csdlEntityContainer
   */
  private void updateAnnotationsOnStructuralProperties(final CsdlStructuralType<?> structuralType,
    final PathName typeName, final CsdlEntityContainer csdlEntityContainer) {
    final var properties = structuralType.getFields();
    String containerName = null;
    PathName schemaName = null;
    String entitySetName = null;
    final List<CsdlEntitySet> entitySets = null != csdlEntityContainer
      ? csdlEntityContainer.getEntitySets()
      : new ArrayList<>();
    if (structuralType instanceof CsdlComplexType) {
      removeAnnotationsAddedToCTTypePropFromES(structuralType, typeName, csdlEntityContainer,
        properties, entitySets);
    } else {
      for (final CsdlEntitySet entitySet : entitySets) {
        entitySetName = entitySet.getName();
        final String entityTypeName = entitySet.getTypePathName()
          .toDotSeparated();
        if (null != entityTypeName && entityTypeName.equalsIgnoreCase(typeName.toDotSeparated())) {
          containerName = csdlEntityContainer.getName();
          schemaName = typeName.getParent();
          break;
        }
      }
      for (final var property : properties) {
        final var name = property.getName();
        final List<CsdlAnnotation> annotPropDerivedFromES = getAnnotationsMap()
          .get(schemaName + "." + containerName + "/" + entitySetName + "/" + name);
        removeAnnotationsOnPropDerivedFromEntitySet(structuralType, property,
          annotPropDerivedFromES);
        var aliasName = getAliasInfo(schemaName);
        final List<CsdlAnnotation> annotPropDerivedFromESOnAlias = getAnnotationsMap()
          .get(aliasName.toDotSeparated() + "." + containerName + "/" + entitySetName + "/" + name);
        removeAnnotationsOnPropDerivedFromEntitySet(structuralType, property,
          annotPropDerivedFromESOnAlias);
        final List<CsdlAnnotation> propAnnotations = getAnnotationsMap()
          .get(typeName.toDotSeparated() + "/" + name);
        addAnnotationsOnPropertiesOfStructuralType(structuralType, name, propAnnotations);
        aliasName = getAliasInfo(typeName.getParent());
        final List<CsdlAnnotation> propAnnotationsOnAlias = getAnnotationsMap()
          .get(aliasName.toDotSeparated() + "." + typeName.getName() + "/" + name);
        addAnnotationsOnPropertiesOfStructuralType(structuralType, name, propAnnotationsOnAlias);
      }
    }
  }
}
