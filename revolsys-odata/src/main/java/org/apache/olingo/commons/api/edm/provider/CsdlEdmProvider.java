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

import java.util.List;

import org.apache.olingo.commons.api.ex.ODataException;

import com.revolsys.collection.list.Lists;
import com.revolsys.io.PathName;

/**
 * The interface Csdl edm provider.
 */
public class CsdlEdmProvider {

  public CsdlActionImport getActionImport(final PathName entityContainer,
    final String actionImportName) throws ODataException {
    return null;
  }

  public List<CsdlAction> getActions(final PathName actionName) throws ODataException {
    return null;
  }

  public List<CsdlAliasInfo> getAliasInfos() throws ODataException {
    return Lists.empty();
  }

  public CsdlAnnotations getAnnotationsGroup(final PathName targetName, final String qualifier)
    throws ODataException {
    return null;
  }

  public CsdlComplexType getComplexType(final PathName complexTypeName) throws ODataException {
    return null;
  }

  public CsdlEntityContainer getEntityContainer() throws ODataException {
    return null;
  }

  public CsdlEntityContainerInfo getEntityContainerInfo(final PathName entityContainerName)
    throws ODataException {
    return null;
  }

  public CsdlEntitySet getEntitySet(final PathName entityContainer, final String entitySetName)
    throws ODataException {
    return null;
  }

  public CsdlEntityType getEntityType(final PathName entityTypeName) throws ODataException {
    return null;
  }

  public CsdlEnumType getEnumType(final PathName enumTypeName) throws ODataException {
    return null;
  }

  public CsdlFunctionImport getFunctionImport(final PathName entityContainer,
    final String functionImportName) throws ODataException {
    return null;
  }

  public List<CsdlFunction> getFunctions(final PathName functionName) throws ODataException {
    return null;
  }

  public List<CsdlSchema> getSchemas() throws ODataException {
    return null;
  }

  public CsdlSingleton getSingleton(final PathName entityContainer, final String singletonName)
    throws ODataException {
    return null;
  }

  public CsdlTerm getTerm(final PathName termName) throws ODataException {
    return null;
  }

  public CsdlTypeDefinition getTypeDefinition(final PathName typeDefinitionName)
    throws ODataException {
    return null;
  }
}
