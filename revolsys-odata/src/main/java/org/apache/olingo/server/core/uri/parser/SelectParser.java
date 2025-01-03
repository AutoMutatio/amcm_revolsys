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
package org.apache.olingo.server.core.uri.parser;

import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmAction;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmFunction;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.core.edm.Edm;
import org.apache.olingo.server.api.uri.UriInfoKind;
import org.apache.olingo.server.api.uri.queryoption.SelectItem;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.core.uri.UriInfoImpl;
import org.apache.olingo.server.core.uri.UriResourceAction;
import org.apache.olingo.server.core.uri.UriResourceComplexProperty;
import org.apache.olingo.server.core.uri.UriResourceFunction;
import org.apache.olingo.server.core.uri.UriResourceNavigationProperty;
import org.apache.olingo.server.core.uri.UriResourcePartTyped;
import org.apache.olingo.server.core.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.core.uri.parser.UriTokenizer.TokenKind;
import org.apache.olingo.server.core.uri.queryoption.SelectItemImpl;
import org.apache.olingo.server.core.uri.queryoption.SelectOptionImpl;
import org.apache.olingo.server.core.uri.validator.UriValidationException;

import com.revolsys.io.PathName;

public class SelectParser {

  private final Edm edm;

  public SelectParser(final Edm edm) {
    this.edm = edm;
  }

  private void addSelectPath(final UriTokenizer tokenizer, final EdmStructuredType referencedType,
    final UriInfoImpl resource) throws UriParserException {
    final String name = tokenizer.getText();
    final EdmProperty property = referencedType.getStructuralProperty(name);

    if (property == null) {
      final EdmNavigationProperty navigationProperty = referencedType.getNavigationProperty(name);
      if (navigationProperty == null) {
        throw new UriParserSemanticException("Selected property not found.",
          UriParserSemanticException.MessageKeys.EXPRESSION_PROPERTY_NOT_IN_TYPE,
          referencedType.getName(), name);
      } else {
        resource.addResourcePart(new UriResourceNavigationProperty(navigationProperty));
      }

    } else if (property.isPrimitive() || property.getType()
      .getKind() == EdmTypeKind.ENUM || property.getType()
        .getKind() == EdmTypeKind.DEFINITION) {
      resource.addResourcePart(new UriResourcePrimitiveProperty(property, null));

    } else {
      final UriResourceComplexProperty complexPart = new UriResourceComplexProperty(property);
      resource.addResourcePart(complexPart);
      if (tokenizer.next(TokenKind.SLASH)) {
        if (tokenizer.next(TokenKind.QualifiedName)) {
          final var qualifiedName = PathName.fromDotSeparated(tokenizer.getText());
          final EdmComplexType type = this.edm.getComplexType(qualifiedName);
          if (type == null) {
            throw new UriParserSemanticException("Type not found.",
              UriParserSemanticException.MessageKeys.UNKNOWN_TYPE, qualifiedName.toString());
          } else if (type.compatibleTo(property.getType())) {
            complexPart.setTypeFilter(type);
            if (tokenizer.next(TokenKind.SLASH)) {
              if (tokenizer.next(TokenKind.ODataIdentifier)) {
                addSelectPath(tokenizer, type, resource);
              } else {
                throw new UriParserSemanticException("Unknown part after '/'.",
                  UriParserSemanticException.MessageKeys.UNKNOWN_PART, "");
              }
            }
          } else {
            throw new UriParserSemanticException("The type cast is not compatible.",
              UriParserSemanticException.MessageKeys.INCOMPATIBLE_TYPE_FILTER, type.getName());
          }
        } else if (tokenizer.next(TokenKind.ODataIdentifier)) {
          addSelectPath(tokenizer, (EdmStructuredType)property.getType(), resource);
        } else if (tokenizer.next(TokenKind.SLASH)) {
          throw new UriParserSyntaxException("Illegal $select expression.",
            UriParserSyntaxException.MessageKeys.SYNTAX);
        } else {
          throw new UriParserSemanticException("Unknown part after '/'.",
            UriParserSemanticException.MessageKeys.UNKNOWN_PART, "");
        }
      }
    }
  }

  private void ensureReferencedTypeNotNull(final EdmStructuredType referencedType)
    throws UriParserException {
    if (referencedType == null) {
      throw new UriParserSemanticException("The referenced part is not typed.",
        UriParserSemanticException.MessageKeys.ONLY_FOR_TYPED_PARTS, "select");
    }
  }

  public SelectOption parse(final UriTokenizer tokenizer, final EdmStructuredType referencedType,
    final boolean referencedIsCollection) throws UriParserException, UriValidationException {
    final List<SelectItem> selectItems = new ArrayList<>();
    SelectItem item;
    do {
      item = parseItem(tokenizer, referencedType, referencedIsCollection);
      selectItems.add(item);
    } while (tokenizer.next(TokenKind.COMMA));

    return new SelectOptionImpl().setSelectItems(selectItems);
  }

  private PathName parseAllOperationsInSchema(final UriTokenizer tokenizer)
    throws UriParserException {
    final PathName namespace = PathName.fromDotSeparated(tokenizer.getText());
    if (tokenizer.next(TokenKind.DOT)) {
      if (tokenizer.next(TokenKind.STAR)) {
        // Validate the namespace. Currently a namespace from a non-default
        // schema is not supported.
        // There is no direct access to the namespace without loading the whole
        // schema;
        // however, the default entity container should always be there, so its
        // access methods can be used.
        if (this.edm.getEntityContainer(namespace.newChild(this.edm.getEntityContainer()
          .getName())) == null) {
          throw new UriParserSemanticException(
            "Wrong namespace '" + namespace.toDotSeparated() + "'.",
            UriParserSemanticException.MessageKeys.UNKNOWN_PART, namespace.toDotSeparated());
        }
        return namespace.newChild(tokenizer.getText());
      } else {
        throw new UriParserSemanticException("Expected star after dot.",
          UriParserSemanticException.MessageKeys.UNKNOWN_PART, "");
      }
    }
    return null;
  }

  private UriResourcePartTyped parseBoundOperation(final UriTokenizer tokenizer,
    final PathName qualifiedName, final EdmStructuredType referencedType,
    final boolean referencedIsCollection) throws UriParserException {
    final EdmAction boundAction = this.edm.getBoundAction(qualifiedName,
      referencedType.getPathName(), referencedIsCollection);
    if (boundAction == null) {
      final List<String> parameterNames = parseFunctionParameterNames(tokenizer);
      final EdmFunction boundFunction = this.edm.getBoundFunction(qualifiedName,
        referencedType.getPathName(), referencedIsCollection, parameterNames);
      if (boundFunction == null) {
        throw new UriParserSemanticException("Function not found.",
          UriParserSemanticException.MessageKeys.UNKNOWN_PART, qualifiedName.toString());
      } else {
        return new UriResourceFunction(null, boundFunction, null);
      }
    } else {
      return new UriResourceAction(boundAction);
    }
  }

  private List<String> parseFunctionParameterNames(final UriTokenizer tokenizer)
    throws UriParserException {
    final List<String> names = new ArrayList<>();
    if (tokenizer.next(TokenKind.OPEN)) {
      do {
        ParserHelper.requireNext(tokenizer, TokenKind.ODataIdentifier);
        names.add(tokenizer.getText());
      } while (tokenizer.next(TokenKind.COMMA));
      ParserHelper.requireNext(tokenizer, TokenKind.CLOSE);
    }
    return names;
  }

  private SelectItem parseItem(final UriTokenizer tokenizer, final EdmStructuredType referencedType,
    final boolean referencedIsCollection) throws UriParserException {
    final SelectItemImpl item = new SelectItemImpl();
    if (tokenizer.next(TokenKind.STAR)) {
      item.setStar(true);

    } else if (tokenizer.next(TokenKind.QualifiedName)) {
      // The namespace or its alias could consist of dot-separated OData
      // identifiers.
      final var allOperationsInSchema = parseAllOperationsInSchema(tokenizer);
      if (allOperationsInSchema != null) {
        item.addAllOperationsInSchema(allOperationsInSchema);

      } else {
        ensureReferencedTypeNotNull(referencedType);
        final var qualifiedName = PathName.fromDotSeparated(tokenizer.getText());
        EdmStructuredType type = this.edm.getEntityType(qualifiedName);
        if (type == null) {
          type = this.edm.getComplexType(qualifiedName);
        }
        if (type == null) {
          item.setResourcePath(new UriInfoImpl().setKind(UriInfoKind.resource)
            .addResourcePart(parseBoundOperation(tokenizer, qualifiedName, referencedType,
              referencedIsCollection)));

        } else {
          if (type.compatibleTo(referencedType)) {
            item.setTypeFilter(type);
            if (tokenizer.next(TokenKind.SLASH)) {
              ParserHelper.requireNext(tokenizer, TokenKind.ODataIdentifier);
              final UriInfoImpl resource = new UriInfoImpl().setKind(UriInfoKind.resource);
              addSelectPath(tokenizer, type, resource);
              item.setResourcePath(resource);
            }
          } else {
            throw new UriParserSemanticException("The type cast is not compatible.",
              UriParserSemanticException.MessageKeys.INCOMPATIBLE_TYPE_FILTER, type.getName());
          }
        }
      }

    } else {
      ParserHelper.requireNext(tokenizer, TokenKind.ODataIdentifier);
      // The namespace or its alias could be a single OData identifier.
      final var allOperationsInSchema = parseAllOperationsInSchema(tokenizer);
      if (allOperationsInSchema != null) {
        item.addAllOperationsInSchema(allOperationsInSchema);

      } else {
        ensureReferencedTypeNotNull(referencedType);
        final UriInfoImpl resource = new UriInfoImpl().setKind(UriInfoKind.resource);
        addSelectPath(tokenizer, referencedType, resource);
        item.setResourcePath(resource);
      }
    }

    return item;
  }
}
