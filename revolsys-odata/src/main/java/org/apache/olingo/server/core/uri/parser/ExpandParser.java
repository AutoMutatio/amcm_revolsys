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

import java.util.Collection;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.core.edm.Edm;
import org.apache.olingo.server.api.uri.UriInfoKind;
import org.apache.olingo.server.api.uri.queryoption.AliasQueryOption;
import org.apache.olingo.server.api.uri.queryoption.ExpandItem;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.LevelsExpandOption;
import org.apache.olingo.server.api.uri.queryoption.SystemQueryOption;
import org.apache.olingo.server.api.uri.queryoption.SystemQueryOptionKind;
import org.apache.olingo.server.core.uri.UriInfoImpl;
import org.apache.olingo.server.core.uri.UriResourceComplexProperty;
import org.apache.olingo.server.core.uri.UriResourceCount;
import org.apache.olingo.server.core.uri.UriResourceEntitySet;
import org.apache.olingo.server.core.uri.UriResourceNavigationProperty;
import org.apache.olingo.server.core.uri.UriResourcePartTyped;
import org.apache.olingo.server.core.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.core.uri.UriResourceRef;
import org.apache.olingo.server.core.uri.parser.UriTokenizer.TokenKind;
import org.apache.olingo.server.core.uri.queryoption.CountOptionImpl;
import org.apache.olingo.server.core.uri.queryoption.ExpandItemImpl;
import org.apache.olingo.server.core.uri.queryoption.ExpandOptionImpl;
import org.apache.olingo.server.core.uri.queryoption.LevelsOptionImpl;
import org.apache.olingo.server.core.uri.queryoption.SkipOptionImpl;
import org.apache.olingo.server.core.uri.queryoption.TopOptionImpl;
import org.apache.olingo.server.core.uri.queryoption.apply.DynamicStructuredType;
import org.apache.olingo.server.core.uri.validator.UriValidationException;

public class ExpandParser {

  protected static UriInfoImpl parseExpandPath(final UriTokenizer tokenizer, final Edm edm,
    final EdmStructuredType referencedType, final ExpandItemImpl item) throws UriParserException {
    final UriInfoImpl resource = new UriInfoImpl().setKind(UriInfoKind.resource);

    EdmStructuredType type = referencedType;
    String name = null;
    while (tokenizer.next(TokenKind.ODataIdentifier)) {
      name = tokenizer.getText();
      final EdmProperty property = type.getStructuralProperty(name);
      if (property != null && property.getType()
        .getKind() == EdmTypeKind.COMPLEX) {
        type = (EdmStructuredType)property.getType();
        final UriResourceComplexProperty complexResource = new UriResourceComplexProperty(property);
        ParserHelper.requireNext(tokenizer, TokenKind.SLASH);
        final EdmStructuredType typeCast = ParserHelper.parseTypeCast(tokenizer, edm, type);
        if (typeCast != null) {
          complexResource.setTypeFilter(typeCast);
          ParserHelper.requireNext(tokenizer, TokenKind.SLASH);
          type = typeCast;
        }
        resource.addResourcePart(complexResource);
      }
    }

    final EdmNavigationProperty navigationProperty = type.getNavigationProperty(name);
    if (navigationProperty == null) {
      // For handling $expand with Stream Properties in version 4.01
      final EdmProperty streamProperty = (EdmProperty)type.getProperty(name);
      if (streamProperty != null && streamProperty.getType() == EdmPrimitiveTypeKind.Stream) {
        resource.addResourcePart(new UriResourcePrimitiveProperty(streamProperty, null));
      } else if (tokenizer.next(TokenKind.STAR)) {
        item.setIsStar(true);
      } else {
        throw new UriParserSemanticException(
          "Navigation Property '" + name + "' not found in type '" + type.getPathName()
            + "'.",
          UriParserSemanticException.MessageKeys.EXPRESSION_PROPERTY_NOT_IN_TYPE, type.getName(),
          name);
      }
    } else {
      resource.addResourcePart(new UriResourceNavigationProperty(navigationProperty));
    }

    return resource;
  }

  private final Edm edm;

  private final Map<String, AliasQueryOption> aliases;

  private final Collection<String> crossjoinEntitySetNames;

  public ExpandParser(final Edm edm, final Map<String, AliasQueryOption> aliases,
    final Collection<String> crossjoinEntitySetNames) {
    this.edm = edm;
    this.aliases = aliases;
    this.crossjoinEntitySetNames = crossjoinEntitySetNames;
  }

  public ExpandOption parse(final UriTokenizer tokenizer, final EdmStructuredType referencedType)
    throws UriParserException, UriValidationException {
    final ExpandOptionImpl expandOption = new ExpandOptionImpl();
    do {
      // In the crossjoin case the start has to be an EntitySet name which will
      // dictate the reference type
      if (this.crossjoinEntitySetNames != null && !this.crossjoinEntitySetNames.isEmpty()) {
        final ExpandItem item = parseCrossJoinItem(tokenizer);
        expandOption.addExpandItem(item);
      } else {
        final ExpandItem item = parseItem(tokenizer, referencedType);
        expandOption.addExpandItem(item);
      }
    } while (tokenizer.next(TokenKind.COMMA));

    return expandOption;
  }

  private ExpandItem parseCrossJoinItem(final UriTokenizer tokenizer)
    throws UriParserSemanticException {
    final ExpandItemImpl item = new ExpandItemImpl();
    if (tokenizer.next(TokenKind.STAR)) {
      item.setIsStar(true);
    } else if (tokenizer.next(TokenKind.ODataIdentifier)) {
      final String entitySetName = tokenizer.getText();
      if (this.crossjoinEntitySetNames.contains(entitySetName)) {
        final UriInfoImpl resource = new UriInfoImpl().setKind(UriInfoKind.resource);
        final UriResourceEntitySet entitySetResourceSegment = new UriResourceEntitySet(
          this.edm.getEntityContainer()
            .getEntitySet(entitySetName));
        resource.addResourcePart(entitySetResourceSegment);

        item.setResourcePath(resource);
      } else {
        throw new UriParserSemanticException("Unknown crossjoin entity set.",
          UriParserSemanticException.MessageKeys.UNKNOWN_PART, entitySetName);
      }
    } else {
      throw new UriParserSemanticException(
        "If the target resource is a crossjoin an entity set is " + "needed as the starting point.",
        UriParserSemanticException.MessageKeys.UNKNOWN_PART);
    }
    return item;
  }

  private ExpandItem parseItem(final UriTokenizer tokenizer, final EdmStructuredType referencedType)
    throws UriParserException, UriValidationException {
    final ExpandItemImpl item = new ExpandItemImpl();
    if (tokenizer.next(TokenKind.STAR)) {
      item.setIsStar(true);
      if (tokenizer.next(TokenKind.SLASH)) {
        ParserHelper.requireNext(tokenizer, TokenKind.REF);
        item.setIsRef(true);
      } else if (tokenizer.next(TokenKind.OPEN)) {
        ParserHelper.requireNext(tokenizer, TokenKind.LEVELS);
        ParserHelper.requireNext(tokenizer, TokenKind.EQ);
        item.setSystemQueryOption((SystemQueryOption)parseLevels(tokenizer));
        ParserHelper.requireNext(tokenizer, TokenKind.CLOSE);
      }

    } else {
      final EdmStructuredType typeCast = ParserHelper.parseTypeCast(tokenizer, this.edm,
        referencedType);
      if (typeCast != null) {
        item.setTypeFilter(typeCast);
        ParserHelper.requireNext(tokenizer, TokenKind.SLASH);
      }

      final UriInfoImpl resource = parseExpandPath(tokenizer, this.edm, referencedType, item);

      final UriResourcePartTyped lastPart = (UriResourcePartTyped)resource.getLastResourcePart();

      boolean hasSlash = false;
      EdmStructuredType typeCastSuffix = null;
      if (tokenizer.next(TokenKind.SLASH)) {
        hasSlash = true;
        if (lastPart instanceof UriResourceNavigationProperty) {
          final UriResourceNavigationProperty navigationResource = (UriResourceNavigationProperty)lastPart;
          final EdmNavigationProperty navigationProperty = navigationResource.getProperty();
          typeCastSuffix = ParserHelper.parseTypeCast(tokenizer, this.edm,
            navigationProperty.getType());
          if (typeCastSuffix != null) {
            if (navigationProperty.isCollection()) {
              navigationResource.setCollectionTypeFilter(typeCastSuffix);
            } else {
              navigationResource.setEntryTypeFilter(typeCastSuffix);
            }
            hasSlash = false;
          }
        }
      }
      // For handling $expand for Stream property in v 4.01
      if (lastPart instanceof UriResourcePrimitiveProperty) {
        item.setResourcePath(resource);
      } else {
        final EdmStructuredType newReferencedType = typeCastSuffix != null ? typeCastSuffix
          : (EdmStructuredType)lastPart.getType();
        final boolean newReferencedIsCollection = lastPart.isCollection();
        if (hasSlash || tokenizer.next(TokenKind.SLASH)) {
          if (tokenizer.next(TokenKind.REF)) {
            resource.addResourcePart(new UriResourceRef());
            item.setIsRef(true);
            parseOptions(tokenizer, newReferencedType, newReferencedIsCollection, item, true,
              false);
          } else {
            ParserHelper.requireNext(tokenizer, TokenKind.COUNT);
            resource.addResourcePart(new UriResourceCount());
            item.setCountPath(true);
            parseOptions(tokenizer, newReferencedType, newReferencedIsCollection, item, false,
              true);
          }
        } else {
          parseOptions(tokenizer, newReferencedType, newReferencedIsCollection, item, false, false);
        }

        item.setResourcePath(resource);
      }
    }

    return item;
  }

  private LevelsExpandOption parseLevels(final UriTokenizer tokenizer) throws UriParserException {
    final LevelsOptionImpl option = new LevelsOptionImpl();
    if (tokenizer.next(TokenKind.MAX)) {
      option.setText(tokenizer.getText());
      option.setMax();
    } else {
      ParserHelper.requireNext(tokenizer, TokenKind.IntegerValue);
      option.setText(tokenizer.getText());
      option.setValue(ParserHelper.parseNonNegativeInteger(SystemQueryOptionKind.LEVELS.toString(),
        tokenizer.getText(), false));
    }
    return option;
  }

  private void parseOptions(final UriTokenizer tokenizer, final EdmStructuredType referencedType,
    final boolean referencedIsCollection, final ExpandItemImpl item, final boolean forRef,
    final boolean forCount) throws UriParserException, UriValidationException {
    if (tokenizer.next(TokenKind.OPEN)) {
      do {
        SystemQueryOption systemQueryOption;
        if (!forCount && tokenizer.next(TokenKind.COUNT)) {
          ParserHelper.requireNext(tokenizer, TokenKind.EQ);
          ParserHelper.requireNext(tokenizer, TokenKind.BooleanValue);
          final CountOptionImpl countOption = new CountOptionImpl();
          countOption.setText(tokenizer.getText());
          countOption.setValue(Boolean.parseBoolean(tokenizer.getText()));
          systemQueryOption = countOption;

        } else if (!forRef && !forCount && tokenizer.next(TokenKind.EXPAND)) {
          ParserHelper.requireNext(tokenizer, TokenKind.EQ);
          systemQueryOption = new ExpandParser(this.edm, this.aliases, null).parse(tokenizer,
            referencedType);

        } else if (tokenizer.next(TokenKind.FILTER)) {
          ParserHelper.requireNext(tokenizer, TokenKind.EQ);
          systemQueryOption = new FilterParser(this.edm).parse(tokenizer, referencedType, null,
            this.aliases);

        } else if (!forRef && !forCount && tokenizer.next(TokenKind.LEVELS)) {
          ParserHelper.requireNext(tokenizer, TokenKind.EQ);
          systemQueryOption = (SystemQueryOption)parseLevels(tokenizer);

        } else if (!forCount && tokenizer.next(TokenKind.ORDERBY)) {
          ParserHelper.requireNext(tokenizer, TokenKind.EQ);
          systemQueryOption = new OrderByParser(this.edm).parse(tokenizer, referencedType, null,
            this.aliases);

        } else if (tokenizer.next(TokenKind.SEARCH)) {
          ParserHelper.requireNext(tokenizer, TokenKind.EQ);
          ParserHelper.bws(tokenizer);
          systemQueryOption = new SearchParser().parse(tokenizer);

        } else if (!forRef && !forCount && tokenizer.next(TokenKind.SELECT)) {
          ParserHelper.requireNext(tokenizer, TokenKind.EQ);
          systemQueryOption = new SelectParser(this.edm).parse(tokenizer, referencedType,
            referencedIsCollection);

        } else if (!forCount && tokenizer.next(TokenKind.SKIP)) {
          ParserHelper.requireNext(tokenizer, TokenKind.EQ);
          ParserHelper.requireNext(tokenizer, TokenKind.IntegerValue);
          final int value = ParserHelper.parseNonNegativeInteger(
            SystemQueryOptionKind.SKIP.toString(), tokenizer.getText(), true);
          final SkipOptionImpl skipOption = new SkipOptionImpl();
          skipOption.setText(tokenizer.getText());
          skipOption.setValue(value);
          systemQueryOption = skipOption;

        } else if (!forCount && tokenizer.next(TokenKind.TOP)) {
          ParserHelper.requireNext(tokenizer, TokenKind.EQ);
          ParserHelper.requireNext(tokenizer, TokenKind.IntegerValue);
          final int value = ParserHelper.parseNonNegativeInteger(
            SystemQueryOptionKind.TOP.toString(), tokenizer.getText(), true);
          final TopOptionImpl topOption = new TopOptionImpl();
          topOption.setText(tokenizer.getText());
          topOption.setValue(value);
          systemQueryOption = topOption;

        } else if (!forRef && !forCount && tokenizer.next(TokenKind.APPLY)) {
          ParserHelper.requireNext(tokenizer, TokenKind.EQ);
          systemQueryOption = new ApplyParser(this.edm).parse(tokenizer,
            // Data aggregation may change the structure of the result, so we
            // create a new dynamic type.
            new DynamicStructuredType(referencedType), null, this.aliases);

        } else {
          throw new UriParserSyntaxException("Allowed query option expected.",
            UriParserSyntaxException.MessageKeys.SYNTAX);
        }
        try {
          item.setSystemQueryOption(systemQueryOption);
        } catch (final ODataRuntimeException e) {
          throw new UriParserSyntaxException(
            "Double system query option '" + systemQueryOption.getName() + "'.", e,
            UriParserSyntaxException.MessageKeys.DOUBLE_SYSTEM_QUERY_OPTION,
            systemQueryOption.getName());
        }
      } while (tokenizer.next(TokenKind.SEMI));
      ParserHelper.requireNext(tokenizer, TokenKind.CLOSE);
    }
  }
}
