package com.revolsys.odata.model;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.ODataEntity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.RecordEntity;
import org.apache.olingo.commons.api.edm.EdmAction;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmFunction;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlNavigationProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriParameter;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceAction;
import org.apache.olingo.server.api.uri.UriResourceFunction;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByItem;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.SelectItem;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.SystemQueryOptionKind;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;

import com.revolsys.collection.list.Lists;
import com.revolsys.io.PathName;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.And;
import com.revolsys.record.query.Condition;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.record.schema.RecordStore;

public class ODataEntityType extends CsdlEntityType {

  private static final List<String> AUDIT_FIELD_NAMES = Arrays.asList("deleted", "createTimestamp",
    "createUserId", "modifyTimestamp", "modifyUserId");

  public static Set<String> getSelectedPropertyNames(final List<SelectItem> selectItems) {
    final Set<String> selected = new HashSet<>();
    for (final SelectItem item : selectItems) {
      final UriResource resource = item.getResourcePath()
        .getUriResourceParts()
        .get(0);
      if (resource instanceof UriResourceProperty) {
        final UriResourceProperty uriResourceProperty = (UriResourceProperty)resource;
        final EdmProperty property = uriResourceProperty.getProperty();
        final String name = property.getName();
        selected.add(name);
      } else if (resource instanceof UriResourceNavigation) {
        final UriResourceNavigation uriResourceNavigation = (UriResourceNavigation)resource;
        final EdmNavigationProperty property = uriResourceNavigation.getProperty();
        final String name = property.getName();
        selected.add(name);
      } else if (resource instanceof UriResourceAction) {
        final UriResourceAction uriResourceAction = (UriResourceAction)resource;
        final EdmAction action = uriResourceAction.getAction();
        final String name = action.getName();
        selected.add(name);
      } else if (resource instanceof UriResourceFunction) {
        final UriResourceFunction uriResourceFunction = (UriResourceFunction)resource;
        final EdmFunction function = uriResourceFunction.getFunction();
        final String name = function.getName();
        selected.add(name);
      }
    }
    return selected;
  }

  public static boolean hasSelect(final SelectOption select) {
    return select != null && select.getSelectItems() != null && !select.getSelectItems()
      .isEmpty();
  }

  public static boolean isAll(final SelectOption select) {
    if (hasSelect(select)) {
      for (final SelectItem item : select.getSelectItems()) {
        if (item.isStar()) {
          return true;
        }
      }
      return false;
    } else {
      return true;
    }
  }

  private final PathName pathName;

  private RecordDefinition recordDefinition;

  private final ODataSchema schema;

  private int maxLimit = 10000;

  private final AbstractODataEntitySet entitySet;

  public ODataEntityType(final AbstractODataEntitySet entitySet, final ODataSchema schema,
    final String typeName, final PathName pathName) {
    setName(typeName);
    this.entitySet = entitySet;
    this.schema = schema;
    this.pathName = pathName;
    final RecordDefinition recordDefinition = getRecordStore().getRecordDefinition(this.pathName);
    setRecordDefinition(recordDefinition);
  }

  void addLimits(final Query query, final UriInfo uriInfo) throws ODataApplicationException {
    final SkipOption skipOption = uriInfo.getSkipOption();
    if (skipOption != null) {
      final int offset = skipOption.getValue();
      if (offset >= 0) {
        query.setOffset(offset);
      } else {
        throw new ODataApplicationException("Invalid value for $skip=" + offset + " must be > 0",
          HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
      }
    }

    final TopOption topOption = uriInfo.getTopOption();
    if (topOption != null) {
      final int limit = topOption.getValue();
      if (limit >= 0) {
        query.setLimit(limit);
      } else {
        throw new ODataApplicationException("Invalid value for $top=" + limit + " must be > 0",
          HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
      }
    }
    final int maxLimit = getMaxLimit();
    if (query.getLimit() > maxLimit) {
      query.setLimit(maxLimit);
    }

  }

  public void addNavigationProperty(final CsdlNavigationProperty navigationProperty) {
    this.navigationProperties.add(navigationProperty);
  }

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

  public int getMaxLimit() {
    return this.maxLimit;
  }

  @Override
  public ODataNavigationProperty getNavigationProperty(final String name) {
    return (ODataNavigationProperty)super.getNavigationProperty(name);
  }

  // public Entity getRelatedEntity(final Entity entity, final EdmEntityType
  // relatedEntityType,
  // final List<UriParameter> keyPredicates) {
  //
  // final EntityCollection relatedEntities = getRelatedEntityCollection(entity,
  // relatedEntityType);
  // return Util.findEntity(relatedEntityType, relatedEntities, keyPredicates);
  // }
  //
  // public EntityCollection getRelatedEntityCollection(final Entity
  // sourceEntity,
  // final EdmEntityType targetEntityType) {
  // final EntityCollection navigationTargetEntityCollection = new
  // EntityCollection();
  //
  // final FullQualifiedName relatedEntityFqn =
  // targetEntityType.getFullQualifiedName();
  // final String sourceEntityFqn = sourceEntity.getType();
  //
  // if
  // (sourceEntityFqn.equals(DemoEdmProvider.ET_PRODUCT_FQN.getFullQualifiedNameAsString())
  // && relatedEntityFqn.equals(DemoEdmProvider.ET_CATEGORY_FQN)) {
  // // relation Products->Category (result all categories)
  // final int productID = (Integer)sourceEntity.getProperty("ID").getValue();
  // if (productID == 1 || productID == 2) {
  // navigationTargetEntityCollection.getEntities().add(categoryList.get(0));
  // } else if (productID == 3 || productID == 4) {
  // navigationTargetEntityCollection.getEntities().add(categoryList.get(1));
  // } else if (productID == 5 || productID == 6) {
  // navigationTargetEntityCollection.getEntities().add(categoryList.get(2));
  // }
  // } else if (sourceEntityFqn
  // .equals(DemoEdmProvider.ET_CATEGORY_FQN.getFullQualifiedNameAsString())
  // && relatedEntityFqn.equals(DemoEdmProvider.ET_PRODUCT_FQN)) {
  // // relation Category->Products (result all products)
  // final int categoryID = (Integer)sourceEntity.getProperty("ID").getValue();
  // if (categoryID == 1) {
  // // the first 2 products are notebooks
  // navigationTargetEntityCollection.getEntities().addAll(productList.subList(0,
  // 2));
  // } else if (categoryID == 2) {
  // // the next 2 products are organizers
  // navigationTargetEntityCollection.getEntities().addAll(productList.subList(2,
  // 4));
  // } else if (categoryID == 3) {
  // // the first 2 products are monitors
  // navigationTargetEntityCollection.getEntities().addAll(productList.subList(4,
  // 6));
  // }
  // } else if
  // (sourceEntityFqn.equals(DemoEdmProvider.ET_PRODUCT_FQN.getFullQualifiedNameAsString())
  // && relatedEntityFqn.equals(DemoEdmProvider.ET_SUPPLIER_FQN)) {
  // final int productID = (Integer)sourceEntity.getProperty("ID").getValue();
  // if (productID == 1) {
  // navigationTargetEntityCollection.getEntities().add(supplierList.get(0));
  // navigationTargetEntityCollection.getEntities().add(supplierList.get(1));
  // } else if (productID == 2) {
  // navigationTargetEntityCollection.getEntities().add(supplierList.get(2));
  // } else if (productID == 3) {
  // navigationTargetEntityCollection.getEntities().add(supplierList.get(3));
  // } else if (productID == 4) {
  // navigationTargetEntityCollection.getEntities().add(supplierList.get(4));
  // } else if (productID == 5) {
  // navigationTargetEntityCollection.getEntities().add(supplierList.get(5));
  // } else if (productID == 6) {
  // navigationTargetEntityCollection.getEntities().add(supplierList.get(6));
  // }
  // }
  //
  // if (navigationTargetEntityCollection.getEntities().isEmpty()) {
  // return null;
  // }
  //
  // return navigationTargetEntityCollection;
  // }

  public PathName getPathName() {
    return this.pathName;
  }

  public RecordDefinition getRecordDefinition() {
    return this.recordDefinition;
  }

  public RecordStore getRecordStore() {
    return this.schema.getRecordStore();
  }

  public ODataEntity getRelatedEntity(final ODataRequest request, final ODataEntity entity,
    final ODataNavigationProperty navigationProperty) throws ODataApplicationException {
    final Condition where = navigationProperty.whereCondition(entity);
    return readEntity(request, null, where);
  }

  public ODataEntity newEntity(final Record record) {
    return new RecordEntity(this, record);
  }

  protected Query newQuery(final ODataRequest request) {
    return this.entitySet.newQuery(request);
  }

  public Query newQuery(final ODataRequest request, final UriInfo uriInfo) {
    final RecordDefinition recordDefinition = this.recordDefinition;

    final Query query = newQuery(request);
    if (recordDefinition != null) {
      final SelectOption selectOption = uriInfo.getSelectOption();
      if (selectOption != null && !isAll(selectOption)) {
        final List<SelectItem> selectItems = selectOption.getSelectItems();
        final Set<String> propertyNames = getSelectedPropertyNames(selectItems);
        query.select(propertyNames);
      }

      final FilterOption filterOption = uriInfo.getFilterOption();
      if (filterOption != null) {
        if (filterOption.getKind() == SystemQueryOptionKind.FILTER) {
          final Expression expression = filterOption.getExpression();
          final Condition filterCondition = (Condition)ODataExpressionHandler
            .toQueryValue(recordDefinition, expression);
          query.and(filterCondition);
        }
      }
      final OrderByOption orderByOption = uriInfo.getOrderByOption();
      if (orderByOption != null) {
        final List<OrderByItem> orderItemList = orderByOption.getOrders();
        for (final OrderByItem orderByItem : orderItemList) {
          final Expression expression = orderByItem.getExpression();
          if (expression instanceof Member) {
            final UriInfoResource resourcePath = ((Member)expression).getResourcePath();
            final UriResource uriResource = resourcePath.getUriResourceParts()
              .get(0);
            if (uriResource instanceof UriResourcePrimitiveProperty) {
              final EdmProperty edmProperty = ((UriResourcePrimitiveProperty)uriResource)
                .getProperty();
              final String sortPropertyName = edmProperty.getName();
              // TODO map property names
              query.addOrderBy(sortPropertyName, !orderByItem.isDescending());
            }
          }
        }
      }
      for (final String idFieldName : recordDefinition.getIdFieldNames()) {
        if (!query.hasOrderBy(idFieldName)) {
          query.addOrderBy(idFieldName, true);
        }
      }
    }
    return query;
  }

  protected ODataEntityType newRecordDefinition(final List<String> idFieldNames,
    final List<String> fieldNames) {
    idFieldNames.retainAll(fieldNames);
    sortFieldNames(idFieldNames, fieldNames);
    final RecordDefinition recordDefinition = new RecordDefinitionBuilder(this.recordDefinition,
      fieldNames)//
      .setIdFieldNames(idFieldNames)//
      .getRecordDefinition();
    setRecordDefinition(recordDefinition);
    return this;
  }

  public ODataEntity readEntity(final ODataRequest request, final EdmEntitySet edmEntitySet,
    final List<UriParameter> keyParams, final List<String> propertyNames)
    throws ODataApplicationException {
    final RecordDefinition recordDefinition = this.recordDefinition;
    final And and = new And();
    for (final UriParameter key : keyParams) {
      final String keyName = key.getName();
      final String keyText = key.getText()
        .replaceAll("(^'|'$)", "");
      and.addCondition(recordDefinition.equal(keyName, keyText));
    }
    return readEntity(request, propertyNames, and);
  }

  public ODataEntity readEntity(final ODataRequest request, final List<String> propertyNames,
    final Condition where) throws ODataApplicationException {
    final Query query = newQuery(request).and(where);
    if (propertyNames != null) {
      query.select(propertyNames);
    }

    final RecordStore recordStore = getRecordStore();
    final var entity = request.getConnection()
      .transactionCall(() -> {
        try (
          RecordReader reader = recordStore.getRecords(query)) {

          for (final Record record : reader) {
            return newEntity(record);
          }
        }
        return null;
      });
    if (entity != null) {
      return entity;
    } else {
      throw new ODataApplicationException("Entity for requested key doesn't exist",
        HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
    }
  }

  public EntityCollection readEntityCollection(final ODataRequest request, final UriInfo uriInfo,
    final EdmEntitySet edmEntitySet) throws ODataApplicationException {
    return request.getConnection()
      .transactionCall(() -> {
        final Query query = newQuery(request, uriInfo);

        final EntityCollection entityCollection = new EntityCollection();
        final RecordStore recordStore = getRecordStore();
        final CountOption countOption = uriInfo.getCountOption();
        if (countOption != null) {
          if (countOption.getValue()) {
            entityCollection.setCount(recordStore.getRecordCount(query));
          }
        }
        try (
          RecordReader reader = recordStore.getRecords(query)) {
          addLimits(query, uriInfo);

          final var entityList = entityCollection.getEntities();
          for (final Record record : reader) {
            final var entity = newEntity(record);
            entityList.add(entity);
          }
        }
        return entityCollection;
      });
  }

  public ODataEntityIterator readEntityIterator(final ODataRequest request, final UriInfo uriInfo,
    final EdmEntitySet edmEntitySet) throws ODataApplicationException {
    return new ODataEntityIterator(request, uriInfo, edmEntitySet, this);
  }

  public Property readPrimitive(final ODataRequest request, final EdmEntitySet edmEntitySet,
    final List<UriParameter> keyParams, final String propertyName)
    throws ODataApplicationException {
    final ODataEntity entity = readEntity(request, edmEntitySet, keyParams,
      Arrays.asList(propertyName));
    return entity.getProperty(propertyName);
  }

  public ODataEntityType removeFieldNames(final String... removeFieldNames) {
    final List<String> names = Arrays.asList(removeFieldNames);
    final RecordDefinition recordDefinition = this.recordDefinition;
    final List<String> fieldNames = recordDefinition.getFieldNames()
      .stream()
      .filter(name -> !names.contains(name))
      .collect(Collectors.toList());
    final List<String> idFieldNames = new ArrayList<>(recordDefinition.getIdFieldNames());
    return newRecordDefinition(idFieldNames, fieldNames);
  }

  public ODataEntityType setIdFieldNames(final String... idFieldNames) {
    final RecordDefinition recordDefinition = this.recordDefinition;
    final List<String> fieldNames = new ArrayList<>(recordDefinition.getFieldNames());
    final List<String> idNames = Lists.newArray(idFieldNames);
    return newRecordDefinition(idNames, fieldNames);
  }

  public ODataEntityType setMaxLimit(final int maxLimit) {
    this.maxLimit = maxLimit;
    return this;
  }

  public void setRecordDefinition(final RecordDefinition recordDefinition) {
    this.recordDefinition = recordDefinition;

    if (this.recordDefinition != null) {
      getFields().clear();

      final List<CsdlPropertyRef> keys = new ArrayList<>();
      for (final FieldDefinition field : this.recordDefinition.getFields()) {
        final String name = field.getName();

        this.fields.add(field);

        if (recordDefinition.isIdField(name)) {
          final CsdlPropertyRef propertyRef = new CsdlPropertyRef() //
            .setName(name)//
          ;
          keys.add(propertyRef);
        }
      }
      if (keys.isEmpty()) {
        setKey(null);
      } else {
        setKey(keys);
      }
    }

  }

  protected void sortFieldNames(final List<String> idFieldNames, final List<String> fieldNames) {
    fieldNames.sort((name1, name2) -> {
      final int auditIndex1 = AUDIT_FIELD_NAMES.indexOf(name1);
      final int auditIndex2 = AUDIT_FIELD_NAMES.indexOf(name2);
      if (auditIndex1 == auditIndex2) {
        final int nameCompare = name1.compareToIgnoreCase(name2);
        if (idFieldNames.contains(name1)) {
          if (idFieldNames.contains(name2)) {
            return nameCompare;
          } else {
            return -1;
          }
        } else {
          if (idFieldNames.contains(name2)) {
            return 1;
          } else {
            return nameCompare;
          }
        }
      } else if (auditIndex1 < auditIndex2) {
        return -1;
      } else {
        return 1;
      }
    });
  }

  @Override
  public String toString() {
    return getName();
  }
}
