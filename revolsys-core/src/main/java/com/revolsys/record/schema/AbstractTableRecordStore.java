package com.revolsys.record.schema;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.LoggerFactory;

import com.revolsys.collection.json.JsonList;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.json.JsonType;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.value.ValueHolder;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.type.CollectionDataType;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.exception.Exceptions;
import com.revolsys.function.Function3;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.io.PathName;
import com.revolsys.jdbc.JdbcConnection;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.ArrayChangeTrackRecord;
import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.ODataParser;
import com.revolsys.record.Record;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.query.Cast;
import com.revolsys.record.query.Column;
import com.revolsys.record.query.ColumnReference;
import com.revolsys.record.query.Condition;
import com.revolsys.record.query.Count;
import com.revolsys.record.query.DeleteStatement;
import com.revolsys.record.query.InsertStatement;
import com.revolsys.record.query.JoinType;
import com.revolsys.record.query.Or;
import com.revolsys.record.query.Parenthesis;
import com.revolsys.record.query.Q;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.TableReference;
import com.revolsys.record.query.UpdateStatement;
import com.revolsys.record.query.Value;
import com.revolsys.record.query.functions.F;
import com.revolsys.record.query.functions.ArrayElements;
import com.revolsys.util.Property;

public class AbstractTableRecordStore implements RecordDefinitionProxy {
  public record VirtualField(AbstractTableRecordStore recordStore, String name,
    Consumer<RecordDefinitionBuilder> addToSchema,
    Function3<Query, VirtualField, String[], QueryValue> newQueryValue) {

    public void addToSchema(final RecordDefinitionBuilder builder) {
      this.addToSchema.accept(builder);
    }

    public QueryValue newQueryValue(final Query query, final String... path) {
      return this.newQueryValue.apply(query, this, path);
    }
  }

  public static JsonObject schemaToJson(final RecordDefinition recordDefinition) {
    if (recordDefinition == null) {
      return JsonObject.EMPTY;
    }
    final JsonList jsonFields = JsonList.array();
    final String idFieldName = recordDefinition.getIdFieldName();
    final JsonObject jsonSchema = JsonObject.hash()
      .addValue("typeName", recordDefinition.getPathName())
      .addValue("title", recordDefinition.getTitle())
      .addValue("idFieldName", idFieldName)
      .addValue("geometryFieldName", recordDefinition.getGeometryFieldName())
      .addValue("fields", jsonFields);
    final GeometryFactory geometryFactory = recordDefinition.getGeometryFactory();
    if (geometryFactory != null) {
      final int coordinateSystemId = geometryFactory.getHorizontalCoordinateSystemId();
      if (coordinateSystemId > 0) {
        jsonSchema.addValue("srid", coordinateSystemId);
      }
    }
    for (final FieldDefinition field : recordDefinition.getFields()) {
      final String fieldName = field.getName();
      final DataType dataType = field.getDataType();
      String dataTypeString = dataType.toString();
      if (dataTypeString.startsWith("List")) {
        dataTypeString = dataTypeString.substring(4) + "[]";
      }
      final JsonObject jsonField = JsonObject.hash()
        .addValue("name", fieldName)
        .addNotEmpty("title", field.getTitle()
          .replace(" Ind", "")
          .replace(" Code", ""))
        .addNotEmpty("description", field.getDescription())
        .addValue("dataType", dataTypeString)
        .addValue("required", field.isRequired());
      if (recordDefinition.isIdField(fieldName)) {
        jsonField.addValue("readonly", true);
      }
      final int length = field.getLength();
      if (length > 0) {
        jsonField.addValue("length", length);
      }
      final int scale = field.getScale();
      if (scale > 0) {
        jsonField.addValue("scale", scale);
      }
      jsonField//
        .addNotEmpty("default", field.getDefaultValue())
        .addNotEmpty("allowedValues", field.getAllowedValues())
        .addNotEmpty("min", field.getMinValue())
        .addNotEmpty("max", field.getMaxValue());
      final CodeTable codeTable = field.getCodeTable();
      if (codeTable != null) {
        jsonField.addNotEmpty("codeTable", codeTable.getName());
      }
      jsonFields.add(jsonField);
    }
    return jsonSchema;
  }

  private final ValueHolder<JsonObject> schema = ValueHolder.lazy(this::schemaToJson);

  private final Map<String, VirtualField> virtualFieldByName = new LinkedHashMap<>();

  private RecordStore recordStore;

  private final PathName tablePath;

  private final String typeName;

  private RecordDefinition recordDefinition;

  protected Map<QueryValue, Boolean> defaultSortOrder = new LinkedHashMap<>();

  private final Set<String> searchFieldNames = new LinkedHashSet<>();

  private String tableAlias;

  public AbstractTableRecordStore(final PathName typePath) {
    this.tablePath = typePath;
    this.typeName = typePath.getName();
  }

  public AbstractTableRecordStore(final PathName typePath, final JdbcRecordStore recordStore) {
    this(typePath);
    setRecordStore(recordStore);
  }

  protected void addDefaultSortOrder(final String fieldName) {
    addDefaultSortOrder(fieldName, true);
  }

  protected void addDefaultSortOrder(final String fieldName, final boolean ascending) {
    final RecordDefinition recordDefinition = getRecordDefinition();
    if (recordDefinition != null) {
      final FieldDefinition field = recordDefinition.getFieldDefinition(fieldName);
      if (field != null) {
        this.defaultSortOrder.put(field, ascending);
      }
    }
  }

  public void addQueryOrderBy(final Query query, final String orderBy) {
    if (Property.hasValue(orderBy)) {
      for (String orderClause : orderBy.split(",")) {
        orderClause = orderClause.strip();
        String fieldName;
        boolean ascending = true;
        final int spaceIndex = orderClause.indexOf(' ');
        if (spaceIndex == -1) {
          fieldName = orderClause;
        } else {
          fieldName = orderClause.substring(0, spaceIndex);
          if ("desc".equalsIgnoreCase(orderClause.substring(spaceIndex + 1))) {
            ascending = false;
          }
        }
        Object orderField;
        if (hasField(fieldName)) {
          orderField = getColumn(fieldName);
        } else {
          try {
            orderField = Integer.parseInt(fieldName);
          } catch (final NumberFormatException e) {
            orderField = fieldName;
          }
        }
        query.addOrderBy(orderField, ascending);
      }
    }
  }

  protected void addSearchConditions(final Query query, final Or or, String search) {
    final String searchText = search.strip()
      .toLowerCase();
    search = '%' + searchText + '%';
    for (final String fieldName : this.searchFieldNames) {
      final var column = getTable().getColumn(fieldName);
      if (column != null && column.getDataType() instanceof CollectionDataType) {
        or.addCondition(newQuery().select(Value.newValue(1))
          .setFrom(ArrayElements.unnest(column)
            .toFromAlias(fieldName + "A"))
          .and(new Column(fieldName + "A"), Q.ILIKE, Value.toValue(search))
          .asExists());

      } else {
        final QueryValue left = getSearchColumn(fieldName, searchText);
        if (left != null) {
          final Condition condition = query.newCondition(left, Q.ILIKE, search);
          or.addCondition(condition);
        }
      }
    }
  }

  protected void addSelect(final TableRecordStoreConnection connection, final Query query,
    final String selectItem) {
    final QueryValue selectClause = fieldPathToSelect(query, selectItem);
    query.select(selectClause);
  }

  public void addStringVirtualField(final String name,
    final Function3<Query, VirtualField, String[], QueryValue> newQueryValue) {
    final var field = new VirtualField(this, name, rd -> rd.addField(name), newQueryValue);
    addVirtualField(field);
  }

  public void addVirtualField(final String name, final DataType dataType,
    final Function3<Query, VirtualField, String[], QueryValue> newQueryValue) {
    final var field = new VirtualField(this, name, rd -> rd.addField(name, dataType),
      newQueryValue);
    addVirtualField(field);
  }

  public void addVirtualField(final VirtualField field) {
    this.virtualFieldByName.put(field.name(), field);
  }

  protected Condition alterCondition(final HttpServletRequest request,
    final TableRecordStoreConnection connection, final Query query, final Condition condition) {
    return condition;
  }

  public void applyDefaultSortOrder(final Query query) {
    for (final Entry<QueryValue, Boolean> entry : this.defaultSortOrder.entrySet()) {
      final QueryValue field = entry.getKey();
      if (!query.hasOrderBy(field)) {
        final Boolean ascending = entry.getValue();
        query.addOrderBy(field, ascending);
      }
    }
  }

  public Query applySearchCondition(final Query query, final String search) {
    if (search != null && search.strip()
      .length() > 0) {
      final Or or = new Or();
      addSearchConditions(query, or, search);
      if (!or.isEmpty()) {
        query.and(or);
      }
    }
    return query;
  }

  public boolean canEditField(final String fieldName) {
    if (!this.recordDefinition.hasField(fieldName) || this.recordDefinition.isIdField(fieldName)) {
      return false;
    }
    return true;
  }

  public boolean deleteRecord(final TableRecordStoreConnection connection, final Record record) {
    return connection.transactionCall(() -> this.recordStore.deleteRecord(record));
  }

  public DeleteStatement deleteStatement(final TableRecordStoreConnection connection) {
    final var table = getTable();
    return new TableRecordStoreDeleteStatement(connection).from(table);
  }

  protected void executeUpdate(final TableRecordStoreConnection connection, final String sql,
    final Object... parameters) {
    if (this.recordStore instanceof JdbcRecordStore) {
      connection.transactionNewRun(() -> this.recordStore.<JdbcRecordStore> getRecordStore()
        .executeUpdate(sql, parameters));
    }
    throw new UnsupportedOperationException("Must be a JDBC connection");

  }

  public boolean exists(final TableRecordStoreConnection connection,
    final TableRecordStoreQuery query) {
    return connection.transactionCall(() -> getRecordStore().exists(query));
  }

  public QueryValue fieldPathToQueryValue(final Query query, String path) {
    String wrapFunction = null;
    final int tildeIndex = path.lastIndexOf('~');
    if (tildeIndex != -1) {
      wrapFunction = path.substring(tildeIndex + 1);
      path = path.substring(0, tildeIndex);
    }
    var queryValue = fieldPathToQueryValueDo(query, path);
    if (queryValue == null) {
      queryValue = Q.sql("NULL");
    }
    if (wrapFunction != null) {
      queryValue = pathToQueryValueWrap(queryValue, wrapFunction);
    }
    return queryValue;
  }

  protected QueryValue fieldPathToQueryValueDo(final Query query, final String path) {
    final var parts = path.split("\\.");

    final var virtualField = this.virtualFieldByName.get(parts[0]);
    if (virtualField != null) {
      return virtualField.newQueryValue(query, parts);
    }
    return getTable().columnByPath(path);
  }

  protected QueryValue fieldPathToQueryValueJoin(final Query query,
    final AbstractTableRecordStore joinRs, final String joinAlias, final String joinFieldName,
    final String lookupFieldName, final String[] path) {
    var join = query.getJoin(joinRs, joinAlias);
    if (join == null) {
      join = query.join(JoinType.LEFT_OUTER_JOIN)
        .table(joinRs)//
        .setAlias(joinAlias)
        .on("id", query, joinFieldName);
    }
    final var column = join.getColumn(lookupFieldName);
    QueryValue selectField = column;
    if (path.length > 1) {
      for (int i = 1; i < path.length; i++) {
        final var part = path[i];
        selectField = Q.jsonRawValue(selectField, part);
      }
    }
    return selectField;
  }

  protected QueryValue fieldPathToQueryValueSubQuery(final Query query,
    final AbstractTableRecordStore otherRs, final String joinFieldName,
    final String lookupFieldName, final String[] path) {
    final var otherField = otherRs.getField(lookupFieldName);
    QueryValue selectField = otherField;
    if (path.length > 1) {
      for (int i = 1; i < path.length; i++) {
        final var part = path[i];
        selectField = Q.jsonRawValue(selectField, part);
      }
    }

    final var joinColumn = query.getColumn(joinFieldName);
    final var otherQuery = otherRs.newQuery()
      .select(selectField)
      .and("id", joinColumn);
    return new Parenthesis(otherQuery);
  }

  public QueryValue fieldPathToSelect(final Query query, final String path) {
    var queryValue = fieldPathToQueryValue(query, path);
    // Add alias if needed
    if (queryValue instanceof final ColumnReference column) {
      if (!column.getName()
        .equals(path)) {
        queryValue = queryValue.toAlias(path);
      }
    } else {
      queryValue = queryValue.toAlias(path);
    }
    return queryValue;

  }

  public Map<QueryValue, Boolean> getDefaultSortOrder() {
    return this.defaultSortOrder;
  }

  protected JdbcConnection getJdbcConnection() {
    final RecordStore recordStore = this.recordStore.getRecordStore();
    if (recordStore instanceof JdbcRecordStore) {
      return ((JdbcRecordStore)recordStore).getJdbcConnection();
    }
    throw new UnsupportedOperationException("Must be a JDBC connection");
  }

  @SuppressWarnings("unchecked")
  protected <R extends Record> R getRecord(final TableRecordStoreConnection connection,
    final Query query) {
    return connection.transactionCall(() -> (R)this.recordStore.getRecord(query));
  }

  public Record getRecord(final TableRecordStoreConnection connection, final String fieldName,
    final Object value) {
    return newQuery(connection).and(fieldName, value)
      .getRecord();
  }

  public Record getRecordById(final TableRecordStoreConnection connection, final Object id) {
    return getRecord(connection, "id", id);
  }

  public Record getRecordById(final TableRecordStoreConnection connection, final UUID id) {
    return getRecord(connection, "id", id);
  }

  protected long getRecordCount(final TableRecordStoreConnection connection, final Query query) {
    return connection.transactionCall(() -> this.recordStore.getRecordCount(query));
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    return this.recordDefinition;
  }

  protected RecordReader getRecordReader(final TableRecordStoreConnection connection,
    final Query query) {
    return this.recordStore.getRecords(query);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R extends RecordStore> R getRecordStore() {
    return (R)this.recordStore;
  }

  protected JsonObject getSchema() {
    return this.schema.get();
  }

  protected QueryValue getSearchColumn(final String fieldName, final String searchText) {
    final ColumnReference column = getTable().getColumn(fieldName);
    final DataType dataType = column.getDataType();
    if (dataType != null) {
      if (dataType instanceof CollectionDataType) {
        return new Cast(new Column(column.getName()), "text");
      } else if (dataType != DataTypes.STRING) {
        if (!dataType.isNumeric() || dataType.isValid(searchText)) {
          return new Cast(column, "text");
        } else {
          return null;
        }
      }
    }
    return column;
  }

  public TableReference getTable() {
    return getRecordDefinition();
  }

  @Override
  public String getTableAlias() {
    return this.tableAlias;
  }

  public PathName getTablePath() {
    return this.tablePath;
  }

  protected String getTypeName() {
    return this.typeName;
  }

  public boolean hasRecord(final TableRecordStoreConnection connection, final Query query) {
    return connection.transactionCall(() -> query.getRecordCount() > 0);
  }

  public boolean hasRecord(final TableRecordStoreConnection connection, final String fieldName,
    final Object value) {
    final Query query = newQuery(connection).and(fieldName, value);
    return hasRecord(connection, query);
  }

  protected Record insertRecord(final TableRecordStoreConnection connection, final Query query,
    final Supplier<Record> newRecordSupplier) {
    query.setRecordFactory(ArrayChangeTrackRecord.FACTORY);
    final ChangeTrackRecord changeTrackRecord = query.getRecord();
    if (changeTrackRecord == null) {
      final Record newRecord = newRecordSupplier.get();
      if (newRecord == null) {
        return null;
      } else {
        return insertRecord(connection, newRecord);
      }
    } else {
      return changeTrackRecord.newRecord();
    }
  }

  public Record insertRecord(final TableRecordStoreConnection connection, final Record record) {
    return connection.transactionCall(() -> {
      insertRecordBefore(connection, record);
      validateRecord(record);
      this.recordStore.insertRecord(record);
      insertRecordAfter(connection, record);
      return record;
    });
  }

  protected void insertRecordAfter(final TableRecordStoreConnection connection,
    final Record record) {
  }

  protected void insertRecordBefore(final TableRecordStoreConnection connection,
    final Record record) {
  }

  public InsertStatement insertStatement(final TableRecordStoreConnection connection) {
    return new TableRecordStoreInsertStatement(this, connection).into(getTable());
  }

  protected void insertStatementRecordAfter(final TableRecordStoreConnection connection,
    final Record record) {
  }

  protected boolean isFieldReadonly(final String fieldName) {
    return this.recordDefinition.isIdField(fieldName);
  }

  public void lockTable(final TableRecordStoreConnection connection) {
    if (this.recordStore instanceof JdbcRecordStore) {
      connection.transactionRun(() -> this.recordStore.<JdbcRecordStore> getRecordStore()
        .lockTable(this.tablePath));
    }
  }

  public <R extends Record> InsertUpdateBuilder<R> newInsert(
    final TableRecordStoreConnection connection) {
    return this.<R> newInsertUpdate(connection)
      .setUpdate(false);
  }

  public <R extends Record> InsertUpdateBuilder<R> newInsertUpdate(
    final TableRecordStoreConnection connection) {
    return new TableRecordStoreInsertUpdateBuilder<>(this, connection);
  }

  public Condition newODataFilter(final Query query, String filter) {
    if (Property.hasValue(filter)) {
      filter = filter.replace("%2B", "+");
      return (Condition)ODataParser.parseFilter(path -> fieldPathToQueryValue(query, path), filter);
    } else {
      return null;
    }
  }

  public Query newQuery(final TableRecordStoreConnection connection) {
    final var query = new TableRecordStoreQuery(this, connection);
    query.setBaseFileName(this.typeName);
    return query;
  }

  public Query newQuery(final TableRecordStoreConnection connection,
    final Consumer<Query> configurer) {
    return newQuery(connection).accept(configurer);
  }

  public Query newQuery(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final int maxSize) {
    final String select = request.getParameter("$select");
    final String filter = request.getParameter("$filter");
    final String search = request.getParameter("$search");
    final String orderBy = request.getParameter("$orderby");
    final String aggregate = request.getParameter("$aggregate");

    final boolean count = "true".equals(request.getParameter("$count"));
    int skip = 0;
    try {
      final String value = request.getParameter("$skip");
      if (value != null) {
        skip = Integer.parseInt(value);
      }
    } catch (final Exception e) {
    }
    int top = maxSize;
    try {
      final String value = request.getParameter("$top");
      if (value != null) {
        top = Math.min(Integer.parseInt(value), maxSize);
        if (top <= 0) {
          throw new IllegalArgumentException("$top must be > 1: " + top);
        }
      }
    } catch (final Exception e) {
    }

    final Query query = newQuery(connection).setOffset(skip)
      .setLimit(top)
      .setReturnCount(count);

    if (Property.hasValue(select)) {
      for (String selectItem : select.split(",")) {
        selectItem = selectItem.strip();
        addSelect(connection, query, selectItem);
      }
    }

    boolean hasAggregate = false;
    if (Property.hasValue(aggregate)) {

      final ListEx<QueryValue> aggregates = Lists.split(aggregate, ",")
        .map(element -> parseAggregate(query, element))
        .toList();
      if (aggregates.isEmpty()) {
        return null;
      }
      final int selectCount = query.getSelect()
        .size();
      for (int i = 1; i <= selectCount; i++) {
        // Group by all the non-aggregate functions
        query.addGroupBy(i);
      }
      aggregates.forEach(query::select);
      hasAggregate = true;
    }

    Condition filterCondition = newODataFilter(query, filter);
    if (filterCondition != null) {
      filterCondition = alterCondition(request, connection, query, filterCondition);
      query.and(filterCondition.clone(null, query.getTable()));
    }
    applySearchCondition(query, search);
    addQueryOrderBy(query, orderBy);
    if (!hasAggregate) {
      applyDefaultSortOrder(query);
    }
    return query;
  }

  protected void newQueryFilterConditionSearch(final Query query, final String search) {
    applySearchCondition(query, search);
  }

  public Record newRecord(final TableRecordStoreConnection connection) {
    return getRecordDefinition().newRecord();
  }

  public Record newRecord(final TableRecordStoreConnection connection, final MapEx values) {
    if (values == null) {
      return null;
    } else {
      final Record record = newRecord(connection);
      for (final String fieldName : values.keySet()) {
        final Object value = values.getValue(fieldName);
        if (Property.hasValue(value)) {
          record.setValue(fieldName, value);
        }
      }
      return record;
    }
  }

  protected RecordDefinition newSchema() {
    RecordDefinition schema = getRecordDefinition();
    if (!this.virtualFieldByName.isEmpty()) {
      final var builder = new RecordDefinitionBuilder(schema);
      this.virtualFieldByName.values()
        .forEach(field -> field.addToSchema(builder));
      schema = builder.getRecordDefinition();
    }
    return schema;
  }

  public <R extends Record> InsertUpdateBuilder<R> newUpdate(
    final TableRecordStoreConnection connection) {
    return this.<R> newInsertUpdate(connection)
      .setInsert(false);
  }

  public UUID newUUID() {
    return UUID.randomUUID();
  }

  protected QueryValue parseAggregate(final Query query, final String element) {
    final var parts = element.split(":");
    final var functionName = parts[0];
    final var fieldName = parts[1];
    final var table = getTable();
    QueryValue field = fieldPathToQueryValue(query, fieldName);
    if (field == null) {
      return null;
    }
    String alias = functionName;
    if (parts.length > 2) {
      alias = parts[2];
    }

    return switch (functionName) {
      case "count": {
        yield Count.STAR.toAlias(alias);
      }
      case "countDistinct": {
        yield Count.distinct(table, fieldName)
          .toAlias(alias);
      }

      case "min":
      case "max":
      case "sum":
      case "avg": {
        Class<?> columnClass = Object.class;
        final var baseColumn = field.getColumn();
        if (baseColumn instanceof final FieldDefinition fieldDefinition) {
          columnClass = fieldDefinition.getTypeClass();
        }
        if (JsonType.class.isAssignableFrom(columnClass)) {
          query.and(Q.equal(F.function("jsonb_typeof", field), "number"));
          field = field.toCast("decimal");
        } else if (!Number.class.isAssignableFrom(columnClass)) {
          query
            .and(Q.equal(F.function("pg_input_is_valid", field, Value.newValue("decimal")), true));
          field = field.toCast("decimal");
        }
        yield F.function(functionName, field)
          .toAlias(alias);
      }
      default:
      yield null;
    };
  }

  protected QueryValue pathToQueryValueWrap(final QueryValue value, final String function) {
    throw new IllegalArgumentException("Function " + function + "  not supported");
  }

  public JsonObject schemaToJson() {
    final RecordDefinition schema = newSchema();
    return schemaToJson(schema);
  }

  protected void setDefaultSortOrder(final Collection<String> fieldNames) {
    this.defaultSortOrder.clear();
    for (final String fieldName : fieldNames) {
      addDefaultSortOrder(fieldName);
    }

  }

  public void setDefaultSortOrder(final String... fieldNames) {
    this.defaultSortOrder.clear();
    for (final String fieldName : fieldNames) {
      addDefaultSortOrder(fieldName);
    }
  }

  protected void setDefaultSortOrder(final String fieldName, final boolean ascending) {
    this.defaultSortOrder.clear();
    addDefaultSortOrder(fieldName, ascending);
  }

  protected void setFieldsGenerated(final boolean generated, final String... fieldNames) {
    if (getRecordDefinition() != null) {
      for (final String fieldName : fieldNames) {
        final JdbcFieldDefinition field = (JdbcFieldDefinition)getRecordDefinition()
          .getField(fieldName);
        if (field != null) {
          field.setGenerated(generated);
        }
      }
    }
  }

  protected void setRecordDefinition(final RecordDefinition recordDefinition) {
    this.recordDefinition = recordDefinition;
    if (recordDefinition == null) {
      LoggerFactory.getLogger(getClass())
        .atError()
        .setMessage("Table doesn't exist")
        .addKeyValue("name", getTypeName())
        .log();
    } else {
      setRecordDefinitionPost(recordDefinition);
      this.tableAlias = recordDefinition.getTableAlias();
    }
  }

  protected void setRecordDefinitionPost(final RecordDefinition recordDefinition) {
  }

  protected void setRecordStore(final RecordStore recordStore) {
    this.recordStore = recordStore;
    final RecordDefinition recordDefinition = this.recordStore.getRecordDefinition(this.tablePath);
    setRecordDefinition(recordDefinition);
  }

  public AbstractTableRecordStore setSearchFieldNames(final Collection<String> searchFieldNames) {
    this.searchFieldNames.addAll(searchFieldNames);
    return this;
  }

  public AbstractTableRecordStore setSearchFieldNames(final String... searchFieldNames) {
    for (final String searchFieldName : searchFieldNames) {
      this.searchFieldNames.add(searchFieldName);
    }
    return this;
  }

  public Record updateRecord(final TableRecordStoreConnection connection, final Identifier id,
    final Consumer<Record> updateAction) {
    final Query query = newQuery(connection)
      .and(Q.equalId(getRecordDefinition().getIdFieldNames(), id));
    return query.updateRecord(updateAction);
  }

  public Record updateRecord(final TableRecordStoreConnection connection, final Identifier id,
    final JsonObject values) {
    return updateRecord(connection, id, record -> record.setValues(values));
  }

  public Record updateRecord(final TableRecordStoreConnection connection, final Object id,
    final Consumer<Record> updateAction) {
    final Query query = newQuery(connection);
    query.and(getRecordDefinition().getIdFieldName(), id);
    return query.updateRecord(updateAction);
  }

  protected Record updateRecord(final TableRecordStoreConnection connection, final Query query,
    final Consumer<Record> updateAction) {
    return connection.transactionCall(() -> {
      query.setRecordFactory(ArrayChangeTrackRecord.FACTORY);
      final ChangeTrackRecord record = query.getRecord();
      if (record == null) {
        return null;
      } else {
        updateAction.accept(record);
        updateRecordDo(connection, record);
        return record.newRecord();
      }
    });
  }

  protected void updateRecordAfter(final TableRecordStoreConnection connection,
    final ChangeTrackRecord record) {
  }

  /**
   * Make changes to the record checked to see if it was modified but before saving to the database
   *
   * @param connection
   * @param record
   */
  protected void updateRecordBefore(final TableRecordStoreConnection connection,
    final ChangeTrackRecord record) {
  }

  protected final Record updateRecordDo(final TableRecordStoreConnection connection,
    final ChangeTrackRecord record) {
    try {
      updateRecordPre(connection, record);
      if (record.isModified()) {
        updateRecordBefore(connection, record);
        this.recordStore.updateRecord(record);
        updateRecordAfter(connection, record);
      }
      return record.newRecord();
    } catch (final Exception e) {
      throw Exceptions.toWrapped(e)
        .property("record", record);
    }
  }

  /**
   * Make changes to the record before it was checked to see if it was modified
   *
   * @param connection
   * @param record
   */
  protected void updateRecordPre(final TableRecordStoreConnection connection,
    final ChangeTrackRecord record) {
  }

  public int updateRecords(final TableRecordStoreConnection connection, final Query query,
    final Consumer<? super ChangeTrackRecord> updateAction) {
    return connection.transactionCall(() -> {
      int i = 0;
      final RecordDefinition recordDefinition = getRecordDefinition();
      final RecordStore recordStore = this.recordStore;
      query.setRecordFactory(ArrayChangeTrackRecord.FACTORY);
      try (
        RecordReader reader = getRecordReader(connection, query);
        RecordWriter writer = recordStore.newRecordWriter(recordDefinition)) {
        for (final Record queryRecord : reader) {
          final ChangeTrackRecord record = (ChangeTrackRecord)queryRecord;
          updateAction.accept(record);
          updateRecordPre(connection, record);
          if (record.isModified()) {
            updateRecordBefore(connection, record);
            writer.write(record);
            updateRecordAfter(connection, record);
            i++;
          }
        }
      }
      return i;
    });
  }

  public UpdateStatement updateStatement(final TableRecordStoreConnection connection) {
    return new TableRecordStoreUpdateStatement(connection).table(getTable());
  }

  public UpdateStatement updateStatement(final TableRecordStoreConnection connection,
    final Consumer<UpdateStatement> action) {
    final var statement = new TableRecordStoreUpdateStatement(connection).table(getTable());
    action.accept(statement);
    return statement;
  }

  public void validateRecord(final MapEx record) {
    getRecordDefinition().validateRecord(record);
  }

}
