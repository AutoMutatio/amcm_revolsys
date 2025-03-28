package com.revolsys.record.schema;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import jakarta.servlet.http.HttpServletRequest;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataType;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.io.PathName;
import org.jeometry.common.logging.Logs;
import org.springframework.dao.PermissionDeniedDataAccessException;

import com.revolsys.collection.map.MapEx;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.io.BaseCloseable;
import com.revolsys.jdbc.JdbcConnection;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.ArrayChangeTrackRecord;
import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.ODataParser;
import com.revolsys.record.Record;
import com.revolsys.record.RecordFactory;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.format.json.JsonList;
import com.revolsys.record.io.format.json.JsonObject;
import com.revolsys.record.query.Cast;
import com.revolsys.record.query.ColumnReference;
import com.revolsys.record.query.Condition;
import com.revolsys.record.query.DeleteStatement;
import com.revolsys.record.query.Or;
import com.revolsys.record.query.Q;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.TableReference;
import com.revolsys.record.query.UpdateStatement;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.TransactionOptions;
import com.revolsys.transaction.TransactionRecordReader;
import com.revolsys.util.Property;

public class AbstractTableRecordStore implements RecordDefinitionProxy {
  protected static String DEFAULT = "Default_" + UUID.randomUUID();

  public static JsonObject schemaToJson(final RecordDefinition recordDefinition) {
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
        .addNotEmpty("title", field.getTitle().replace(" Ind", ""))
        .addNotEmpty("description", field.getDescription())
        .addValue("dataType", dataTypeString)
        .addValue("required", field.isRequired());
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

  private RecordStore recordStore;

  private final PathName tablePath;

  private final String typeName;

  private RecordDefinition recordDefinition;

  protected Map<QueryValue, Boolean> defaultSortOrder = new LinkedHashMap<>();

  private final Set<String> searchFieldNames = new LinkedHashSet<>();

  protected Map<String, RecordStoreSecurityPolicies> securityPolicyByGroup = new LinkedHashMap<>();

  private String tableAlias;

  protected final RecordFactory<RecordStoreChangeTrackRecord> changeTrackRecordFactory = recordDefinition -> new RecordStoreChangeTrackRecord(
    this);

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
        query.addOrderBy(fieldName, ascending);
      }
    }
    applyDefaultSortOrder(query);
  }

  protected void addSearchConditions(final Query query, final Or or, String search) {
    final String searchText = search.strip().toLowerCase();
    search = '%' + searchText + '%';
    for (final String fieldName : this.searchFieldNames) {
      final ColumnReference column = getTable().getColumn(fieldName);
      QueryValue left = column;
      final DataType dataType = column.getDataType();
      if (dataType != DataTypes.STRING) {
        if (!dataType.isNumeric() || dataType.isValid(searchText)) {
          left = new Cast(left, "text");
        } else {
          left = null;
        }
      }
      if (left != null) {
        final Condition condition = query.newCondition(left, Q.ILIKE, search);
        or.addCondition(condition);
      }
    }
  }

  protected void addSelect(final TableRecordStoreConnection connection, final Query query,
    final String selectItem) {
    final QueryValue selectClause = newSelectClause(query, selectItem);
    query.select(selectClause);
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
    if (search != null && search.strip().length() > 0) {
      final Or or = new Or();
      addSearchConditions(query, or, search);
      if (!or.isEmpty()) {
        query.and(or);
      }
    }
    return query;
  }

  protected void applyUpdates(final TableRecordStoreConnection connection,
    final RecordStoreChangeTrackRecord record, final RecordAccessType accessType,
    final Consumer<Record> action) {
    final RecordStoreSecurityPolicyForField policy = getSecurityPolicyForField(connection,
      accessType);
    try (
      BaseCloseable c = record.startUpdates(policy)) {
      action.accept(record);
    }
  }

  public boolean canEditField(final String fieldName) {
    if (!this.recordDefinition.hasField(fieldName) || this.recordDefinition.isIdField(fieldName)) {
      return false;
    }
    return true;
  }

  public boolean deleteRecord(final TableRecordStoreConnection connection, final Record record) {
    try (
      Transaction transaction = connection.newTransaction(TransactionOptions.REQUIRED)) {
      return this.recordStore.deleteRecord(record);
    }
  }

  @Override
  public DeleteStatement deleteStatement() {
    return new DeleteStatement().from(getTable());
  }

  public void enforceAccessTypeSecurityPolicy(final TableRecordStoreConnection connection,
    final RecordAccessType accessType) {
    getSecurityPolicyForField(connection, accessType)//
      .enforceAccessAllowed();
  }

  protected void executeUpdate(final TableRecordStoreConnection connection, final String sql,
    final Object... parameters) {
    if (this.recordStore instanceof JdbcRecordStore) {
      try (
        Transaction transaction = connection.newTransaction()) {
        this.recordStore.<JdbcRecordStore> getRecordStore().executeUpdate(sql, parameters);
      }
    }
    throw new UnsupportedOperationException("Must be a JDBC connection");

  }

  public Map<QueryValue, Boolean> getDefaultSortOrder() {
    return this.defaultSortOrder;
  }

  protected RecordStoreSecurityPolicies getGroupSecurityPolicy(final String groupName) {
    final RecordDefinition recordDefinition = getRecordDefinition();
    RecordStoreSecurityPolicies policy = this.securityPolicyByGroup.get(groupName);
    if (policy == null) {
      policy = new RecordStoreSecurityPolicies().setRecordDefinition(recordDefinition)
        .setLabel(groupName);
      this.securityPolicyByGroup.put(groupName, policy);
    }
    return policy;
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
    try (
      Transaction transaction = connection.newTransaction(TransactionOptions.REQUIRED)) {
      return (R)this.recordStore.getRecord(query);
    }
  }

  public Record getRecord(final TableRecordStoreConnection connection, final String fieldName,
    final Object value) {
    return newQuery(connection).and(fieldName, value).getRecord();
  }

  public Record getRecordById(final TableRecordStoreConnection connection, final Object id) {
    return getRecord(connection, "id", id);
  }

  public Record getRecordById(final TableRecordStoreConnection connection, final UUID id) {
    return getRecord(connection, "id", id);
  }

  protected long getRecordCount(final TableRecordStoreConnection connection, final Query query) {
    try (
      Transaction transaction = connection.newTransaction(TransactionOptions.REQUIRED)) {
      return this.recordStore.getRecordCount(query);
    }
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    return this.recordDefinition;
  }

  protected RecordReader getRecordReader(final TableRecordStoreConnection connection,
    final Query query) {
    final Transaction transaction = connection.newTransaction(TransactionOptions.REQUIRED);
    final RecordReader reader = this.recordStore.getRecords(query);
    return new TransactionRecordReader(reader, transaction);
  }

  protected RecordReader getRecordReader(final TableRecordStoreConnection connection,
    final Query query, final Transaction transaction) {
    if (transaction == null) {
      return getRecordReader(connection, query);
    } else {
      return this.recordStore.getRecords(query);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R extends RecordStore> R getRecordStore() {
    return (R)this.recordStore;
  }

  public Set<RecordStoreSecurityPolicies> getSecurityPolicies(
    final TableRecordStoreConnection connection, final RecordAccessType accessType) {
    if (this.securityPolicyByGroup.isEmpty()) {
      return null;
    } else {
      final Set<RecordStoreSecurityPolicies> policies = new LinkedHashSet<>();
      for (final String groupName : connection.getGroupNames()) {
        final RecordStoreSecurityPolicies policy = this.securityPolicyByGroup.get(groupName);
        if (policy != null && policy.isRecordChangeAllowed(accessType)) {
          policies.add(policy);
        }
      }
      if (policies.isEmpty()) {
        throw new PermissionDeniedDataAccessException(
          "No " + accessType + " permission on " + getRecordDefinition(), null);
      }
      return policies;
    }
  }

  public RecordStoreSecurityPolicyForField getSecurityPolicyForField(
    final TableRecordStoreConnection connection, final RecordAccessType accessType) {
    final Set<RecordStoreSecurityPolicies> policies = getSecurityPolicies(connection, accessType);
    return RecordStoreSecurityPolicyForField.create(this.recordDefinition, policies, accessType);
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
    return query.getRecordCount() > 0;
  }

  public boolean hasRecord(final TableRecordStoreConnection connection, final String fieldName,
    final Object value) {
    final Query query = newQuery(connection).and(fieldName, value);
    return hasRecord(connection, query);
  }

  protected Record initNewRecord(final TableRecordStoreConnection connection,
    final Record newRecord) {
    return newRecord;
  }

  protected void initSecurityPolicies() {
    withGroupSecurityPolicy(DEFAULT, policy -> policy.setLabel("Default"));
  }

  protected Record insertOrUpdateRecord(final TableRecordStoreConnection connection,
    final TableRecordStoreQuery query, final Consumer<Record> insertAction,
    final Consumer<Record> updateAction) {
    query.setRecordFactory(ArrayChangeTrackRecord.FACTORY);
    try (
      Transaction transaction = connection.newTransaction(TransactionOptions.REQUIRED)) {
      final ChangeTrackRecord record = query.getRecord();
      if (record == null) {
        return insertRecord(connection, insertAction);
      } else {
        updateAction.accept(record);
        updateRecordDo(connection, record);
        return record.newRecord();
      }
    }
  }

  public Record insertRecord(final TableRecordStoreConnection connection, final Record record) {
    try (
      Transaction transaction = connection.newTransaction(TransactionOptions.REQUIRED)) {
      insertRecordBefore(connection, record);
      validateRecord(record);
      this.recordStore.insertRecord(record);
      insertRecordAfter(connection, record);
    }
    return record;
  }

  protected void insertRecordBefore(final TableRecordStoreConnection connection,
    final Record record) {
  }

  protected Record insertRecord(final TableRecordStoreConnection connection,
    final ChangeTrackRecord record) {
    try (
      Transaction transaction = connection.newTransaction(TransactionOptions.REQUIRED)) {
      insertRecordBefore(connection, record);
      validateRecord(record);
      this.recordStore.insertRecord(record);
      insertRecordAfter(connection, record);
    }
    return record;
  }

  public Record insertRecord(final TableRecordStoreConnection connection,
    final Consumer<Record> action) {
    final RecordStoreChangeTrackRecord record = new RecordStoreChangeTrackRecord(this);
    initNewRecord(connection, record);
    applyUpdates(connection, record, RecordAccessType.INSERT, action);
    try (
      Transaction transaction = connection.newTransaction(TransactionOptions.REQUIRED)) {
      insertRecordBefore(connection, record);
      validateRecord(record);
      this.recordStore.insertRecord(record);
      insertRecordAfter(connection, record);
    }
    return record.newRecord();
  }

  protected Record insertRecord(final TableRecordStoreConnection connection, final Query query,
    final Consumer<Record> insertAction) {
    query.setRecordFactory(ArrayChangeTrackRecord.FACTORY);
    final ChangeTrackRecord changeTrackRecord = query.getRecord();
    if (changeTrackRecord == null) {
      return insertRecord(connection, insertAction);
    } else {
      return changeTrackRecord.newRecord();
    }
  }

  public Record insertRecord(final TableRecordStoreConnection connection, final Record record) {
    try (
      Transaction transaction = connection.newTransaction(TransactionOptions.REQUIRED)) {
      insertRecordBefore(connection, record);
      validateRecord(record);
      this.recordStore.insertRecord(record);
      insertRecordAfter(connection, record);
    }
    return record;
  }

  protected void insertRecordAfter(final TableRecordStoreConnection connection,
    final Record record) {
  }

  protected void insertRecordBefore(final TableRecordStoreConnection connection,
    final ChangeTrackRecord record) {
  }

  protected void insertRecordBefore(final TableRecordStoreConnection connection,
    final Record record) {
  }

  protected boolean isFieldReadonly(final String fieldName) {
    return this.recordDefinition.isIdField(fieldName);
  }

  public void lockTable() {
    if (this.recordStore instanceof JdbcRecordStore) {
      this.recordStore.<JdbcRecordStore> getRecordStore().lockTable(this.tablePath);
    }
  }

  public InsertUpdateBuilder newInsertUpdate(final TableRecordStoreConnection connection) {
    return new TableRecordStoreInsertUpdateBuilder(this, connection);
  }

  protected Condition newODataFilter(final RecordDefinition recordDefinition, String filter) {
    if (Property.hasValue(filter)) {
      filter = filter.replace("%2B", "+");
      return (Condition)ODataParser.parseFilter(recordDefinition, filter);
    } else {
      return null;
    }
  }

  public Query newQuery(final TableRecordStoreConnection connection) {
    return new TableRecordStoreQuery(connection, this);
  }

  public Query newQuery(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final int maxSize) {
    final String select = request.getParameter("$select");
    final String filter = request.getParameter("$filter");
    final String search = request.getParameter("$search");
    final String orderBy = request.getParameter("$orderby");
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

    final Query query = newQuery(connection, RecordAccessType.READ).setOffset(skip).setLimit(top);

    if (Property.hasValue(select)) {
      for (String selectItem : select.split(",")) {
        selectItem = selectItem.strip();
        addSelect(connection, query, selectItem);
      }
    }

    final RecordDefinition recordDefinition = query.getRecordDefinition();
    Condition filterCondition = newODataFilter(recordDefinition, filter);
    if (filterCondition != null) {
      filterCondition = alterCondition(request, connection, query, filterCondition);
      query.and(filterCondition.clone(null, recordDefinition));
    }
    applySearchCondition(query, search);
    addQueryOrderBy(query, orderBy);
    return query;
  }

  public Query newQuery(final TableRecordStoreConnection connection,
    final RecordAccessType accessType) {
    return TableRecordStoreQuery.create(connection, this, accessType);
  }

  protected void newQueryFilterConditionSearch(final Query query, final String search) {
    applySearchCondition(query, search);
  }

  public Record newRecord() {
    return getRecordDefinition().newRecord();
  }

  public Record newRecord(final MapEx values) {
    if (values == null) {
      return null;
    } else {
      final Record record = newRecord();
      for (final String fieldName : values.keySet()) {
        final Object value = values.getValue(fieldName);
        if (Property.hasValue(value)) {
          record.setValue(fieldName, value);
        }
      }
      return record;
    }
  }

  public Record newRecord(final TableRecordStoreConnection connection) {
    final Record newRecord = getRecordDefinition().newRecord();
    return initNewRecord(connection, newRecord);
  }

  public QueryValue newSelectClause(final Query query, final String selectItem) {
    return query.newSelectClause(selectItem);
  }

  public Transaction newTransaction() {
    return this.recordStore.newTransaction();
  }

  public UUID newUUID() {
    return UUID.randomUUID();
  }

  public JsonObject schemaToJson() {
    final RecordDefinition recordDefinition = getRecordDefinition();
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
        .addNotEmpty("title", field.getTitle().replace(" Ind", "").replace(" Code", ""))
        .addNotEmpty("description", field.getDescription())
        .addValue("dataType", dataTypeString)
        .addValue("required", field.isRequired());
      if (isFieldReadonly(fieldName)) {
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
      Logs.error(this, "Table doesn't exist\t" + getTypeName());
    } else {
      setRecordDefinitionPost(recordDefinition);
      initSecurityPolicies();
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
    try (
      Transaction transaction = connection.newTransaction(TransactionOptions.REQUIRED)) {
      query.setRecordFactory(ArrayChangeTrackRecord.FACTORY);
      final ChangeTrackRecord record = query.getRecord();
      if (record == null) {
        return null;
      } else {
        updateAction.accept(record);
        updateRecordDo(connection, record);
        return record.newRecord();
      }
    }
  }

  protected void updateRecordAfter(final TableRecordStoreConnection connection,
    final ChangeTrackRecord record) {
  }

  protected void updateRecordBefore(final TableRecordStoreConnection connection,
    final ChangeTrackRecord record) {
  }

  public Record updateRecordDo(final TableRecordStoreConnection connection,
    final ChangeTrackRecord record) {
    if (record.isModified()) {
      updateRecordBefore(connection, record);
      this.recordStore.updateRecord(record);
      updateRecordAfter(connection, record);
    }
    return record.newRecord();
  }

  protected Record updateRecordDo(final TableRecordStoreConnection connection,
    final RecordStoreChangeTrackRecord record, final Consumer<Record> updateAction) {
    applyUpdates(connection, record, RecordAccessType.UPDATE, updateAction);
    updateRecordDo(connection, record);
    return record.newRecord();
  }

  public int updateRecords(final TableRecordStoreConnection connection, final Query query,
    final Consumer<? super ChangeTrackRecord> updateAction) {
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
        if (record.isModified()) {
          updateRecordBefore(connection, record);
          writer.write(record);
          updateRecordAfter(connection, record);
          i++;
        }
      }
    }
    return i;
  }

  public UpdateStatement updateStatement() {
    return new UpdateStatement().from(getTable());
  }

  public void validateRecord(final MapEx record) {
    getRecordDefinition().validateRecord(record);
  }

  protected void withGroupSecurityPolicy(final Consumer<RecordStoreSecurityPolicies> action,
    final String... groupNames) {
    for (final String groupName : groupNames) {
      withGroupSecurityPolicy(groupName, action);
    }
  }

  protected void withGroupSecurityPolicy(final String groupName,
    final Consumer<RecordStoreSecurityPolicies> action) {
    final RecordStoreSecurityPolicies policy = getGroupSecurityPolicy(groupName);
    action.accept(policy);
  }
}
