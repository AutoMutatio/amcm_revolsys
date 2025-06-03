package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.revolsys.collection.Collector;
import com.revolsys.collection.iterator.Reader;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.value.Single;
import com.revolsys.data.type.DataType;
import com.revolsys.function.Lambdaable;
import com.revolsys.geometry.model.BoundingBox;
import com.revolsys.io.PathName;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.field.JdbcFieldDefinitions;
import com.revolsys.logging.Logs;
import com.revolsys.predicate.Predicates;
import com.revolsys.properties.BaseObjectWithProperties;
import com.revolsys.record.ArrayChangeTrackRecord;
import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.RecordFactory;
import com.revolsys.record.Records;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.query.functions.Exists;
import com.revolsys.record.query.functions.F;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.LockMode;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.transaction.Transactionable;
import com.revolsys.util.Cancellable;
import com.revolsys.util.CancellableProxy;
import com.revolsys.util.Property;
import com.revolsys.util.count.LabelCounters;

public class Query extends BaseObjectWithProperties implements Cloneable, CancellableProxy,
  Transactionable, QueryValue, TableReferenceProxy, Lambdaable<Query>, From, QueryStatement {
  private static void addFilter(final Query query, final RecordDefinition recordDefinition,
    final Map<String, ?> filter, final AbstractMultiCondition multipleCondition) {
    if (filter != null && !filter.isEmpty()) {
      for (final Entry<String, ?> entry : filter.entrySet()) {
        final String name = entry.getKey();
        final FieldDefinition fieldDefinition = recordDefinition.getField(name);
        if (fieldDefinition == null) {
          final Object value = entry.getValue();
          if (value == null) {
            multipleCondition.addCondition(Q.isNull(name));
          } else if (value instanceof Collection) {
            final Collection<?> values = (Collection<?>)value;
            multipleCondition.addCondition(new In(name, values));
          } else {
            multipleCondition.addCondition(Q.equal(name, value));
          }
        } else {
          final Object value = entry.getValue();
          if (value == null) {
            multipleCondition.addCondition(Q.isNull(name));
          } else if (value instanceof Collection) {
            final Collection<?> values = (Collection<?>)value;
            multipleCondition.addCondition(new In(fieldDefinition, values));
          } else {
            multipleCondition.addCondition(Q.equal(fieldDefinition, value));
          }
        }
      }
      query.setWhereCondition(multipleCondition);
    }
  }

  public static Query and(final RecordDefinition recordDefinition, final Map<String, ?> filter) {
    final Query query = new Query(recordDefinition);
    final And and = new And();
    addFilter(query, recordDefinition, filter, and);
    return query;
  }

  public static Query equal(final FieldDefinition field, final Object value) {
    final RecordDefinition recordDefinition = field.getRecordDefinition();
    final Query query = new Query(recordDefinition);
    final Value valueCondition = Value.newValue(field, value);
    final BinaryCondition equal = Q.equal(field, valueCondition);
    query.setWhereCondition(equal);
    return query;
  }

  public static Query equal(final RecordDefinitionProxy recordDefinition, final String name,
    final Object value) {
    final FieldDefinition fieldDefinition = recordDefinition.getFieldDefinition(name);
    if (fieldDefinition == null) {
      return null;
    } else {
      final Query query = Query.newQuery(recordDefinition);
      final Value valueCondition = Value.newValue(fieldDefinition, value);
      final BinaryCondition equal = Q.equal(name, valueCondition);
      query.setWhereCondition(equal);
      return query;
    }
  }

  public static Query intersects(final RecordDefinition recordDefinition,
    final BoundingBox boundingBox) {
    final FieldDefinition geometryField = recordDefinition.getGeometryField();
    if (geometryField == null) {
      return null;
    } else {
      final Query query = recordDefinition.newQuery();
      F.envelopeIntersects(query, boundingBox);
      return query;
    }

  }

  public static Query intersects(final RecordStore recordStore, final PathName path,
    final BoundingBox boundingBox) {
    final RecordDefinition recordDefinition = recordStore.getRecordDefinition(path);
    return intersects(recordDefinition, boundingBox);
  }

  public static Query newQuery(final RecordDefinitionProxy recordDefinition) {
    return newQuery(recordDefinition, null);
  }

  public static Query newQuery(final RecordDefinitionProxy recordDefinition,
    final Condition whereCondition) {
    final TableReference table = TableReference.getTableReference(recordDefinition);
    return new Query(table, whereCondition);
  }

  public static Query or(final RecordDefinition recordDefinition, final Map<String, ?> filter) {
    final Query query = new Query(recordDefinition);
    final Or or = new Or();
    addFilter(query, recordDefinition, filter, or);
    return query;
  }

  public static Query orderBy(final PathName pathName, final String... orderBy) {
    final Query query = new Query(pathName);
    query.setOrderByFieldNames(orderBy);
    return query;
  }

  public static Query where(
    final BiFunction<FieldDefinition, Object, BinaryCondition> whereFunction,
    final FieldDefinition field, final Object value) {
    final RecordDefinition recordDefinition = field.getRecordDefinition();
    final Query query = new Query(recordDefinition);
    final Value valueCondition = Value.newValue(field, value);
    final BinaryCondition equal = whereFunction.apply(field, valueCondition);
    query.setWhereCondition(equal);
    return query;
  }

  private int fetchSize = 100;

  private List<Join> joins = new ArrayList<>();

  private boolean distinct = false;

  private Cancellable cancellable;

  private String baseFileName;

  private RecordFactory<Record> recordFactory;

  private List<QueryValue> selectExpressions = new ArrayList<>();

  private final List<QueryValue> groupBy = new ArrayList<>();

  private From from;

  private int limit = Integer.MAX_VALUE;

  private LockMode lockMode = LockMode.NONE;

  private int offset = 0;

  private List<OrderBy> orderBy = new ArrayList<>();

  private List<Object> parameters = new ArrayList<>();

  private TableReference table;

  private String sql;

  private LabelCounters labelCountMap;

  private Condition whereCondition = Condition.ALL;

  private final List<WithQuery> withQueries = new ArrayList<>();

  private RecordStore recordStore;

  private Union union;

  private Condition having = Condition.ALL;

  private boolean returnCount;

  private final JsonObject resultHeaders = JsonObject.hash();

  public Query() {
    this("/Record");
  }

  public Query(final PathName typePath) {
    this(typePath, null);
  }

  public Query(final PathName typePath, final Condition whereCondition) {
    this(new TableReferenceImpl(typePath), whereCondition);
  }

  public Query(final RecordStore recordStore) {
    this.recordStore = recordStore;
  }

  public Query(final RecordStore recordStore, final TableReferenceProxy table) {
    this.recordStore = recordStore;
    this.table = table.getTableReference();
  }

  public Query(final String typePath) {
    this(typePath, null);
  }

  public Query(final String typePath, final Condition whereCondition) {
    this(PathName.newPathName(typePath), whereCondition);
  }

  public Query(final TableReferenceProxy table) {
    this.table = table.getTableReference();
  }

  public Query(final TableReferenceProxy table, final Condition whereCondition) {
    this.table = table.getTableReference();
    setWhereCondition(whereCondition);
  }

  public Query accept(final Consumer<Query> action) {
    action.accept(this);
    return this;
  }

  public Query addGroupBy(final Object groupByItem) {
    if (groupByItem instanceof final QueryValue queryValue) {
      this.groupBy.add(queryValue);
    } else if (groupByItem instanceof final CharSequence fieldName) {
      final ColumnReference column = this.table.getColumn(fieldName);
      this.groupBy.add(column);
    } else if (groupByItem instanceof final Integer index) {
      final ColumnIndex columnIndex = new ColumnIndex(index);
      this.groupBy.add(columnIndex);
    } else {
      throw new IllegalArgumentException(groupByItem.toString());
    }
    return this;
  }

  public Query addJoin(final Join join) {
    this.joins.add(join);
    return this;
  }

  public Query addOrderBy(final CharSequence field) {
    return addOrderBy(field, true);
  }

  public Query addOrderBy(final Map<?, Boolean> orderBy) {
    for (final Entry<?, Boolean> entry : orderBy.entrySet()) {
      final Object field = entry.getKey();
      final Boolean ascending = entry.getValue();
      addOrderBy(field, ascending);
    }
    return this;
  }

  public Query addOrderBy(final Object field) {
    return addOrderBy(field, true);
  }

  public Query addOrderBy(final Object field, final boolean ascending) {
    QueryValue queryValue;
    if (field instanceof QueryValue) {
      queryValue = (QueryValue)field;
    } else if (field instanceof final CharSequence fieldName) {
      if (hasField(fieldName)) {
        queryValue = getColumn(fieldName);
      } else {
        try {
          queryValue = new ColumnIndex(Integer.parseInt(fieldName.toString()));
        } catch (final NumberFormatException e) {
          queryValue = new Column(fieldName);
        }
      }
    } else if (field instanceof final Integer index) {
      queryValue = new ColumnIndex(index);
    } else {
      throw new IllegalArgumentException("Not a field name: " + field);
    }
    final OrderBy order = new OrderBy(queryValue, ascending);
    return addOrderBy(order);
  }

  public Query addOrderBy(final OrderBy order) {
    for (final ListIterator<OrderBy> iterator = this.orderBy.listIterator(); iterator.hasNext();) {
      final OrderBy order2 = iterator.next();
      if (order2.getField()
        .equals(order.getField())) {
        iterator.set(order);
        return this;
      }
    }

    this.orderBy.add(order);
    return this;
  }

  public void addOrderBy(final SqlAppendable sql, final TableReferenceProxy table,
    final List<OrderBy> orderBy) {
    if (!orderBy.isEmpty()) {
      sql.append(" ORDER BY ");
      appendOrderByFields(sql, table, orderBy);
    }
  }

  public Query addOrderById() {
    final RecordDefinition recordDefinition = getRecordDefinition();
    if (recordDefinition != null) {
      for (final FieldDefinition idField : recordDefinition.getIdFields()) {
        addOrderBy(idField);
      }
    }
    return this;
  }

  @Deprecated
  public Query addParameter(final Object value) {
    this.parameters.add(value);
    return this;
  }

  public Query addResultHeader(final String key, final Object value) {
    this.resultHeaders.addNotEmpty(key, value);
    return this;
  }

  public Query addResultHeaders(final MapEx values) {
    this.resultHeaders.addValues(values);
    return this;
  }

  public Query and(final ColumnReference left, final Object value) {
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue)value;
      } else {
        right = new Value(left, value);
      }
      condition = new Equal(left, right);
    }
    return and(condition);
  }

  public Query and(final Condition condition) {
    if (condition != null && !condition.isEmpty()) {
      final RecordDefinition recordDefinition = getRecordDefinition();
      condition.changeRecordDefinition(recordDefinition, recordDefinition);
      this.whereCondition = this.whereCondition.and(condition);
    }
    return this;
  }

  public Query and(final Condition... conditions) {
    if (conditions != null) {
      Condition whereCondition = getWhereCondition();
      for (final Condition condition : conditions) {
        if (Property.hasValue(condition)) {
          whereCondition = whereCondition.and(condition);
        }
      }
      setWhereCondition(whereCondition);
    }
    return this;
  }

  public Query and(final Iterable<? extends Condition> conditions) {
    if (conditions != null) {
      Condition whereCondition = getWhereCondition();
      for (final Condition condition : conditions) {
        if (Property.hasValue(condition)) {
          whereCondition = whereCondition.and(condition);
        }
      }
      setWhereCondition(whereCondition);
    }
    return this;
  }

  public Query and(final QueryValue fieldName,
    final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    final Condition condition = newCondition(fieldName, operator, value);
    return and(condition);
  }

  public Query and(final String fieldName,
    final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    final Condition condition = newCondition(fieldName, operator, value);
    return and(condition);
  }

  public Query and(final String fieldName,
    final java.util.function.Function<QueryValue, Condition> operator) {
    final Condition condition = newCondition(fieldName, operator);
    return and(condition);
  }

  public Query and(final String fieldName, final Object value) {
    final ColumnReference left = this.table.getColumn(fieldName);
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue)value;
      } else {
        right = new Value(left, value);
      }
      condition = new Equal(left, right);
    }
    return and(condition);
  }

  public Query and(final TableReferenceProxy table, final String columnName, final Object value) {
    final ColumnReference column = table.getColumn(columnName);
    return and(column, value);
  }

  public Query andEqualId(final Object id) {
    final RecordDefinition recordDefinition = getRecordDefinition();
    final String idFieldName = recordDefinition.getIdFieldName();
    return and(idFieldName, Q.EQUAL, id);
  }

  /**
   * Create an Or from the conditions and and it to this query;
   *
   * @param conditions
   * @return
   */
  public Query andOr(final Condition... conditions) {
    if (conditions != null && conditions.length > 0) {
      final Or or = new Or(conditions);
      if (!or.isEmpty()) {
        and(or);
      }
    }
    return this;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    appendSql(sql);
  }

  @Override
  public void appendFrom(final SqlAppendable string) {
    this.from.appendFrom(string);
  }

  public SqlAppendable appendOrderByFields(final SqlAppendable sql, final TableReferenceProxy table,
    final List<OrderBy> orderBy) {
    boolean first = true;
    for (final OrderBy order : orderBy) {
      if (first) {
        first = false;
      } else {
        sql.append(", ");
      }
      order.appendSql(this, table, sql);
    }
    return sql;
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    if (!this.withQueries.isEmpty()) {
      for (final var with : this.withQueries) {
        index = with.appendParameters(index, statement);
      }
    }
    for (final Object parameter : getParameters()) {
      final JdbcFieldDefinition field = JdbcFieldDefinitions.newFieldDefinition(parameter);
      try {
        index = field.setPreparedStatementValue(statement, index, parameter);
      } catch (final SQLException e) {
        throw new RuntimeException("Error setting value:" + parameter, e);
      }
    }
    index = appendSelectParameters(index, statement);
    for (final Join join : getJoins()) {
      index = join.appendParameters(index, statement);
    }
    final Condition where = getWhereCondition();
    if (!where.isEmpty()) {
      index = where.appendParameters(index, statement);
    }
    if (!this.having.isEmpty()) {
      index = this.having.appendParameters(index, statement);
    }
    if (this.union != null) {
      index = this.union.appendParameters(index, statement);
    }
    return index;
  }

  public void appendSelect(final SqlAppendable sql) {
    final TableReference table = this.table;
    final List<QueryValue> select = getSelect();
    if (select.isEmpty()) {
      table.appendSelectAll(this, sql);
    } else {
      boolean first = true;
      for (final QueryValue selectItem : select) {
        if (first) {
          first = false;
        } else {
          sql.append(", ");
        }
        if (table == null) {
          selectItem.appendSelect(this, this.recordStore, sql);
        } else {
          table.appendSelect(this, sql, selectItem);
        }
      }
    }
  }

  public int appendSelectParameters(int index, final PreparedStatement statement) {
    for (final QueryValue select : this.selectExpressions) {
      index = select.appendParameters(index, statement);
    }
    return index;
  }

  void appendSql(final SqlAppendable sql) {
    appendSql(sql, this.table, this.orderBy);
  }

  protected void appendSql(final SqlAppendable sql, final TableReferenceProxy table,
    final List<OrderBy> orderBy) {
    From from = getFrom();
    if (from == null && table != null) {
      from = table.getTableReference();
    }
    final List<Join> joins = getJoins();
    final LockMode lockMode = getLockMode();
    final boolean distinct = isDistinct();
    final List<QueryValue> groupBy = getGroupBy();
    if (this.union != null) {
      sql.append('(');
    }
    if (!this.withQueries.isEmpty()) {
      sql.append("WITH ");
      boolean first = true;
      for (final WithQuery withQuery : this.withQueries) {
        if (first) {
          first = false;
        } else {
          sql.append("\n");
        }
        withQuery.appendSql(sql);
      }
      sql.append(" ");
    }
    sql.append("SELECT ");
    if (distinct) {
      sql.append("DISTINCT ");
    }
    appendSelect(sql);
    if (from != null) {
      sql.append(" FROM ");
      from.appendFromWithAlias(sql);
    }
    for (final Join join : joins) {
      appendQueryValue(sql, join);
    }
    appendWhere(sql, sql.isUsePlaceholders());

    if (groupBy != null) {
      boolean hasGroupBy = false;
      for (final QueryValue groupByItem : groupBy) {
        if (hasGroupBy) {
          sql.append(", ");
        } else {
          sql.append(" GROUP BY ");
          hasGroupBy = true;
        }
        table.getTableReference()
          .appendQueryValue(this, sql, groupByItem);
      }
    }
    if (!this.having.isEmpty()) {
      sql.append(" HAVING ");
      appendQueryValue(sql, this.having);
    }

    addOrderBy(sql, table, orderBy);

    lockMode.append(sql);

    if (this.union != null) {
      sql.append(')');
      this.union.appendSql(sql);
    }
  }

  public void appendWhere(final SqlAppendable sql, final boolean usePlaceholders) {
    final Condition where = getWhereCondition();
    if (!where.isEmpty()) {
      sql.append(" WHERE ");
      appendQueryValue(sql, where);
    }
  }

  public Exists asExists() {
    return new Exists(this);
  }

  public Query clearOrderBy() {
    this.orderBy.clear();
    return this;
  }

  public Query clearSelect() {
    this.selectExpressions.clear();
    return this;
  }

  @Override
  public Query clone() {
    final Query clone = (Query)super.clone();
    clone.table = this.table;
    clone.selectExpressions = new ArrayList<>(clone.selectExpressions);
    clone.joins = new ArrayList<>(clone.joins);
    clone.selectExpressions = new ArrayList<>(clone.selectExpressions);
    clone.parameters = new ArrayList<>(this.parameters);
    clone.orderBy = new ArrayList<>(this.orderBy);
    if (this.whereCondition != null) {
      clone.whereCondition = this.whereCondition.clone();
    }
    if (!clone.getSelect()
      .isEmpty() || clone.whereCondition != null) {
      clone.sql = null;
    }
    return clone;
  }

  @Override
  public Query clone(final TableReference oldTable, final TableReference newTable) {
    final Query clone = (Query)super.clone();
    clone.table = this.table;
    clone.selectExpressions = QueryValue.cloneQueryValues(oldTable, newTable,
      clone.selectExpressions);
    clone.joins = QueryValue.cloneQueryValues(oldTable, newTable, this.joins);
    clone.parameters = new ArrayList<>(this.parameters);
    clone.orderBy = new ArrayList<>(this.orderBy);
    if (this.whereCondition != null) {
      clone.whereCondition = this.whereCondition.clone(oldTable, newTable);
    }
    if (!clone.getSelect()
      .isEmpty() || clone.whereCondition != null) {
      clone.sql = null;
    }
    return clone;
  }

  public <O> O collect(final Collector<Record, O> collector) {
    final var result = collector.newResult();
    forEachRecord(value -> collector.collect(result, value));
    return result;
  }

  public int deleteRecords() {
    return getRecordStore().deleteRecords(this);
  }

  public boolean exists() {
    return getRecordCount() != 0;
  }

  public int fetchSize() {
    return this.fetchSize;
  }

  public Query fetchSize(final int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  /**
   * Process each result record. Ensures that there is an open transaction
   * @param action
   */
  public final void forEachRecord(final Consumer<? super Record> action) {
    transactionRun(() -> {
      try (
        RecordReader reader = getRecordReader()) {
        reader.forEach(action);
      }
    });
  }

  @SuppressWarnings("unchecked")
  public <R extends MapEx> void forEachRecord(final Iterable<R> records,
    final Consumer<? super R> consumer) {
    final List<OrderBy> orderBy = getOrderBy();
    final Predicate<R> filter = (Predicate<R>)getWhereCondition();
    if (orderBy.isEmpty()) {
      if (filter == null) {
        records.forEach(consumer);
      } else {
        records.forEach(record -> {
          if (filter.test(record)) {
            consumer.accept(record);
          }
        });
      }
    } else {
      final Comparator<R> comparator = Records.newComparatorOrderBy(orderBy);
      final List<R> results = Predicates.filter(records, filter);
      results.sort(comparator);
      results.forEach(consumer);
    }
  }

  public String getBaseFileName() {
    return this.baseFileName;
  }

  @Override
  public Cancellable getCancellable() {
    return this.cancellable;
  }

  public From getFrom() {
    return this.from;
  }

  @Override
  public FieldDefinition getGeometryField() {
    return getRecordDefinition().getGeometryField();
  }

  public List<QueryValue> getGroupBy() {
    return this.groupBy;
  }

  public Join getJoin(final TableReferenceProxy tableProxy, final String alias) {
    if (tableProxy != null) {
      final var table = tableProxy.getTableReference();
      for (final var join : this.joins) {
        if (join.getTable() == table) {
          if (DataType.equal(alias, join.getTableAlias())) {
            return join;
          }
        }
      }
    }
    return null;
  }

  public List<Join> getJoins() {
    return this.joins;
  }

  public int getLimit() {
    return this.limit;
  }

  public LockMode getLockMode() {
    return this.lockMode;
  }

  public int getOffset() {
    return this.offset;
  }

  public List<OrderBy> getOrderBy() {
    return new ArrayList<>(this.orderBy);
  }

  public List<Object> getParameters() {
    return this.parameters;
  }

  public String getQualifiedTableName() {
    if (this.table == null) {
      return null;
    } else {
      return this.table.getQualifiedTableName();
    }
  }

  @SuppressWarnings("unchecked")
  public <R extends Record> R getRecord() {
    return (R)getRecordDefinition().getRecord(this);
  }

  public long getRecordCount() {
    return getRecordStore().getRecordCount(this);
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    if (this.table == null) {
      return null;
    } else {
      return this.table.getRecordDefinition();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V extends Record> RecordFactory<V> getRecordFactory() {
    return (RecordFactory<V>)this.recordFactory;
  }

  public RecordReader getRecordReader() {
    return getRecordStore().getRecords(this);
  }

  public ListEx<Record> getRecords() {
    final ListEx<Record> records = Lists.newArray();
    forEachRecord(records::add);
    return records;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R extends RecordStore> R getRecordStore() {
    if (this.recordStore == null) {
      final var recordDefinition = getRecordDefinition();
      if (recordDefinition == null) {
        if (this.from == null) {
          return null;
        } else {
          return this.from.getRecordStore();
        }
      } else {
        return recordDefinition.getRecordStore();
      }
    } else {
      return (R)this.recordStore;
    }
  }

  public JsonObject getResultHeaders() {
    return this.resultHeaders;
  }

  public List<QueryValue> getSelect() {
    return this.selectExpressions;
  }

  public int getSelectCount() {
    return this.selectExpressions.size();
  }

  @SuppressWarnings({
    "unchecked", "rawtypes"
  })
  public List<QueryValue> getSelectExpressions() {
    if (this.selectExpressions.isEmpty() && this.table != null) {
      final RecordDefinition recordDefinition = this.table.getRecordDefinition();
      if (recordDefinition != null) {
        return (List)recordDefinition.getFieldDefinitions();
      }
    }
    return this.selectExpressions;
  }

  public String getSelectSql() {
    final boolean usePlaceholders = true;
    return getSelectSql(usePlaceholders);
  }

  private String getSelectSql(final boolean usePlaceholders) {
    String sql = getSql();
    final List<OrderBy> orderBy = getOrderBy();
    final TableReference table = getTable();
    final RecordDefinition recordDefinition = getRecordDefinition();
    if (sql == null) {
      sql = newSelectSql(orderBy, table, usePlaceholders);
    } else {
      if (sql.toUpperCase()
        .startsWith("SELECT * FROM ")) {
        final StringBuilderSqlAppendable sqlBuilder = newSqlAppendable();
        sqlBuilder.append("SELECT ");
        if (recordDefinition == null) {
          sqlBuilder.append("*");
        } else {
          recordDefinition.appendSelectAll(this, sqlBuilder);
        }
        sqlBuilder.append(" FROM ");
        sqlBuilder.append(sql.substring(14));
        sql = sqlBuilder.toSqlString();
      }
      if (!orderBy.isEmpty()) {
        final StringBuilderSqlAppendable sqlBuilder = newSqlAppendable();
        sqlBuilder.append(sql);
        addOrderBy(sqlBuilder, table, orderBy);
        sql = sqlBuilder.toSqlString();
      }
    }
    return sql;
  }

  public String getSql() {
    return this.sql;
  }

  public LabelCounters getStatistics() {
    return this.labelCountMap;
  }

  public TableReference getTable() {
    return this.table;
  }

  public PathName getTablePath() {
    if (this.table == null) {
      return null;
    } else {
      return this.table.getTablePath();
    }
  }

  @Override
  public TableReference getTableReference() {
    return getRecordDefinition();
  }

  @Override
  public <V> V getValue(final MapEx record) {
    return null;
  }

  public String getWhere() {
    return this.whereCondition.toFormattedString();
  }

  public Condition getWhereCondition() {
    return this.whereCondition;
  }

  public Query groupBy(final Object... groupBy) {
    this.groupBy.clear();
    if (groupBy != null) {
      for (final Object groupByItem : groupBy) {
        addGroupBy(groupByItem);
      }
    }
    return this;
  }

  public boolean hasJoin(final TableReferenceProxy tableProxy, final String alias) {
    if (tableProxy != null) {
      final var table = tableProxy.getTableReference();
      for (final var join : this.joins) {
        if (join.getTable() == table) {
          if (DataType.equal(alias, join.getTableAlias())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public boolean hasOrderBy(final QueryValue column) {
    for (final OrderBy orderBy : this.orderBy) {
      if (orderBy.getField()
        .equals(column)) {
        return true;
      }
    }
    return false;
  }

  public boolean hasOrderBy(final String fieldName) {
    for (final OrderBy order : this.orderBy) {
      if (order.isField(fieldName)) {
        return true;
      }
    }
    return false;
  }

  public boolean hasSelect() {
    return !this.selectExpressions.isEmpty();
  }

  public Query having(final Consumer<WhereConditionBuilder> action) {
    final WhereConditionBuilder builder = new WhereConditionBuilder(getTableReference());
    this.having = builder.build(action);
    return this;
  }

  public Record insertRecord(final Supplier<Record> newRecordSupplier) {
    final ChangeTrackRecord changeTrackRecord = getRecord();
    if (changeTrackRecord == null) {
      final Record newRecord = newRecordSupplier.get();
      if (newRecord == null) {
        return null;
      } else {
        getRecordStore().insertRecord(newRecord);
        return newRecord;
      }
    } else {
      return changeTrackRecord.newRecord();
    }
  }

  public boolean isCustomResult() {
    if (!getJoins().isEmpty()) {
      return true;
    } else if (!getGroupBy().isEmpty()) {
      return true;
    } else if (this.selectExpressions.isEmpty()) {
      return false;
    } else if (this.selectExpressions.size() == 1
      && this.selectExpressions.get(0) instanceof AllColumns) {
      return false;
    } else {
      return true;
    }
  }

  public boolean isDistinct() {
    return this.distinct;
  }

  public boolean isReturnCount() {
    return this.returnCount;
  }

  public boolean isSelectEmpty() {
    return this.selectExpressions.isEmpty();
  }

  public Query join(final BiConsumer<Query, Join> action) {
    return join(JoinType.JOIN, action);
  }

  public Join join(final JoinType joinType) {
    final Join join = new Join(joinType);
    this.joins.add(join);
    return join;
  }

  public Query join(final JoinType joinType, final BiConsumer<Query, Join> action) {
    final Join join = join(joinType);
    action.accept(this, join);
    return this;
  }

  public Join join(final TableReferenceProxy table) {
    return join(JoinType.JOIN).table(table);
  }

  @Override
  public Condition newCondition(final CharSequence fieldName,
    final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    final ColumnReference left = this.table.getColumn(fieldName);
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue)value;
      } else {
        right = new Value(left, value);
      }
      condition = operator.apply(left, right);
    }
    return condition;
  }

  @Override
  public Condition newCondition(final CharSequence fieldName,
    final java.util.function.Function<QueryValue, Condition> operator) {
    final ColumnReference column = this.table.getColumn(fieldName);
    final Condition condition = operator.apply(column);
    return condition;
  }

  @Override
  public Condition newCondition(final QueryValue left,
    final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue)value;
      } else if (left instanceof ColumnReference) {
        right = new Value((ColumnReference)left, value);
      } else {
        right = Value.newValue(value);
      }
      condition = operator.apply(left, right);
    }
    return condition;
  }

  public String newDeleteSql() {
    final StringBuilderSqlAppendable sql = newSqlAppendable();
    sql.append("DELETE FROM ");
    From from = getFrom();
    if (from == null) {
      from = this.table;
    }
    from.appendFromWithAlias(sql);
    appendWhere(sql, true);
    return sql.toSqlString();
  }

  public Query newQuery(final RecordDefinition recordDefinition) {
    final Query query = clone();
    query.setRecordDefinition(recordDefinition);
    return query;
  }

  public <QV extends QueryValue> QV newQueryValue(final CharSequence fieldName,
    final BiFunction<QueryValue, QueryValue, QV> operator, final Object value) {
    final ColumnReference left = this.table.getColumn(fieldName);
    QueryValue right;
    if (value instanceof QueryValue) {
      right = (QueryValue)value;
    } else {
      right = new Value(left, value);
    }
    return operator.apply(left, right);
  }

  public <QV extends QueryValue> QV newQueryValue(final CharSequence fieldName,
    final java.util.function.Function<QueryValue, QV> operator) {
    final ColumnReference column = this.table.getColumn(fieldName);
    return operator.apply(column);
  }

  public Record newRecord() {
    return getRecordDefinition().newRecord();
  }

  public QueryValue newSelectClause(final Object select) {
    QueryValue selectExpression;
    if (select instanceof final QueryValue queryValue) {
      selectExpression = queryValue;
    } else if (select instanceof final CharSequence chars) {
      final String name = chars.toString();
      selectExpression = getTable().columnByPath(name);
      if (selectExpression == null || !(selectExpression instanceof ColumnReference)) {
        final var alias = name.substring(name.lastIndexOf('.') + 1);
        if (selectExpression == null) {
          selectExpression = Q.sql("NULL");
        }
        selectExpression = selectExpression.toAlias(alias);
      }
    } else {
      throw new IllegalArgumentException("Not a valid select expression :" + select);
    }
    return selectExpression;
  }

  public String newSelectSql(final List<OrderBy> orderBy, final TableReferenceProxy table,
    final boolean usePlaceholders) {
    final StringBuilderSqlAppendable sql = newSqlAppendable();
    sql.setUsePlaceholders(usePlaceholders);
    appendSql(sql, table, orderBy);
    return sql.toSqlString();
  }

  protected StringBuilderSqlAppendable newSqlAppendable() {
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder();
    final RecordDefinition recordDefinition = getRecordDefinition();
    if (recordDefinition != null) {
      sql.setRecordStore(recordDefinition.getRecordStore());
    }
    return sql;
  }

  public Query or(final CharSequence fieldName,
    final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    final ColumnReference left = this.table.getColumn(fieldName);
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue)value;
      } else {
        right = Value.newValue(value);
      }
      condition = operator.apply(left, right);
    }
    return or(condition);
  }

  public Query or(final CharSequence fieldName,
    final java.util.function.Function<QueryValue, Condition> operator) {
    final Condition condition = newCondition(fieldName, operator);
    return or(condition);
  }

  public Query or(final Condition condition) {
    final Condition whereCondition = getWhereCondition();
    if (whereCondition.isEmpty()) {
      setWhereCondition(condition);
    } else if (whereCondition instanceof final Or or) {
      or.or(condition);
    } else {
      setWhereCondition(new Or(whereCondition, condition));
    }
    return this;
  }

  public Query orderBy(final Object... orderBy) {
    clearOrderBy();
    for (final Object orderByItem : orderBy) {
      addOrderBy(orderByItem);
    }
    return this;
  }

  @SuppressWarnings("unchecked")
  public <R extends Reader<V>, V> R reader() {
    return (R)getRecordDefinition().getRecordStore()
      .getRecords(this);
  }

  public Query readerConsume(final Consumer<RecordReader> action) {
    try (
      var reader = getRecordReader()) {
      action.accept(getRecordReader());
    }
    return this;
  }

  public <O> O readerMap(final Function<RecordReader, O> action) {
    try (
      var reader = getRecordReader()) {
      return action.apply(reader);
    }
  }

  public Query recordStore(final RecordStore recordStore) {
    this.recordStore = recordStore;
    return this;
  }

  public void removeSelect(final String name) {
    for (final Iterator<QueryValue> iterator = this.selectExpressions.iterator(); iterator
      .hasNext();) {
      final QueryValue queryValue = iterator.next();
      if (queryValue instanceof final Column column) {
        if (column.getName()
          .equals(name)) {
          iterator.remove();
        }

      }

    }
  }

  public Query select(final Collection<?> selectExpressions) {
    this.selectExpressions.clear();
    for (final Object selectExpression : selectExpressions) {
      select(selectExpression);
    }
    return this;
  }

  public Query select(final Object select) {
    final QueryValue selectExpression = newSelectClause(select);
    this.selectExpressions.add(selectExpression);
    return this;
  }

  public Query select(final Object... select) {
    this.selectExpressions.clear();
    for (final Object selectItem : select) {
      select(selectItem);
    }
    return this;
  }

  public Query select(final QueryValue select) {
    this.selectExpressions.add(select);
    return this;
  }

  public Query select(final TableReferenceProxy table, final String fieldName) {
    final ColumnReference column = table.getColumn(fieldName);
    this.selectExpressions.add(column);
    return this;
  }

  public Query select(final TableReferenceProxy table, final String... fieldNames) {
    for (final String fieldName : fieldNames) {
      final ColumnReference column = table.getColumn(fieldName);
      this.selectExpressions.add(column);
    }
    return this;
  }

  public Query selectAlias(final ColumnReference column, final String alias) {
    final ColumnAlias columnAlias = new ColumnAlias(column, alias);
    this.selectExpressions.add(columnAlias);
    return this;
  }

  public Query selectAlias(final QueryValue value, final String alias) {
    final SelectAlias columnAlias = new SelectAlias(value, alias);
    this.selectExpressions.add(columnAlias);
    return this;
  }

  public Query selectAlias(final String name, final String alias) {
    final ColumnReference column = this.table.getColumn(name);
    return selectAlias(column, alias);
  }

  public Query selectAlias(final TableReferenceProxy table, final String fieldName,
    final String alias) {
    final ColumnReference column = table.getColumn(fieldName);
    return selectAlias(column, alias);
  }

  public Query selectAll() {
    return select(getRecordDefinition().getFieldDefinitions());
  }

  public Query selectCsv(final String select) {
    if (Property.hasValue(select)) {
      for (String selectItem : select.split(",")) {
        selectItem = selectItem.strip();
        select(selectItem);
      }
    }
    return this;
  }

  public Query setBaseFileName(final String baseFileName) {
    this.baseFileName = baseFileName;
    return this;
  }

  public Query setCancellable(final Cancellable cancellable) {
    this.cancellable = cancellable;
    return this;
  }

  public Query setDistinct(final boolean distinct) {
    this.distinct = distinct;
    return this;
  }

  public Query setFrom(final From from) {
    this.from = from;
    return this;
  }

  public Query setFrom(final From from, final String alias) {
    final var fromAlias = new FromAlias(from, alias);
    return setFrom(fromAlias);
  }

  public Query setFrom(final String from) {
    final var fromSql = new FromSql(from);
    return setFrom(fromSql);
  }

  public Query setFrom(final String from, final String alias) {
    final FromSql fromSql = new FromSql(from);
    final var fromAlias = new FromAlias(fromSql, alias);
    return setFrom(fromAlias);
  }

  public Query setGroupBy(final List<?> groupBy) {
    this.groupBy.clear();
    if (groupBy != null) {
      for (final Object groupByItem : groupBy) {
        addGroupBy(groupByItem);

      }
    }
    return this;
  }

  public Query setGroupBy(final String... fieldNames) {
    final List<String> groupBy = Arrays.asList(fieldNames);
    return setGroupBy(groupBy);
  }

  public Query setLimit(final int limit) {
    if (limit < 0) {
      this.limit = Integer.MAX_VALUE;
    } else {
      this.limit = limit;
    }
    return this;
  }

  public Query setLockMode(final LockMode lockMode) {
    if (lockMode == null) {
      this.lockMode = LockMode.NONE;
    } else {
      this.lockMode = lockMode;
    }
    return this;
  }

  public Query setOffset(final int offset) {
    if (offset > 0) {
      this.offset = offset;
    }
    return this;
  }

  public Query setOrderBy(final CharSequence field) {
    clearOrderBy();
    return addOrderBy(field);
  }

  public Query setOrderBy(final Map<?, Boolean> orderBy) {
    clearOrderBy();
    if (orderBy != null) {
      for (final Entry<?, Boolean> entry : orderBy.entrySet()) {
        final Object field = entry.getKey();
        final Boolean ascending = entry.getValue();
        addOrderBy(field, ascending);
      }
    }
    return this;
  }

  public Query setOrderByFieldNames(final List<? extends CharSequence> orderBy) {
    clearOrderBy();
    for (final CharSequence field : orderBy) {
      addOrderBy(field);
    }
    return this;
  }

  public Query setOrderByFieldNames(final String... orderBy) {
    clearOrderBy();
    for (final CharSequence field : orderBy) {
      addOrderBy(field);
    }
    return this;
  }

  public Query setRecordDefinition(final RecordDefinition recordDefinition) {
    if (this.table == null) {
      this.table = recordDefinition;

    }
    if (this.whereCondition != null) {
      this.whereCondition.changeRecordDefinition(getRecordDefinition(), recordDefinition);
    }
    return this;
  }

  @SuppressWarnings("unchecked")
  public Query setRecordFactory(final RecordFactory<?> recordFactory) {
    this.recordFactory = (RecordFactory<Record>)recordFactory;
    return this;
  }

  public Query setReturnCount(final boolean returnCount) {
    this.returnCount = returnCount;
    return this;
  }

  public Query setSelect(final Collection<?> selectExpressions) {
    this.selectExpressions.clear();
    for (final Object selectExpression : selectExpressions) {
      select(selectExpression);
    }
    return this;
  }

  public Query setSelect(final Object... selectExpressions) {
    this.selectExpressions.clear();
    for (final Object selectExpression : selectExpressions) {
      select(selectExpression);
    }
    return this;
  }

  public Query setSelect(final TableReferenceProxy table, final String... fieldNames) {
    this.selectExpressions.clear();
    return select(table, fieldNames);
  }

  public Query setSql(final String sql) {
    this.sql = sql;
    return this;
  }

  public Query setStatistics(final LabelCounters labelCountMap) {
    this.labelCountMap = labelCountMap;
    return this;
  }

  public Query setWhere(final String where) {
    final RecordDefinition recordDefinition = getRecordDefinition();
    final Condition whereCondition = QueryValue.parseWhere(recordDefinition, where);
    return setWhereCondition(whereCondition);
  }

  public Query setWhereCondition(final Condition whereCondition) {
    if (whereCondition == null || whereCondition instanceof NoCondition) {
      this.whereCondition = Condition.ALL;
    } else {
      this.whereCondition = whereCondition;
      final RecordDefinition recordDefinition = getRecordDefinition();
      if (recordDefinition != null) {
        whereCondition.changeRecordDefinition(recordDefinition, recordDefinition);
      }
    }
    return this;
  }

  public <R extends Record> Single<R> singleRecord() {
    return Single.ofNullable(getRecord());
  }

  public <V extends Record> void sort(final List<V> records) {
    final List<OrderBy> orderBy = getOrderBy();
    if (!orderBy.isEmpty()) {
      final Comparator<Record> comparator = Records.newComparatorOrderBy(orderBy);
      records.sort(comparator);
    }
  }

  @Override
  public String toString() {
    final StringBuilder string = new StringBuilder();
    try {
      if (this.sql == null) {
        final String sql = getSelectSql(false);
        string.append(sql);

        if (this.offset > 0) {
          string.append("\n OFFSET " + this.offset);
        }
        if (this.limit != Integer.MAX_VALUE) {
          string.append("\n LIMIT " + this.limit);
        }
      } else {
        string.append(this.sql);
      }

      if (!this.parameters.isEmpty()) {
        string.append(" ");
        string.append(this.parameters);
      }
    } catch (final Throwable t) {
      Logs.error(this, t);
    }
    return string.toString();
  }

  public Query union(final Query query, final boolean distinct) {
    this.union = new Union(query, distinct);
    return this;
  }

  public Query unionAll(final Query query) {
    if (query == this) {
      throw new IllegalArgumentException("Cannot union query to itself");
    }
    this.union = new Union(query, false);
    return this;
  }

  public Record updateRecord(final Consumer<Record> updateAction) {
    final Record record = getRecord();
    if (record == null) {
      return null;
    } else {
      updateAction.accept(record);
      getRecordStore().updateRecord(record);
      return record;
    }
  }

  public int updateRecords(final Consumer<? super ChangeTrackRecord> updateAction) {
    final RecordDefinition recordDefinition = getRecordDefinition();
    final RecordStore recordStore = recordDefinition.getRecordStore();
    return recordStore.transactionCall(() -> {
      int i = 0;
      setRecordFactory(ArrayChangeTrackRecord.FACTORY);
      try (
        final RecordReader reader = getRecordReader();
        final RecordWriter writer = recordStore.newRecordWriter(recordDefinition)) {
        for (final Record record : reader) {
          final ChangeTrackRecord changeTrackRecord = (ChangeTrackRecord)record;
          updateAction.accept(changeTrackRecord);
          if (changeTrackRecord.isModified()) {
            writer.write(changeTrackRecord);
            i++;
          }
        }
      }
      return i;
    });
  }

  public Query where(final BiConsumer<Query, WhereConditionBuilder> action) {
    final WhereConditionBuilder builder = new WhereConditionBuilder(getTableReference(),
      this.whereCondition);
    this.whereCondition = builder.build(this, action);
    return this;
  }

  public Query where(final Consumer<WhereConditionBuilder> action) {
    final WhereConditionBuilder builder = new WhereConditionBuilder(getTableReference(),
      this.whereCondition);
    this.whereCondition = builder.build(action);
    return this;
  }

  public Query with(final BiConsumer<Query, WithQuery> queryBuilder) {
    final var with = new WithQuery();
    this.withQueries.add(with);
    queryBuilder.accept(this, with);
    return this;
  }
}
