package com.revolsys.record.code;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.list.Lists;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.identifier.ListIdentifier;
import com.revolsys.data.identifier.SingleIdentifier;
import com.revolsys.exception.Exceptions;
import com.revolsys.logging.Logs;
import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.record.Record;
import com.revolsys.record.query.Condition;
import com.revolsys.record.query.Q;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Property;

public abstract class AbstractRecordStoreSingleValueCodeTable<SELF extends AbstractRecordStoreSingleValueCodeTable<SELF>>
  extends AbstractSingleValueCodeTable {

  private static final String DEFAULT_FIELD_NAME = "VALUE";

  private boolean createMissingCodes = true;

  private List<String> fieldNameAliases = new ArrayList<>();

  protected String idFieldName;

  private boolean loadAll = true;

  private boolean loaded = false;

  private boolean loading = false;

  private boolean loadMissingCodes = true;

  private final ReentrantLockEx lock = new ReentrantLockEx();

  private final java.util.concurrent.locks.Condition lockCondition = this.lock.newCondition();

  protected String orderBy = DEFAULT_FIELD_NAME;

  private final ThreadLocal<Boolean> threadLoading = new ThreadLocal<>();

  protected String valueFieldName = DEFAULT_FIELD_NAME;

  private boolean allowNullValues = false;

  private Condition filter;

  public void addFieldAlias(final String columnName) {
    this.fieldNameAliases.add(columnName);
  }

  @Override
  public final void addValue(final Record code) {
    addValueDo(code);
    clearCaches();
  }

  protected void addValueDo(final Record code) {
    final String idFieldName = getIdFieldName();
    final Identifier id = code.getIdentifier(idFieldName);
    if (id == null) {
      // TODO throw new NullPointerException(idFieldName + "=null for " + code);
    } else {
      Object value = code.getValue(this.valueFieldName);
      if (value instanceof SingleIdentifier) {
        final SingleIdentifier identifier = (SingleIdentifier)value;
        value = identifier.getValue(0);
      }
      if (value == null) {
        if (!this.allowNullValues) {
          throw new NullPointerException(this.valueFieldName + "=null for " + code);
        }
      }
      addValue(id, value);
    }
  }

  @Override
  protected abstract int calculateValueFieldLength();

  protected void clearCaches() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public SELF clone() {
    final var clone = (AbstractRecordStoreSingleValueCodeTable<SELF>)super.clone();
    clone.fieldNameAliases = new ArrayList<>(this.fieldNameAliases);
    return (SELF)clone;
  }

  protected abstract QueryValue fieldPathToSelect(Query query, String idFieldName);

  public Condition filter() {
    return this.filter;
  }

  public SELF filter(final Condition filter) {
    this.filter = filter;
    return self();
  }

  public void forEachRecord(final Consumer<Record> action) {
    final var query = newQuery();
    if (query != null) {
      final var idSelect = fieldPathToSelect(query, this.idFieldName);
      final var valueSelect = fieldPathToSelect(query, this.valueFieldName);
      query.select(idSelect, valueSelect)
        .addOrderBy(this.orderBy)
        .forEachRecord(action);
    }
  }

  @SuppressWarnings("unchecked")
  public <C extends CodeTable> C getCodeTable() {
    return (C)this;
  }

  @Override
  public List<String> getFieldNameAliases() {
    return this.fieldNameAliases;
  }

  @Override
  public String getIdFieldName() {
    return this.idFieldName;
  }

  @Override
  public JsonObject getMap(final Identifier id) {
    final Object value = getValue(id);
    if (value == null) {
      return JsonObject.hash();
    } else {
      return JsonObject.hash(this.valueFieldName, value);
    }
  }

  @SuppressWarnings({
    "rawtypes", "unchecked"
  })
  @Override
  public <V> V getValue(final Object id) {
    if (id instanceof Identifier) {
      return getValue((Identifier)id);
    } else if (id instanceof List) {
      final List list = (List)id;
      return getValue(new ListIdentifier(list));
    } else {
      return getValue(Identifier.newIdentifier(id));
    }
  }

  public String getValueFieldName() {
    return this.valueFieldName;
  }

  @Override
  public List<String> getValueFieldNames() {
    return Collections.singletonList(this.valueFieldName);
  }

  public boolean isAllowNullValues() {
    return this.allowNullValues;
  }

  public boolean isCreateMissingCodes() {
    return this.createMissingCodes;
  }

  @Override
  public boolean isLoadAll() {
    return this.loadAll;
  }

  @Override
  public boolean isLoaded() {
    return this.loaded;
  }

  @Override
  public boolean isLoading() {
    return this.loading;
  }

  @Override
  public boolean isLoadMissingCodes() {
    return this.loadMissingCodes;
  }

  @Override
  public void loadAll() {
    try (
      var _ = this.lock.lockX()) {
      if (this.threadLoading.get() != Boolean.TRUE) {
        if (this.loading) {
          while (this.loading) {
            try {
              this.lockCondition.await(1, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
              throw Exceptions.toRuntimeException(e);
            }
          }
          return;
        } else {
          this.threadLoading.set(Boolean.TRUE);
          this.loading = true;
          try {
            forEachRecord(this::addValueDo);
          } finally {
            this.loading = false;
            this.loaded = true;
            this.threadLoading.set(null);
            this.lockCondition.signalAll();
          }
          Property.firePropertyChange(this, "valuesChanged", false, true);
        }
      }
    }
  }

  @Override
  protected Identifier loadId(final Object value, final boolean createId) {
    try (
      var _ = this.lock.lockX()) {
      if (this.loadAll && !this.loadMissingCodes && !isEmpty()) {
        return null;
      }
      Identifier id = null;
      if (createId && this.loadAll && !isLoaded()) {
        loadAll();
        id = getIdentifier(value, false);
      } else {
        final Query query = newQuery().clearSelect();
        if (value == null) {
          query.and(Q.isNull(this.valueFieldName));
        } else {
          query.and(Q.or(query.newCondition(this.idFieldName, Q.EQUAL, value),
            query.newCondition(this.valueFieldName, Q.EQUAL, value)));
        }
        query.forEachRecord(this::addValueDo);

        id = getIdByValue(value);
        Property.firePropertyChange(this, "valuesChanged", false, true);
      }
      if (createId && id == null) {
        return newIdentifier(value);
      } else {
        return id;
      }
    }
  }

  @Override
  protected Object loadValues(final Object id) {
    if (this.loadAll && !isLoaded()) {
      loadAll();
    } else if (!this.loadAll || this.loadMissingCodes) {
      try {
        final Record code;
        code = recordByIdDo(id);
        if (code != null) {
          addValue(code);
        }
      } catch (final Throwable e) {
        Logs.error(this, e);
        return null;
      }
    }
    return getValueById(id);
  }

  protected Identifier newIdentifier(final Object value) {
    throw new UnsupportedOperationException();
  }

  public Query newQuery() {
    final var query = newQueryDo();
    if (query != null) {
      if (this.filter != null) {
        query.and(this.filter);
      }
    }
    return query;
  }

  protected abstract Query newQueryDo();

  protected Record recordByIdDo(final Object id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void refresh() {
    try (
      var _ = this.lock.lockX()) {
      clearCaches();
      super.refresh();
      if (isLoadAll()) {
        this.loaded = false;
        loadAll();
      }
    }
  }

  @Override
  public void refreshIfNeeded() {
    if (this.loadAll && !this.loading) {
      super.refreshIfNeeded();
    }
  }

  @SuppressWarnings("unchecked")
  public SELF self() {
    return (SELF)this;
  }

  public SELF setAllowNullValues(final boolean allowNullValues) {
    this.allowNullValues = allowNullValues;
    return self();
  }

  public SELF setCreateMissingCodes(final boolean createMissingCodes) {
    this.createMissingCodes = createMissingCodes;
    return self();
  }

  public SELF setFieldAliases(final String... fieldNameAliases) {
    setFieldNameAliases(Lists.newArray(fieldNameAliases));
    return self();
  }

  public SELF setFieldNameAliases(final List<String> fieldNameAliases) {
    this.fieldNameAliases = new ArrayList<>(fieldNameAliases);
    return self();
  }

  public SELF setIdFieldName(final String idFieldName) {
    this.idFieldName = idFieldName;
    return self();
  }

  public SELF setLoadAll(final boolean loadAll) {
    this.loadAll = loadAll;
    return self();
  }

  @Override
  public SELF setLoadMissingCodes(final boolean loadMissingCodes) {
    this.loadMissingCodes = loadMissingCodes;
    return self();
  }

  public SELF setOrderByFieldName(final String orderByFieldName) {
    this.orderBy = orderByFieldName;
    return self();
  }

  protected void setRecordDefinitionAfter(final RecordDefinition recordDefinition) {
  }

  protected void setRecordDefinitionBefore(final RecordDefinition oldRecordDefinition) {
  }

  public SELF setValueFieldName(final String valueFieldName) {
    this.valueFieldName = valueFieldName;
    if (this.orderBy == DEFAULT_FIELD_NAME) {
      setOrderByFieldName(valueFieldName);
    }
    return self();
  }

  public String toString(final List<String> values) {
    final StringBuilder string = new StringBuilder(values.get(0));
    for (int i = 1; i < values.size(); i++) {
      final String value = values.get(i);
      string.append(",");
      string.append(value);
    }
    return string.toString();
  }
}
