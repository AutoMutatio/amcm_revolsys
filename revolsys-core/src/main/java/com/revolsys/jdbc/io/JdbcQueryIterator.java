package com.revolsys.jdbc.io;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.springframework.dao.DataAccessException;

import com.revolsys.collection.iterator.AbstractIterator;
import com.revolsys.exception.Exceptions;
import com.revolsys.io.PathName;
import com.revolsys.jdbc.JdbcConnection;
import com.revolsys.jdbc.JdbcUtils;
import com.revolsys.record.Record;
import com.revolsys.record.RecordFactory;
import com.revolsys.record.io.RecordIterator;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.BaseCloseable;

public class JdbcQueryIterator extends AbstractIterator<Record>
  implements RecordReader, RecordIterator {
  private JdbcConnection connection;

  private List<QueryValue> selectExpressions = new ArrayList<>();

  private Query query;

  private JdbcRecordDefinition recordDefinition;

  private RecordFactory<Record> recordFactory;

  private AbstractJdbcRecordStore recordStore;

  private ResultSet resultSet;

  private PreparedStatement statement;

  public JdbcQueryIterator(final AbstractJdbcRecordStore recordStore, final Query query) {
    Transaction.assertInTransaction();

    this.recordFactory = query.getRecordFactory();
    if (this.recordFactory == null) {
      this.recordFactory = recordStore.getRecordFactory();
    }
    this.recordStore = recordStore;
    this.query = query;
  }

  @Override
  public synchronized void closeDo() {
    JdbcUtils.close(this.statement, this.resultSet);
    BaseCloseable.closeSilent(this.connection);
    this.selectExpressions = null;
    this.connection = null;
    this.recordFactory = null;
    this.recordStore = null;
    this.recordDefinition = null;
    this.query = null;
    this.resultSet = null;
    this.statement = null;
  }

  @Override
  protected Record getNext() throws NoSuchElementException {
    try {
      if (this.resultSet != null && !this.query.isCancelled() && this.resultSet.next()) {
        final Record record = this.recordStore.getNextRecord(this.recordDefinition,
          this.selectExpressions, this.recordFactory, this.resultSet);
        return record;
      } else {
        close();
        throw new NoSuchElementException();
      }
    } catch (final SQLException e) {
      final boolean cancelled = this.query.isCancelled();
      DataAccessException e2;
      if (cancelled) {
        e2 = null;
      } else {
        final String sql = this.query.getSql();
        e2 = this.recordStore.getException("Get Next", sql, e);
      }
      close();
      if (cancelled) {
        throw new NoSuchElementException();
      } else if (e2 == null) {
        throw Exceptions.toRuntimeException(e);
      } else {
        throw e2;
      }
    } catch (final RuntimeException e) {
      close();
      throw e;
    } catch (final Error e) {
      close();
      throw e;
    }
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    if (this.recordDefinition == null) {
      hasNext();
    }
    return this.recordDefinition;
  }

  @Override
  public JdbcRecordStore getRecordStore() {
    return this.recordStore;
  }

  protected ResultSet getResultSet() {
    final Query query = this.query;
    final PathName tableName = query.getTablePath();
    final RecordDefinition queryRecordDefinition = query.getRecordDefinition();
    if (queryRecordDefinition != null) {
      this.recordDefinition = this.recordStore.getRecordDefinition(queryRecordDefinition);
      if (this.recordDefinition != null) {
        query.setRecordDefinition(this.recordDefinition);
      }
    }
    if (this.recordDefinition == null) {
      if (tableName != null) {
        this.recordDefinition = this.recordStore.getRecordDefinition(tableName);
        if (this.recordDefinition != null) {
          query.setRecordDefinition(this.recordDefinition);
        }
      }
    }
    String dbTableName;
    if (this.recordDefinition == null) {
      final PathName pathName = PathName.newPathName(tableName);
      if (pathName == null) {
        dbTableName = null;
      } else {
        dbTableName = pathName.getName();
      }
    } else {
      dbTableName = this.recordDefinition.getDbTableName();
    }

    final String sql = this.recordStore.getSelectSql(query);
    try {
      this.statement = this.connection.prepareStatement(sql);
      this.statement.setFetchSize(this.query.fetchSize());

      this.resultSet = this.recordStore.getResultSet(this.statement, query);
      final ResultSetMetaData resultSetMetaData = this.resultSet.getMetaData();

      if (this.recordDefinition == null || !query.getJoins()
        .isEmpty() || this.recordStore != this.recordDefinition.getRecordStore()
        || query.getSql() != null) {
        this.recordDefinition = this.recordStore.getRecordDefinition(tableName, resultSetMetaData,
          dbTableName);
        query.setRecordDefinition(this.recordDefinition);
      } else if (query.isCustomResult()) {
        this.recordDefinition = this.recordStore.getRecordDefinition(query, resultSetMetaData);
      }
      this.selectExpressions = query.getSelectExpressions();
      if (this.selectExpressions.isEmpty() || query.isSelectStar()) {
        this.selectExpressions = (List)this.recordDefinition.getFieldDefinitions();
      }

    } catch (final SQLException e) {
      JdbcUtils.close(this.statement, this.resultSet);
      throw this.recordStore.getException("Execute Query", sql, e);
    }
    return this.resultSet;
  }

  protected String getSql(final Query query) {
    return query.getSelectSql();
  }

  @Override
  protected void initDo() {
    this.connection = this.recordStore.getJdbcConnection();
    this.resultSet = getResultSet();
  }

}
