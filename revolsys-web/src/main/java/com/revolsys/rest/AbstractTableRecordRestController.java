package com.revolsys.rest;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.record.schema.AbstractTableRecordStore;
import com.revolsys.record.schema.TableRecordStoreConnection;
import com.revolsys.record.schema.TableRecordStoreFactory;
import com.revolsys.record.schema.TableRecordStoreQuery;

public class AbstractTableRecordRestController {

  protected int maxPageSize = Integer.MAX_VALUE;

  public AbstractTableRecordRestController() {
  }

  protected <RS extends AbstractTableRecordStore> RS getTableRecordStore(
    final TableRecordStoreFactory connection, final CharSequence tablePath) {
    final RS tableRecordStore = connection.getTableRecordStore(tablePath);
    if (tableRecordStore == null) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    }
    return tableRecordStore;
  }

  protected boolean isUpdateable(final TableRecordStoreConnection connection, final Identifier id) {
    return true;
  }

  protected TableRecordStoreQuery newQuery(final TableRecordStoreConnection connection,
    final HttpServletRequest request, final CharSequence tablePath) {
    final AbstractTableRecordStore recordStore = getTableRecordStore(connection, tablePath);
    return recordStore.newQuery(connection, request, Integer.MAX_VALUE);
  }

}
