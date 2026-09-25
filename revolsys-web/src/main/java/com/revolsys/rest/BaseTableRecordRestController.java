package com.revolsys.rest;

import java.util.UUID;

import jakarta.servlet.http.HttpServletRequest;

import com.revolsys.io.PathName;
import com.revolsys.record.schema.AbstractTableRecordStore;
import com.revolsys.record.schema.TableRecordStoreConnection;
import com.revolsys.record.schema.TableRecordStoreFactory;
import com.revolsys.record.schema.TableRecordStoreQuery;

public class BaseTableRecordRestController extends AbstractTableRecordRestController {

  public static UUID getUuid(final String referenceOrId) {
    UUID uuid = null;
    if (referenceOrId.length() == 36) {
      try {
        uuid = UUID.fromString(referenceOrId);
      } catch (final Exception e) {
      }
    }
    return uuid;
  }

  protected final PathName tablePath;

  protected final String tableName;

  public BaseTableRecordRestController(final PathName tablePath) {
    this.tablePath = tablePath;
    this.tableName = tablePath.getName();
  }

  protected <RS extends AbstractTableRecordStore> RS getTableRecordStore(
    final TableRecordStoreFactory connection) {
    return super.getTableRecordStore(connection, this.tablePath);
  }

  protected TableRecordStoreQuery newQuery(final TableRecordStoreConnection connection) {
    return getTableRecordStore(connection, this.tableName).newQuery(connection);
  }

  protected TableRecordStoreQuery newQuery(final TableRecordStoreConnection connection,
    final HttpServletRequest request) {
    return super.newQuery(connection, request, this.tablePath);
  }

}
