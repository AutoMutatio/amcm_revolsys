package com.revolsys.rest;

import java.io.IOException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;

import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.TableRecordStoreConnection;

public class BaseTableRest extends AbstractTableRecordRestController {

  public BaseTableRest() {
    super();
  }

  @GetMapping("/app/api/{tableName}({id:[0-9]+})")
  public ResponseEntity<Record> getRecordIntegral(
    @RequestAttribute("tableConnection") final TableRecordStoreConnection connection,
    @PathVariable final String tableName, @PathVariable final String id) {
    return getRecordString(connection, tableName, id);
  }

  @GetMapping("/app/api/{tableName}('{id}')")
  public ResponseEntity<Record> getRecordString(
    @RequestAttribute("tableConnection") final TableRecordStoreConnection connection,
    @PathVariable final String tableName, @PathVariable final String id) {
    final var record = getTableRecordStore(connection, tableName).newQuery(connection)//
      .andEqualId(id)
      .getRecord();
    return ResponseEntity.ofNullable(record);
  }

  @GetMapping(path = "/app/api/{tableName:[A-Za-z0-9_\\\\.]+}")
  public Query listRecords(
    @RequestAttribute("tableConnection") final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response,
    @PathVariable final String tableName) {
    return newQuery(connection, request, tableName);
  }

  @PostMapping(path = "/app/api/{tableName:[A-Za-z0-9_\\\\.]+}", consumes = {
    MediaType.APPLICATION_FORM_URLENCODED_VALUE
  })
  public void listRecordsPost(
    @RequestAttribute("tableConnection") final TableRecordStoreConnection connection,
    final HttpServletRequest request, final HttpServletResponse response,
    @PathVariable final String tableName) throws IOException {
    listRecords(connection, request, response, tableName);
  }

}
