package com.revolsys.record.io.format.odata;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jeometry.common.json.JsonObject;
import org.jeometry.common.util.Cancellable;

import com.revolsys.http.HttpRequestBuilder;
import com.revolsys.record.query.ColumnReference;
import com.revolsys.record.query.OrderBy;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;

public class ODataQuery extends Query {

  private final ODataResource resource;

  private int pageLimit = Integer.MAX_VALUE;

  private boolean count;

  private String format;

  public ODataQuery(final ODataResource resource) {
    this.resource = resource;
  }

  private void addFilterTo(final HttpRequestBuilder request) {
    final QueryValue condition = getWhereCondition();
    if (condition != null) {
      final StringBuilder filter = new StringBuilder();
      ODataRecordStore.appendQueryValue(filter, condition);
      if (filter.length() > 0) {
        request.setParameter("$filter", filter);
      }
    }
  }

  private void addOrderByTo(final HttpRequestBuilder request) {
    final List<OrderBy> orderBys = getOrderBy();
    if (!orderBys.isEmpty()) {
      final StringBuilder order = new StringBuilder();
      boolean first = true;
      for (final OrderBy orderBy : orderBys) {
        if (first) {
          first = false;
        } else {
          order.append(", ");
        }
        final QueryValue orderField = orderBy.getField();
        if (orderField instanceof ColumnReference) {
          final ColumnReference column = (ColumnReference)orderField;
          order.append(column.getName());
        }
        if (!orderBy.isAscending()) {
          order.append(" desc");
        }
      }
      request.setParameter("$orderby", order);
    }
  }

  private void addSelectTo(final HttpRequestBuilder request) {
    final List<QueryValue> selectValues = getSelect();
    if (!selectValues.isEmpty()) {
      final StringBuilder select = new StringBuilder();
      for (final QueryValue selectItem : selectValues) {
        if (selectItem instanceof ColumnReference) {
          final ColumnReference column = (ColumnReference)selectItem;
          if (select.length() > 0) {
            select.append(',');
          }
          select.append(column.getName());
        } else {
          throw new IllegalArgumentException("Not supported:" + selectItem);
        }
      }
      request.setParameter("$select", select);
    }
  }

  public int forEach(final Cancellable cancellable, final Consumer<JsonObject> action) {
    throw new UnsupportedOperationException();
  }

  public <V> V getFirst(final Function<JsonObject, V> converter) {
    throw new UnsupportedOperationException();
  }

  private ODataRequestBuilder initRequest() {
    return this.resource.get().withBuilder(request -> {
      if (this.format != null) {
        request.setParameter(ODataRecordStore.FORMAT_JSON);
      }
      addSelectTo(request);
      addFilterTo(request);
      addOrderByTo(request);
      if (this.count) {
        request.setParameter("$count", true);
      }
      final int offset = getOffset();
      if (offset > 0) {
        request.setParameter("$skip", offset);
      }
      final int limit = getLimit();
      if (limit > 0 && limit < Integer.MAX_VALUE) {
        request.setParameter("$top", limit);
      }
    });
  }

  public <V> ODataJsonQueryIterator<V> iterator(final Function<JsonObject, V> converter) {
    return new ODataJsonQueryIterator<>(this.resource.getFactory(), initRequest().request(),
      converter, toString(), this.pageLimit);
  }

  public ODataQuery setCount(final boolean count) {
    this.count = count;
    return this;
  }

  public ODataQuery setFormat(final String format) {
    this.format = format;
    return this;
  }

  public void setPageLimit(final int pageLimit) {
    this.pageLimit = Math.max(0, pageLimit);
  }

}
