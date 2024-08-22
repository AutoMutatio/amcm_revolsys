package com.revolsys.odata.model;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.olingo.commons.api.data.AbstractEntityCollection;
import org.apache.olingo.commons.api.data.ODataEntity;
import org.apache.olingo.commons.api.data.Operation;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.ex.ODataNotSupportedException;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.CountOption;

import com.revolsys.collection.iterator.BaseIterable;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.transaction.TransactionBuilder;
import com.revolsys.transaction.Transactionable;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.UriBuilder;

public class ODataEntityIterator extends AbstractEntityCollection
  implements BaseCloseable, Iterator<ODataEntity>, Transactionable {

  public static class Options {
    private boolean useMaxLimit = true;

    public boolean isUseMaxLimit() {
      return this.useMaxLimit;
    }

    public Options useMaxLimit(final boolean useMaxLimit) {
      this.useMaxLimit = useMaxLimit;
      return this;
    }
  }

  private BaseIterable<Record> reader;

  private Iterator<Record> iterator;

  private final EdmEntityType entityType;

  private final int skip;

  private int readCount;

  private final int limit;

  private final ODataRequest request;

  private final UriInfo uriInfo;

  private final Query query;

  private boolean countLoaded;

  private Integer count;

  private EdmEntityType resultEntityType;

  public ODataEntityIterator(final EdmEntityType entityType, final ODataRequest request,
    final UriInfo uriInfo, final Options options) throws ODataApplicationException {
    this.request = request;
    this.uriInfo = uriInfo;
    this.entityType = entityType;
    this.resultEntityType = entityType;
    final Query query = entityType.newQuery(request, uriInfo, options);

    final CountOption countOption = this.uriInfo.getCountOption();
    if (countOption == null) {
      this.countLoaded = true;
    } else {
      this.countLoaded = !countOption.getValue();
    }
    this.query = query;
    final ODataEntityType odataEntityType = entityType.getEntityType();
    odataEntityType.addLimits(this.query, this.uriInfo, options);
    this.skip = query.getOffset();
    this.limit = query.getLimit();
  }

  @Override
  public void close() {
    BaseCloseable.closeValueSilent(this.reader);
  }

  public ODataEntityIterator count(final boolean count) {
    this.countLoaded = !count;
    return this;
  }

  @Override
  public Integer getCount() {
    if (!this.countLoaded) {
      this.countLoaded = true;
      this.request.getConnection()
        .transactionRun(() -> {
          final RecordStore recordStore = this.entityType.getRecordStore();
          final Integer count = recordStore.getRecordCount(this.query.clone());
          this.count = count;
        });
    }
    return this.count;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * <b>ATTENTION:</b> <code>getDeltaLink</code> is not supported by default.
   */
  @Override
  public URI getDeltaLink() {
    throw new ODataNotSupportedException("Entity Iterator does not support getDeltaLink()");
  }

  public EdmEntityType getEdmEntityType() {
    return this.resultEntityType;
  }

  private Iterator<Record> getIterator() {
    if (this.reader == null) {
      this.reader = this.request.getConnection()
        .transactionCall(() -> this.entityType.getRecordStore()
          .getRecords(this.query));
      if (this.resultEntityType != this.entityType) {
        this.reader = this.reader.map(r -> this.resultEntityType.newRecord(r));
      }
      this.iterator = this.reader.iterator();
    }
    return this.iterator;
  }

  @Override
  public URI getNext() {
    final Integer count = getCount();
    final int totalRead = this.skip + this.readCount;
    if (count == null) {
      if (this.readCount < this.limit) {
        return null;
      }
    } else {
      if (totalRead >= count) {
        return null;
      }
    }

    final String uri = this.request.getRawRequestUri();
    return new UriBuilder(uri).setParameter("$skip", totalRead)
      .build();
  }

  /**
   * {@inheritDoc}
   * <p/>
   * <b>ATTENTION:</b> <code>getOperations</code> is not supported by default.
   */
  @Override
  public List<Operation> getOperations() {
    // "Remove is not supported for iteration over Entities."
    throw new ODataNotSupportedException(
      "Entity Iterator does not support getOperations() by default");
  }

  @Override
  public boolean hasNext() {
    final Iterator<Record> iterator = getIterator();
    final boolean hasNext = iterator.hasNext();
    if (!hasNext) {
      close();
    }
    return hasNext;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<ODataEntity> iterator() {
    return this;
  }

  @Override
  public ODataEntity next() {
    final Iterator<Record> iterator = getIterator();
    final Record record = iterator.next();
    this.readCount++;
    return this.resultEntityType.newEntity(record);
  }

  public ODataEntityIterator query(final Consumer<Query> query) {
    query.accept(this.query);
    return this;
  }

  public Iterable<Record> records() {
    return this::getIterator;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * <b>ATTENTION:</b> <code>remove</code> is not supported by default.
   */
  @Override
  public void remove() {
    // "Remove is not supported for iteration over Entities."
    throw new ODataNotSupportedException("Entity Iterator does not support remove()");
  }

  public ODataEntityIterator setResultEntityType(final EdmEntityType resultEntityType) {
    this.resultEntityType = resultEntityType;
    return this;
  }

  @Override
  public String toString() {
    return this.query.toString();
  }

  @Override
  public TransactionBuilder transaction() {
    return this.query.transaction();
  }
}
