package com.revolsys.odata.service.processor;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerStreamResult;
import org.apache.olingo.server.api.uri.UriHelper;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.core.uri.UriResource;
import org.apache.olingo.server.core.uri.UriResourceEntitySet;
import org.apache.olingo.server.core.uri.UriResourceFunction;
import org.apache.olingo.server.core.uri.UriResourceNavigationProperty;

import com.revolsys.collection.map.Maps;
import com.revolsys.logging.Logs;
import com.revolsys.odata.model.ODataEdmProvider;
import com.revolsys.odata.model.ODataEntityIterator;
import com.revolsys.odata.model.ODataEntityIterator.Options;

public class ODataEntityCollectionProcessor extends AbstractProcessor
  implements EntityCollectionProcessor {

  public interface Handler {
    void accept(ODataEntityCollectionProcessor processor, ODataRequest request,
      ODataResponse response, UriInfo info, UriResourceFunction function,
      ContentType responseFormat);
  }

  private final Map<ContentType, ODataSerializer> serializerByContentType = Maps
    .lazy(ODataSerializer::createSerializer);

  public ODataEntityCollectionProcessor(final ODataEdmProvider provider) {
    super(provider);
  }

  ODataSerializer getSerializer(final ContentType contentType) throws SerializerException {
    return this.serializerByContentType.get(contentType);
  }

  @Override
  public void readEntityCollection(final ODataRequest request, final ODataResponse response,
    final UriInfo uriInfo, final ContentType responseFormat)
    throws ODataApplicationException, ODataLibraryException {
    ODataEntityIterator entityIterator = null;

    final List<UriResource> resourceParts = uriInfo.getUriResourceParts();
    final int segmentCount = resourceParts.size();

    final UriResource uriResource = resourceParts.get(0);
    if (uriResource instanceof final UriResourceEntitySet uriResourceEntitySet) {
      final EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();
      if (segmentCount == 1) {
        entityIterator = edmEntitySet.getEntityType()
          .readEntityIterator(request, uriInfo, new Options());
      } else if (segmentCount == 2) {
        final UriResource lastSegment = resourceParts.get(1);
        if (lastSegment instanceof UriResourceNavigationProperty) {
          // final UriResourceNavigationProperty uriResourceNavigation =
          // (UriResourceNavigationProperty)lastSegment;
          // final EdmNavigationProperty edmNavigationProperty =
          // uriResourceNavigation.getProperty();
          // final EdmEntityType targetEntityType =
          // edmNavigationProperty.getType();
          // responseEdmEntitySet =
          // Util.getNavigationTargetEntitySet(edmEntitySet,
          // edmNavigationProperty);
          //
          // // 2nd: fetch the data from backend
          // // first fetch the entity where the first segment of the URI points
          // to
          // // e.g. Categories(3)/Products first find the single entity:
          // Category(3)
          // List<UriParameter> keyPredicates =
          // uriResourceEntitySet.getKeyPredicates();
          // Entity sourceEntity = storage.readEntityData(edmEntitySet,
          // keyPredicates);
          // // error handling for e.g. DemoService.svc/Categories(99)/Products
          // if(sourceEntity == null) {
          // throw new ODataApplicationException("Entity not found.",
          // HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT);
          // }
          // // then fetch the entity collection where the entity navigates to
          // entityCollection = storage.getRelatedEntityCollection(sourceEntity,
          // targetEntityType);
        }
      } else { // this would be the case for e.g. Products(1)/Category/Products
        throw new ODataApplicationException("Not supported",
          HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
      }
      serializeEntitySet(request, response, uriInfo, responseFormat, entityIterator);

    } else if (uriResource instanceof final UriResourceFunction uriFunction) {
      final var function = uriFunction.getFunction();
      final var handler = getProvider().getFunctionEntitySetHandler(function.getFunction());
      if (handler == null) {
        throw new ODataApplicationException("Function not found",
          HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT);
      } else {
        handler.accept(this, request, response, uriInfo, uriFunction, responseFormat);
      }
    } else {
      throw new ODataApplicationException("Only EntitySet is supported",
        HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    }
  }

  public void serializeEntitySet(final ODataRequest request, final ODataResponse response,
    final UriInfo uriInfo, final ContentType responseFormat, final ODataEntityIterator iterator) {
    final ODataSerializer serializer = getSerializer(responseFormat);

    final EdmEntityType entityType = iterator.getEdmEntityType();
    String selectList = null;
    final SelectOption selectOption = uriInfo.getSelectOption();
    if (selectOption != null) {
      selectList = UriHelper.buildContextURLSelectList(entityType, null, selectOption);
    }

    final ContextURL contextUrl = newContextUrl().selectList(selectList)
      .entitySetOrSingletonOrType(entityType.getName())
      .build();

    final String id = request.getRawBaseUri() + "/" + entityType.getName();
    final EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions//
      .with()
      .id(id)
      .count(uriInfo.getCountOption())
      .contextURL(contextUrl)
      .select(selectOption)
      .writeContentErrorCallback((context, channel) -> {
        final String message = request.getRawRequestUri();
        final Exception exception = context.getException();
        Throwable cause = exception.getCause();
        while (cause != null) {
          if (cause.getMessage()
            .equals("Broken pipe")) {
            return;
          }
          cause = cause.getCause();
        }
        Logs.error(this, message, exception);
      })
      .build();

    if (iterator != null) {
      final SerializerStreamResult serializerResult = serializer
        .entityCollectionStreamed(this.serviceMetadata, entityType, iterator, opts);

      final ODataEntityInteratorDataContent dataContent = new ODataEntityInteratorDataContent(
        serializerResult, iterator);
      response.setODataContent(dataContent);
    }
    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
  }
}
