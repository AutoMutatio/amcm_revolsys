package com.revolsys.odata.service.processor;

import java.net.URI;
import java.util.Locale;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.ContextURL.Builder;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmNavigationProperty;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.commons.core.edm.EdmBindingTarget;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.Processor;

import com.revolsys.odata.model.ODataEdmProvider;
import com.revolsys.odata.model.ODataEntityType;

public abstract class AbstractProcessor implements Processor {

  protected ServiceMetadata serviceMetadata;

  protected final ODataEdmProvider provider;

  public AbstractProcessor(final ODataEdmProvider provider) {
    this.provider = provider;
  }

  public ODataEntityType getEntityType(final EdmEntitySet edmEntitySet)
    throws ODataApplicationException {
    final EdmEntityType edmEntityType = edmEntitySet.getEntityType();
    final var entityTypeName = edmEntityType.getPathName();
    final ODataEntityType entityType = (ODataEntityType)this.provider.getEntityType(entityTypeName);
    if (entityType == null) {
      throw new ODataApplicationException(
        "Entity type " + entityTypeName + " for requested key doesn't exist",
        HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
    }
    return entityType;
  }

  public EdmEntitySet getNavigationTargetEntitySet(final EdmEntitySet startEdmEntitySet,
    final EdmNavigationProperty edmNavigationProperty) throws ODataApplicationException {
    final String navigationPropertyName = edmNavigationProperty.getName();
    final EdmBindingTarget edmBindingTarget = startEdmEntitySet
      .getRelatedBindingTarget(navigationPropertyName);
    if (edmBindingTarget == null) {
      throw new ODataApplicationException("Not supported.",
        HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    } else if (edmBindingTarget instanceof EdmEntitySet) {
      return (EdmEntitySet)edmBindingTarget;
    } else {
      throw new ODataApplicationException("Not supported.",
        HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    }

  }

  public ODataEdmProvider getProvider() {
    return this.provider;
  }

  @Override
  public void init(final ServiceMetadata serviceMetadata) {
    this.serviceMetadata = serviceMetadata;
  }

  protected Builder newContextUrl(final ODataRequest request) {
    return ContextURL//
      .with()
      .serviceRoot(request.<URI> getAttribute("serviceRootUri"));
  }

}
