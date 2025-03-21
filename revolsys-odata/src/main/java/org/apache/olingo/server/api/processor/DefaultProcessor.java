/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.olingo.server.api.processor;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpMethod;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ODataServerError;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.etag.ETagHelper;
import org.apache.olingo.server.api.etag.ServiceMetadataETagSupport;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.core.etag.ETagHelperImpl;

/**
 * <p>Processor implementation for handling default cases:
 * <ul><li>request for the metadata document</li>
 * <li>request for the service document</li>
 * <li>error handling</li></ul></p>
 * <p>This implementation is registered in the ODataHandler by default.
 * The default can be replaced by re-registering a custom implementation.</p>
 */
public class DefaultProcessor
  implements MetadataProcessor, ServiceDocumentProcessor, ErrorProcessor {

  private ServiceMetadata serviceMetadata;

  @Override
  public void init(final ServiceMetadata serviceMetadata) {
    this.serviceMetadata = serviceMetadata;
  }

  @Override
  public void processError(final ODataRequest request, final ODataResponse response,
    final ODataServerError serverError, final ContentType requestedContentType) {
    try {
      final ODataSerializer serializer = ODataSerializer.createSerializer(requestedContentType);
      response.setContent(serializer.error(serverError)
        .getContent());
      response.setStatusCode(serverError.getStatusCode());
      response.setHeader(HttpHeader.CONTENT_TYPE, requestedContentType.toContentTypeString());
    } catch (final Exception e) {
      // This should never happen but to be sure we have this catch here to
      // prevent sending a stacktrace to a client.
      final String responseContent = "{\"error\":{\"code\":null,\"message\":\"An unexpected exception occurred during error processing\"}}";
      response
        .setContent(new ByteArrayInputStream(responseContent.getBytes(Charset.forName("utf-8"))));
      response.setStatusCode(HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      response.setHeader(HttpHeader.CONTENT_TYPE,
        ContentType.APPLICATION_JSON.toContentTypeString());
    }
  }

  @Override
  public void readMetadata(final ODataRequest request, final ODataResponse response,
    final UriInfo uriInfo, final ContentType requestedContentType)
    throws ODataApplicationException, ODataLibraryException {
    boolean isNotModified = false;
    final ServiceMetadataETagSupport eTagSupport = this.serviceMetadata
      .getServiceMetadataETagSupport();
    if (eTagSupport != null && eTagSupport.getMetadataETag() != null) {
      // Set application etag at response
      response.setHeader(HttpHeader.ETAG, eTagSupport.getMetadataETag());
      // Check if metadata document has been modified
      final ETagHelper eTagHelper = new ETagHelperImpl();
      isNotModified = eTagHelper.checkReadPreconditions(eTagSupport.getMetadataETag(),
        request.getHeaders(HttpHeader.IF_MATCH), request.getHeaders(HttpHeader.IF_NONE_MATCH));
    }

    // Send the correct response
    if (isNotModified) {
      response.setStatusCode(HttpStatusCode.NOT_MODIFIED.getStatusCode());
    } else {
      // HTTP HEAD requires no payload but a 200 OK response
      if (HttpMethod.HEAD == request.getMethod()) {
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
      } else {
        final ODataSerializer serializer = ODataSerializer.createSerializer(requestedContentType);
        response.setContent(serializer.metadataDocument(this.serviceMetadata)
          .getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, requestedContentType.toContentTypeString());
      }
    }
  }

  @Override
  public void readServiceDocument(final ODataRequest request, final ODataResponse response,
    final UriInfo uriInfo, final ContentType requestedContentType)
    throws ODataApplicationException, ODataLibraryException {
    boolean isNotModified = false;
    final ServiceMetadataETagSupport eTagSupport = this.serviceMetadata
      .getServiceMetadataETagSupport();
    if (eTagSupport != null && eTagSupport.getServiceDocumentETag() != null) {
      // Set application etag at response
      response.setHeader(HttpHeader.ETAG, eTagSupport.getServiceDocumentETag());
      // Check if service document has been modified
      final ETagHelper eTagHelper = new ETagHelperImpl();
      isNotModified = eTagHelper.checkReadPreconditions(eTagSupport.getServiceDocumentETag(),
        request.getHeaders(HttpHeader.IF_MATCH), request.getHeaders(HttpHeader.IF_NONE_MATCH));
    }

    // Send the correct response
    if (isNotModified) {
      response.setStatusCode(HttpStatusCode.NOT_MODIFIED.getStatusCode());
    } else {
      // HTTP HEAD requires no payload but a 200 OK response
      if (HttpMethod.HEAD == request.getMethod()) {
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
      } else {
        final ODataSerializer serializer = ODataSerializer.createSerializer(requestedContentType);
        response.setContent(serializer.serviceDocument(this.serviceMetadata, null)
          .getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, requestedContentType.toContentTypeString());
      }
    }
  }
}
