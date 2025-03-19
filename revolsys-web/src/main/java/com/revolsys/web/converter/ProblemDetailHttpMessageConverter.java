package com.revolsys.web.converter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.lang.Nullable;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.record.io.format.xml.XmlWriter;
import com.revolsys.util.Property;

public class ProblemDetailHttpMessageConverter extends AbstractHttpMessageConverter<ProblemDetail> {

  public ProblemDetailHttpMessageConverter() {
    super(
      StandardCharsets.UTF_8,
        MediaType.APPLICATION_JSON,
        MediaType.APPLICATION_PROBLEM_JSON,
        MediaType.TEXT_XML,
        MediaType.APPLICATION_XML,
        MediaType.APPLICATION_PROBLEM_XML);
  }

  @Override
  protected void addDefaultHeaders(final HttpHeaders headers, final ProblemDetail value,
    @Nullable final MediaType type) throws IOException {
    var contentType = type;
    if (contentType == null) {
      contentType = MediaType.APPLICATION_JSON;
    }
    contentType = new MediaType(contentType, StandardCharsets.UTF_8);
    super.addDefaultHeaders(headers, value, contentType);
  }

  @Override
  protected ProblemDetail readInternal(final Class<? extends ProblemDetail> clazz,
    final HttpInputMessage inputMessage) throws IOException {
    throw new UnsupportedOperationException("Read not supported");
  }

  @Override
  public boolean supports(final Class<?> clazz) {
    return ProblemDetail.class.isAssignableFrom(clazz);
  }

  @Override
  protected boolean supportsRepeatableWrites(final ProblemDetail value) {
    return true;
  }

  @Override
  protected void writeInternal(final ProblemDetail problem, final HttpOutputMessage outputMessage)
    throws IOException {
    if (problem == null) {
      return;
    }
    final var headers = outputMessage.getHeaders();
    final var contentType = headers.getContentType();
    try (
      var out = outputMessage.getBody();
      var writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
      if (MediaType.TEXT_XML.isCompatibleWith(contentType)
        || MediaType.APPLICATION_XML.isCompatibleWith(contentType)
        || MediaType.APPLICATION_PROBLEM_XML.isCompatibleWith(contentType)) {
        // Write XML
        try (
          var xmlOut = new XmlWriter(out)) {
          xmlOut.startDocument("UTF-8", "1.0");
          xmlOut.startTag("urn:ietf:rfc:7807", "problem");
          final var type = problem.getType();
          if (Property.hasValue(type)) {
            xmlOut.element("type", type);
          }
          final var title = problem.getTitle();
          if (Property.hasValue(title)) {
            xmlOut.element("title", title);
          }
          final var status = problem.getStatus();
          if (Property.hasValue(status)) {
            xmlOut.element("status", status);
          }
          final var detail = problem.getDetail();
          if (Property.hasValue(detail)) {
            xmlOut.element("detail", detail);
          }
          final var instance = problem.getInstance();
          if (Property.hasValue(instance)) {
            xmlOut.element("instance", instance);
          }
          xmlOut.element("timestamp", Instant.now());
          // TODO properties
          xmlOut.endTag();
        }
      } else {
        // Default to JSON
        JsonObject.hash()
          .addValue("type", problem.getType())
          .addValue("title", problem.getTitle())
          .addValue("status", problem.getStatus())
          .addValue("detail", problem.getDetail())
          .addValue("instance", problem.getInstance())
          .addValue("timestamp", Instant.now())
          .addValues(problem.getProperties())
          .removeEmptyValues()
          .appendJsonFormatted(writer);
      }
    }
  }
}
