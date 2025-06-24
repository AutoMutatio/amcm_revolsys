package com.revolsys.web.converter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.io.IoFactory;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.RecordWriterFactory;
import com.revolsys.record.io.format.csv.Csv;
import com.revolsys.record.io.format.json.JsonRecordWriter;
import com.revolsys.record.io.format.xlsx.Xlsx;
import com.revolsys.record.query.Query;
import com.revolsys.util.Property;
import com.revolsys.web.HttpServletUtils;

public class QueryHttpMessageConverter extends AbstractHttpMessageConverter<Query> {

  private static final MediaType CSV = MediaType.parseMediaType(Csv.MIME_TYPE);

  private static final MediaType XLSX = MediaType.parseMediaType(Xlsx.MEDIA_TYPE);

  public QueryHttpMessageConverter() {
    super(StandardCharsets.UTF_8, MediaType.APPLICATION_JSON, CSV, XLSX);
    // when adding more types in the implementation of
    // org.springframework.web.servlet.config.annotation.WebMvcConfigurer
    // register the file etensions
  }

  @Override
  protected boolean canRead(final MediaType mediaType) {
    return false;
  }

  @Override
  protected Query readInternal(final Class<? extends Query> clazz,
    final HttpInputMessage inputMessage) throws IOException {
    throw new UnsupportedOperationException("Cannot read query objects");
  }

  @Override
  public boolean supports(final Class<?> clazz) {
    return Query.class.isAssignableFrom(clazz);
  }

  @Override
  protected void writeInternal(final Query query, final HttpOutputMessage outputMessage)
    throws IOException {
    query.transaction()
      .requiresNew()
      .readOnly()
      .run(() -> {
        try (
          final var reader = query.getRecordReader()) {
          reader.open();
          final var headers = outputMessage.getHeaders();
          final var contentType = headers.getContentType();
          if (MediaType.APPLICATION_JSON.equalsTypeAndSubtype(contentType)) {
            writeJson(outputMessage, query, reader);
          } else {
            final var request = ((ServletRequestAttributes)RequestContextHolder
              .currentRequestAttributes()).getRequest();

            final var simpleMediaType = contentType.getType() + "/" + contentType.getSubtype();
            final var writerFactory = IoFactory.factoryByMediaType(RecordWriterFactory.class,
              simpleMediaType);
            var fileName = request.getParameter("fileName");
            if (!Property.hasValue(fileName)) {
              var baseFileName = query.getBaseFileName();
              if (baseFileName == null) {
                baseFileName = query.getTablePath()
                  .getName();
              }
              fileName = baseFileName + "." + writerFactory.getFileExtension(simpleMediaType);
            }
            headers.add("Content-Disposition", "attachment; filename=" + fileName);

            try (
              var out = outputMessage.getBody();
              RecordWriter recordWriter = writerFactory.newRecordWriter("", reader, out,
                StandardCharsets.UTF_8)) {
              recordWriter.setProperty("maxFieldLength", 32000);
              recordWriter.writeAll(reader);
            }
          }
        }
      });
  }

  private void writeJson(final HttpOutputMessage outputMessage, final Query query,
    final RecordReader reader) throws IOException {
    final var offset = query.getOffset();
    final var limit = query.getLimit();
    try (
      var out = outputMessage.getBody();
      var writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
      var jsonWriter = new JsonRecordWriter(reader, writer);) {
      final JsonObject header = JsonObject.hash();
      jsonWriter.setHeader(header);
      Long count = null;
      if (query.isReturnCount()) {
        count = query.getRecordCount();
      }
      if (count != null) {
        header.addValue("@odata.count", count);
      }
      final var resultHeaders = query.getResultHeaders();
      if (resultHeaders != null) {
        header.addValues(resultHeaders);
      }
      jsonWriter.setItemsPropertyName("value");
      final int writeCount = jsonWriter.writeAll(reader);
      final int nextSkip = offset + writeCount;
      boolean writeNext = false;
      if (writeCount != 0) {
        if (count == null) {
          if (writeCount >= limit) {
            writeNext = true;
          }
        } else if (offset + writeCount < count) {
          writeNext = true;
        }
      }

      if (writeNext) {
        final var request = ((ServletRequestAttributes)RequestContextHolder
          .currentRequestAttributes()).getRequest();
        final String nextLink = HttpServletUtils.getFullRequestUriBuilder(request)
          .setParameter("$skip", nextSkip)
          .buildString();
        jsonWriter.setFooter(JsonObject.hash("@odata.nextLink", nextLink));
      }
    }
  }

}
