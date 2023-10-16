package com.revolsys.record.io.format.json;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.SQLException;

import com.revolsys.collection.json.JsonParser;
import com.revolsys.exception.WrappedException;
import com.revolsys.io.AbstractIoFactory;
import com.revolsys.io.FileUtil;
import com.revolsys.io.map.MapReader;
import com.revolsys.io.map.MapReaderFactory;
import com.revolsys.io.map.MapWriter;
import com.revolsys.io.map.MapWriterFactory;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.RecordWriterFactory;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.spring.resource.Resource;

public class JsonIo extends AbstractIoFactory
  implements MapReaderFactory, MapWriterFactory, RecordWriterFactory {

  public static final String FILE_EXTENSION = "json";

  public static final String MIME_TYPE = "application/json";

  public static final String MIME_TYPE_UTF8 = "application/json;charset=utf-8";

  public static JsonParser newParser(final Object source) {
    Runnable closeAction = null;
    Reader reader;
    if (source instanceof Clob) {
      try {
        final Clob clob = (Clob)source;
        reader = clob.getCharacterStream();

        closeAction = () -> {
          try {
            clob.free();
          } catch (final SQLException e) {
            throw new RuntimeException("Unable to free clob resources", e);
          }
        };
      } catch (final SQLException e) {
        throw new RuntimeException("Unable to read clob", e);
      }
    } else if (source instanceof Reader) {
      reader = (Reader)source;
    } else if (source instanceof CharSequence) {
      reader = new StringReader(source.toString());
    } else {
      try {
        final Resource resource = Resource.getResource(source);
        reader = resource.newBufferedReader();
      } catch (final WrappedException e) {
        reader = new StringReader(source.toString());
      }
    }
    return new JsonParser(reader, closeAction);
  }

  @SuppressWarnings("unchecked")
  public static <V> V read(final Object source) {
    try (
      final JsonParser parser = newParser(source)) {
      return (V)JsonParser.read(parser);
    }
  }

  public JsonIo() {
    super("JSON");
    addMediaTypeAndFileExtension(MIME_TYPE, FILE_EXTENSION);
  }

  @Override
  public boolean isReadFromZipFileSupported() {
    return true;
  }

  @Override
  public MapReader newMapReader(final Resource resource) {
    return new JsonMapReader(resource.getInputStream());
  }

  @Override
  public MapWriter newMapWriter(final OutputStream out, final Charset charset) {
    return newMapWriter(out);
  }

  @Override
  public MapWriter newMapWriter(final Writer out) {
    return new JsonMapWriter(out);
  }

  @Override
  public RecordWriter newRecordWriter(final String baseName,
    final RecordDefinitionProxy recordDefinition, final OutputStream outputStream,
    final Charset charset) {
    final OutputStreamWriter writer = FileUtil.newUtf8Writer(outputStream);
    return new JsonRecordWriter(recordDefinition, writer);
  }
}
