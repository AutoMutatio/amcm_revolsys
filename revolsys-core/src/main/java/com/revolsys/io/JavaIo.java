package com.revolsys.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.ServiceLoader;

import com.revolsys.collection.list.Lists;
import com.revolsys.exception.Exceptions;
import com.revolsys.logging.Logs;
import com.revolsys.record.io.BufferedWriterEx;
import com.revolsys.spring.resource.Resource;

public class JavaIo {
  private static List<JavaIo> factories = Lists.newArray();

  static {
    factories.add(new JavaIo());
    try {
      final ClassLoader classLoader = JavaIo.class.getClassLoader();
      final ServiceLoader<JavaIo> factories = ServiceLoader.load(JavaIo.class, classLoader);
      for (final JavaIo factory : factories) {
        JavaIo.factories.add(factory);
      }

    } catch (final Throwable e) {
      Logs.error(JavaIo.class, "Unable to read resources", e);
    }
  }

  public static OutputStream createOutputStream(final Object target) throws IOException {
    for (final JavaIo factory : factories) {
      final OutputStream out = factory.newOutputStream(target);
      if (out != null) {
        return out;
      }
    }
    return null;
  }

  public static Reader createReader(final Object source) {
    for (final JavaIo factory : factories) {
      try {
        final Reader reader = factory.newReader(source);
        if (reader != null) {
          return reader;
        }
      } catch (final IOException e) {
        throw Exceptions.toRuntimeException(e);
      }
    }
    return null;
  }

  public static Writer createWriter(final Object target) throws IOException {
    for (final JavaIo factory : factories) {
      final Writer writer = factory.newWriter(target);
      if (writer != null) {
        return writer;
      }
    }
    return null;
  }

  public OutputStream newOutputStream(final Object target) throws IOException {
    if (target == null) {
      return null;
    } else if (target instanceof final OutputStream out) {
      return out;
    } else if (target instanceof final Path path) {
      return Files.newOutputStream(path, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);
    } else if (target instanceof final File file) {
      return new FileOutputStream(file);
    } else if (target instanceof final URI uri) {
      if ("file".equals(uri.getScheme())) {
        return new FileOutputStream(new File(uri));
      }
    } else if (target instanceof final URL url) {
      if ("file".equals(url.getProtocol())) {
        try {
          return new FileOutputStream(new File(url.toURI()));
        } catch (final URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
      }
    } else if (target instanceof final Resource resource) {
      return resource.newOutputStream();
    }
    return null;

  }

  public Reader newReader(final Object source) throws IOException {
    if (source == null) {
      return null;
    } else if (source instanceof final Reader reader) {
      return reader;
    } else if (source instanceof final Path path) {
      if (Files.exists(path)) {
        return Files.newBufferedReader(path, StandardCharsets.UTF_8);
      }
    } else if (source instanceof final File file) {
      if (file.exists()) {
        return new FileReader(file);
      }
    } else if (source instanceof final InputStream in) {
      return new InputStreamReader(in, StandardCharsets.UTF_8);
    } else if (source instanceof final URI uri) {
      return new InputStreamReader(uri.toURL()
        .openStream());
    } else if (source instanceof final URL url) {
      return new InputStreamReader(url.openStream());
    }
    return null;
  }

  public Writer newWriter(final Object target) throws IOException {
    if (target == null) {
      return null;
    } else if (target instanceof final Writer writer) {
      return writer;
    } else if (target instanceof final Path path) {
      return Files.newBufferedWriter(path, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);
    } else if (target instanceof final File file) {
      return FileUtil.getWriter(file);
    } else if (target instanceof final OutputStream out) {
      return BufferedWriterEx.forStream(out);
    } else if (target instanceof final URI uri) {
      if ("file".equals(uri.getScheme())) {
        return FileUtil.getWriter(new File(uri));
      }
    } else if (target instanceof final URL url) {
      if ("file".equals(url.getProtocol())) {
        try {
          return FileUtil.getWriter(new File(url.toURI()));
        } catch (final URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
      }
    } else if (target instanceof final Resource resource) {
      return resource.newWriter();
    }
    return null;
  }
}
