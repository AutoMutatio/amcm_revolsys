package com.revolsys.io;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;

import com.revolsys.exception.Exceptions;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.Property;

public class IoUtil {

  public static void copy(final byte[] text, final File file) {
    copy(new ByteArrayInputStream(text), file);
  }

  public static void copy(final File src, final File dest) {
    if (src != null && dest != null) {
      if (src.isDirectory()) {
        dest.mkdirs();
        final File[] files = src.listFiles();
        if (files != null) {
          for (final File file : files) {
            final String name = getFileName(file);
            final File destFile = new File(dest, name);
            copy(file, destFile);
          }
        }
      } else {
        try {
          final FileInputStream in = new FileInputStream(src);
          File destFile;
          if (dest.isDirectory()) {
            final String name = getFileName(src);
            destFile = new File(dest, name);
          } else {
            destFile = dest;
          }
          copy(in, destFile);
        } catch (final FileNotFoundException e) {
          Exceptions.throwUncheckedException(e);
        }
      }
    }
  }

  /**
   * Copy the contents of the file to the output stream. The output stream will
   * need to be closed manually after invoking this method.
   *
   * @param file The file to read the contents from.
   * @param out The output stream to write the contents to.
   * @throws IOException If an I/O error occurs.
   */
  public static long copy(final File file, final OutputStream out) throws IOException {
    final FileInputStream in = new FileInputStream(file);
    try {
      return copy(in, out);
    } finally {
      in.close();
    }
  }

  /**
   * Copy the contents of the file to the writer. The writer will need to be
   * closed manually after invoking this method.
   *
   * @param file The file to read the contents from.
   * @param out The writer to write the contents to.
   * @throws IOException If an I/O error occurs.
   */
  public static long copy(final File file, final Writer out) throws IOException {
    final FileReader in = getReader(file);
    try {
      return copy(in, out);
    } finally {
      in.close();
    }
  }

  /**
   * Copy the contents of the input stream to the file. The input stream will
   * need to be closed manually after invoking this method.
   *
   * @param in The input stream to read the contents from.
   * @param file The file to write the contents to.
   * @throws IOException If an I/O error occurs.
   */
  public static long copy(final InputStream in, final File file) {
    try {
      file.getParentFile()
        .mkdirs();
      try (
        final FileOutputStream out = new FileOutputStream(file)) {
        return in.transferTo(out);
      }
    } catch (final IOException e) {
      throw Exceptions.wrap("Unable to open file: " + file, e);
    }
  }

  /**
   * Writes the content of a zip entry to a file using NIO.
   *
   * @param zin input stream from zip file
   * @param file file path where this entry will be saved
   * @param sz file size
   * @throws IOException if an i/o error
   */
  public static void copy(final InputStream zin, final File file, final long sz)
    throws IOException {

    ReadableByteChannel rc = null;
    FileOutputStream out = null;

    try {
      rc = Channels.newChannel(zin);
      out = new FileOutputStream(file);
      final FileChannel fc = out.getChannel();

      // read into the buffer
      long count = 0;
      int attempts = 0;
      while (count < sz) {
        final long written = fc.transferFrom(rc, count, sz);
        count += written;

        if (written == 0) {
          attempts++;
          if (attempts > 100) {
            throw new IOException("Error writing to file " + file);
          }
        } else {
          attempts = 0;
        }
      }

      out.close();
      out = null;
    } finally {
      if (out != null) {
        BaseCloseable.closeSilent(out);
      }
    }
  }

  /**
   * Copy the contents of the input stream to the output stream. The input
   * stream and output stream will need to be closed manually after invoking
   * this method.
   *
   * @param in The input stream to read the contents from.
   * @param out The output stream to write the contents to.
   */
  public static long copy(final InputStream in, final OutputStream out) {
    if (in == null) {
      return 0;
    } else {
      try {
        return in.transferTo(out);
      } catch (final IOException e) {
        return (Long)Exceptions.throwUncheckedException(e);
      }
    }
  }

  /**
   * Copy the contents of the input stream to the output stream. The input
   * stream and output stream will need to be closed manually after invoking
   * this method.
   *
   * @param in The input stream to read the contents from.
   * @param out The output stream to write the contents to.
   */
  public static long copy(final InputStream in, final OutputStream out, final long length) {
    if (in == null) {
      return 0;
    } else {
      try {
        final byte[] buffer = new byte[4096];
        long totalBytes = 0;
        int readBytes;
        while (totalBytes < length && (readBytes = in.read(buffer)) > -1) {
          if (totalBytes + readBytes > length) {
            readBytes = (int)(length - totalBytes);
          }
          totalBytes += readBytes;
          out.write(buffer, 0, readBytes);
        }
        return totalBytes;
      } catch (final IOException e) {
        return (Long)Exceptions.throwUncheckedException(e);
      }
    }
  }

  /**
   * Copy the contents of the reader to the file. The reader will need to be
   * closed manually after invoking this method.
   *
   * @param in The reader to read the contents from.
   * @param file The file to write the contents to.
   * @throws IOException If an I/O error occurs.
   */
  public static void copy(final Reader in, final File file) {
    try {
      final FileWriter out = new FileWriter(file);
      try {
        copy(in, out);
      } finally {
        BaseCloseable.closeSilent(in);
        BaseCloseable.closeSilent(out);
      }
    } catch (final IOException e) {
      throw new IllegalArgumentException("Unable to write to " + file);
    }
  }

  /**
   * Copy the contents of the reader to the writer. The reader and writer will
   * need to be closed manually after invoking this method.
   *
   * @param in The reader to read the contents from.
   * @param out The writer to write the contents to.
   * @throws IOException If an I/O error occurs.
   */
  public static long copy(final Reader in, final Writer out) {
    try {
      final char[] buffer = new char[4906];
      long numBytes = 0;
      int count;
      while ((count = in.read(buffer)) > -1) {
        out.write(buffer, 0, count);
        numBytes += count;
      }
      return numBytes;
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  /**
   * Copy the contents of the reader to the writer. The input
   * stream and output stream will need to be closed manually after invoking
   * this method.
   *
   * @param in The input stream to read the contents from.
   * @param out The output stream to write the contents to.
   */
  public static long copy(final Reader in, final Writer out, final long length) {
    if (in == null) {
      return 0;
    } else {
      try {
        final char[] buffer = new char[4096];
        long totalBytes = 0;
        int readBytes;
        while (totalBytes < length && (readBytes = in.read(buffer)) > -1) {
          if (totalBytes + readBytes > length) {
            readBytes = (int)(length - totalBytes);
          }
          totalBytes += readBytes;
          out.write(buffer, 0, readBytes);
        }
        return totalBytes;
      } catch (final IOException e) {
        return (Long)Exceptions.throwUncheckedException(e);
      }
    }
  }

  public static void copy(final String text, final File file) {
    copy(new StringReader(text), file);
  }

  public static String getFileName(final File file) {
    if (file == null) {
      return null;
    } else {
      String fileName = file.getName();
      if (!Property.hasValue(fileName)) {
        fileName = file.getPath()
          .replaceAll("\\\\$", "");
      }
      return fileName;
    }
  }

  public static FileReader getReader(final File file) {
    try {
      return new FileReader(file);
    } catch (final FileNotFoundException e) {
      throw new IllegalArgumentException("File not found: " + file, e);
    }
  }

  public static String getString(final File file) {
    if (file.exists()) {
      final FileReader in = getReader(file);
      return getString(in);
    } else {
      return null;
    }
  }

  public static String getString(final InputStream in) {
    final Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
    return getString(reader);
  }

  public static String getString(final Reader reader) {
    try {
      final StringWriter out = new StringWriter();
      copy(reader, out);
      return out.toString();
    } finally {
      BaseCloseable.closeSilent(reader);
    }
  }

  public static String getString(final Reader reader, final boolean close) {
    try {
      final StringWriter out = new StringWriter();
      copy(reader, out);
      return out.toString();
    } finally {
      if (close) {
        BaseCloseable.closeSilent(reader);
      }
    }
  }

  public static String getString(final Reader reader, final int count) {
    try {
      final StringWriter out = new StringWriter();
      copy(reader, out, count);
      return out.toString();
    } finally {
      BaseCloseable.closeSilent(reader);
    }
  }

  public static long size(final InputStream in) {
    try {
      long size = 0;
      final byte[] buffer = new byte[8192];
      while (true) {
        final int count = in.read(buffer);
        if (count >= 0) {
          size += count;
        } else {
          return size;
        }
      }
    } catch (final IOException e) {
      return Exceptions.throwUncheckedException(e);
    }
  }

  public static Reader toReader(final InputStream in) {
    try {
      return new InputStreamReader(in);
    } catch (final Exception e) {
      return Exceptions.throwUncheckedException(e);
    }
  }

}
