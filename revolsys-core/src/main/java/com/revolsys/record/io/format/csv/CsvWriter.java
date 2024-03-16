package com.revolsys.record.io.format.csv;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;

import com.revolsys.exception.Exceptions;
import com.revolsys.util.BaseCloseable;

public class CsvWriter implements BaseCloseable {
  /** The writer */
  private final Writer out;

  private String newLine = "\n";

  /**
   * Constructs CSVReader with supplied separator and quote char.
   *
   * @param reader The reader to the CSV file.
   * @throws IOException
   */
  protected CsvWriter(final Writer out) {
    this.out = out;
  }

  /**
   * Closes the underlying reader.
   *
   * @throws IOException if the close fails
   */
  @Override
  public void close() {
    flush();
    BaseCloseable.closeSilent(this.out);
  }

  public void flush() {
    try {
      this.out.flush();
    } catch (final IOException e) {
    }
  }

  public String getNewLine() {
    return this.newLine;
  }

  public void setMaxFieldLength(final int maxFieldLength) {
  }

  public void setNewLine(final String newLine) {
    this.newLine = newLine;
  }

  public void setUseQuotes(final boolean useQuotes) {
  }

  public void write(final Collection<? extends Object> values) {
    write(values.toArray());
  }

  public void write(final Object... values) {
    try {
      final Writer out = this.out;
      for (int i = 0; i < values.length; i++) {
        final Object value = values[i];
        if (value != null) {
          final String string = value.toString();

          final int length = string.length();
          if (length > 0) {
            out.write('"');
            int start = 0;
            for (int j = 0; j < length; j++) {
              final char c = string.charAt(j);
              if (c == '"') {
                out.write(string, start, j - start + 1);
                start = j + 1;
                out.write('"');
              }
            }
            if (start < length) {
              out.write(string, start, length - start);
            }
            out.write('"');
          }
        }
        if (i < values.length - 1) {
          out.write(',');
        }
      }
      out.write(this.newLine);
    } catch (final IOException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }
}
