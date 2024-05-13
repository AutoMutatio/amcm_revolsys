package com.revolsys.record.io.format.json;

import java.io.IOException;

import com.revolsys.collection.json.JsonWriterUtil;

public class JsonStringEncodingWriter {

  private final Appendable out;

  public JsonStringEncodingWriter(final Appendable out) {
    this.out = out;
  }

  public Appendable append(final CharSequence string) throws IOException {
    final int length = string.length();
    return append(string, 0, length);
  }

  public Appendable append(final CharSequence string, int startIndex, final int length)
      throws IOException {
    final var out = this.out;
    int count = 0;
    final int endIndex = startIndex + length;
    for (int i = 0; i < endIndex; i++) {
      final char c = string.charAt(i);
      if (c < JsonWriterUtil.CHARACTER_ESCAPE_END) {
        out.append(string, startIndex, startIndex + count);
        final String escape = JsonWriterUtil.CHARACTER_ESCAPE[c];
        out.append(escape);
        startIndex = i + 1;
        count = 0;
      } else if (c == '"') {
        out.append(string, startIndex, startIndex + count);
        out.append('\\');
        out.append('"');
        startIndex = i + 1;
        count = 0;
      } else if (c == '\\') {
        out.append(string, startIndex, startIndex + count);
        out.append('\\');
        out.append('\\');
        startIndex = i + 1;
        count = 0;
      } else {
        if (count == 1024) {
          out.append(string, startIndex, startIndex + count);
          startIndex = i;
          count = 0;
        }
        count++;
      }
    }
    out.append(string, startIndex, startIndex + count);
    return this;
  }

}
