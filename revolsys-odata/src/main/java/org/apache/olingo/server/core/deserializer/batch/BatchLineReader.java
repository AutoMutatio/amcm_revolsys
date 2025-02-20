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
package org.apache.olingo.server.core.deserializer.batch;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;

public class BatchLineReader {
  /**
   * Read state indicator (whether currently the <code>body</code> or <code>header</code> part is read).
   */
  private static class ReadState {
    private int state = 0;

    public void foundBoundary() {
      this.state = 0;
    }

    public void foundLinebreak() {
      this.state++;
    }

    public boolean isReadBody() {
      return this.state >= 2;
    }

    @Override
    public String toString() {
      return String.valueOf(this.state);
    }
  }

  private static final byte CR = '\r';

  private static final byte LF = '\n';

  private static final int EOF = -1;

  private static final int BUFFER_SIZE = 8192;

  private static final Charset DEFAULT_CHARSET = Charset.forName("ISO-8859-1");

  public static final String BOUNDARY = "boundary";

  public static final String DOUBLE_DASH = "--";

  public static final String CRLF = "\r\n";

  public static final String LFS = "\n";

  private Charset currentCharset = DEFAULT_CHARSET;

  private String currentBoundary = null;

  private final ReadState readState = new ReadState();

  private final InputStream reader;

  private final byte[] buffer;

  private int offset = 0;

  private int limit = 0;

  public BatchLineReader(final InputStream reader) {
    this(reader, BUFFER_SIZE);
  }

  public BatchLineReader(final InputStream reader, final int bufferSize) {
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("Buffer size must be greater than zero.");
    }

    this.reader = reader;
    this.buffer = new byte[bufferSize];
  }

  public void close() throws IOException {
    this.reader.close();
  }

  private int fillBuffer() throws IOException {
    this.limit = this.reader.read(this.buffer, 0, this.buffer.length);
    this.offset = 0;

    return this.limit;
  }

  private ByteBuffer grantBuffer(ByteBuffer buffer) {
    if (!buffer.hasRemaining()) {
      buffer.flip();
      final ByteBuffer tmp = ByteBuffer.allocate(buffer.limit() * 2);
      tmp.put(buffer);
      buffer = tmp;
    }
    return buffer;
  }

  private boolean isBoundary(final String currentLine) {
    return (this.currentBoundary + CRLF).equals(currentLine)
      || (this.currentBoundary + LFS).equals(currentLine)
      || (this.currentBoundary + DOUBLE_DASH + CRLF).equals(currentLine)
      || (this.currentBoundary + DOUBLE_DASH + LFS).equals(currentLine);
  }

  String readLine() throws IOException {
    if (this.limit == EOF) {
      return null;
    }

    ByteBuffer innerBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    // EOF will be considered as line ending
    boolean foundLineEnd = false;

    while (!foundLineEnd) {
      // Is buffer refill required?
      if (this.limit == this.offset && fillBuffer() == EOF) {
        foundLineEnd = true;
      }

      if (!foundLineEnd) {
        final byte currentChar = this.buffer[this.offset++];
        innerBuffer = grantBuffer(innerBuffer);
        innerBuffer.put(currentChar);

        if (currentChar == LF) {
          foundLineEnd = true;
        } else if (currentChar == CR) {
          foundLineEnd = true;

          // Check next byte. Consume \n if available
          // Is buffer refill required?
          if (this.limit == this.offset) {
            fillBuffer();
          }

          // Check if there is at least one character
          if (this.limit != EOF && this.buffer[this.offset] == LF) {
            innerBuffer = grantBuffer(innerBuffer);
            innerBuffer.put(LF);
            this.offset++;
          }
        }
      }
    }

    if (innerBuffer.position() == 0) {
      return null;
    } else {
      final String currentLine = new String(innerBuffer.array(), 0, innerBuffer.position(),
        this.readState.isReadBody() ? this.currentCharset : DEFAULT_CHARSET);
      updateCurrentCharset(currentLine);
      return currentLine;
    }
  }

  public List<Line> toLineList() throws IOException {
    final List<Line> result = new ArrayList<>();
    String currentLine = readLine();
    if (currentLine != null) {
      this.currentBoundary = currentLine.strip();
      int counter = 1;
      result.add(new Line(currentLine, counter++));

      while ((currentLine = readLine()) != null) {
        result.add(new Line(currentLine, counter++));
      }
    }

    return result;
  }

  public List<String> toList() throws IOException {
    final List<String> result = new ArrayList<>();
    String currentLine = readLine();
    if (currentLine != null) {
      this.currentBoundary = currentLine.strip();
      result.add(currentLine);

      while ((currentLine = readLine()) != null) {
        result.add(currentLine);
      }
    }
    return result;
  }

  private void updateCurrentCharset(final String currentLine) {
    if (currentLine != null) {
      if (currentLine.toLowerCase(Locale.ENGLISH)
        .startsWith(HttpHeader.CONTENT_TYPE.toLowerCase(Locale.ENGLISH))) {
        final int cutOff = currentLine.endsWith(CRLF) ? 2 : currentLine.endsWith(LFS) ? 1 : 0;
        final ContentType contentType = ContentType.parse(
          currentLine.substring(HttpHeader.CONTENT_TYPE.length() + 1, currentLine.length() - cutOff)
            .strip());
        if (contentType != null) {
          final String charsetString = contentType.getParameter(ContentType.PARAMETER_CHARSET);
          this.currentCharset = charsetString == null
            ? contentType.isCompatible(ContentType.APPLICATION_JSON) || contentType.getSubtype()
              .contains("xml") ? Charset.forName("UTF-8") : DEFAULT_CHARSET
            : Charset.forName(charsetString);

          final String boundary = contentType.getParameter(BOUNDARY);
          if (boundary != null) {
            this.currentBoundary = DOUBLE_DASH + boundary;
          }
        }
      } else if (CRLF.equals(currentLine) || LFS.equals(currentLine)) {
        this.readState.foundLinebreak();
      } else if (isBoundary(currentLine)) {
        this.readState.foundBoundary();
      }
    }
  }
}
