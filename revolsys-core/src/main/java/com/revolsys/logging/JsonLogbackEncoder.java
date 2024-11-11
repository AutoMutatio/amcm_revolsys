package com.revolsys.logging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.event.KeyValuePair;

import com.revolsys.collection.json.JsonWriter;
import com.revolsys.exception.Exceptions;
import com.revolsys.util.Property;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.encoder.EncoderBase;

/**
 * Write logging events as Json. This encoder keeps state, so must be used by only 1 appender.
 */
public class JsonLogbackEncoder extends EncoderBase<ILoggingEvent> {

  private static final byte[] FOOTER = "".getBytes();

  private static final byte[] HEADER = "\n".getBytes();

  @Override
  public byte[] encode(final ILoggingEvent event) {
    try (
      var out = new ByteArrayOutputStream()) {
      try (
        var json = new JsonWriter(out, false)) {
        json.startObject();

        final var sequenceNumber = event.getSequenceNumber();
        if (sequenceNumber > 0) {
          json.labelValue("i", sequenceNumber);
        }

        final var level = event.getLevel();
        if (level != null) {
          json.labelValue("level", level);
        }

        final var timestamp = event.getInstant();
        json.labelValue("timestamp", timestamp);

        final var logger = event.getLoggerName();
        if (logger != null) {
          json.labelValue("logger", logger);
        }

        final var message = event.getMessage();
        if (Property.hasValue(message)) {
          json.labelValue("message", message);
        }

        final var arguments = event.getArgumentArray();
        if (arguments != null && arguments.length > 0) {
          json.labelValue("arguments", arguments);
        }

        final var thread = event.getThreadName();
        if (thread != null) {
          json.labelValue("thread", thread);
        }

        final var exceptionProxy = event.getThrowableProxy();
        if (exceptionProxy instanceof final ThrowableProxy throwableProxy) {
          final var throwable = throwableProxy.getThrowable();
          final var exceptionJson = Exceptions.toJson(throwable);
          json.labelValue("exception", exceptionJson);
        } else {
          writeException(json, "exception", exceptionProxy);
        }

        //// TODO? event.getLoggerContextVO());

        writeMarkers(json, event);

        writeMdc(json, event);

        writeKeyValuePairs(json, event);

        json.endObject();
        json.newLineForce();
      }
      return out.toByteArray();
    } catch (final IOException e) {
      return new byte[0];
    }
  }

  @Override
  public byte[] footerBytes() {
    return FOOTER;
  }

  @Override
  public byte[] headerBytes() {
    return HEADER;
  }

  private void writeException(final JsonWriter json, final String attributeName,
    final IThrowableProxy exception) {
    if (exception == null) {
      return;
    }
    // in the nominal case, attributeName != null. However, attributeName will
    // be null for suppressed
    // IThrowableProxy array, in which case no attribute name is needed
    if (attributeName != null) {
      json.label(attributeName);
    }

    json.startObject();

    json.labelValueNotEmpty("class", exception.getClassName());

    json.labelValueNotEmpty("message", exception.getMessage());

    if (exception.isCyclic()) {
      json.labelValueNotEmpty("cyclic", "true");
    }

    final var commonFrames = exception.getCommonFrames();
    writeStackTrace(json, exception.getStackTraceElementProxyArray(), commonFrames);

    if (commonFrames != 0) {
      json.labelValueNotEmpty("commonFramesCount", commonFrames);
    }

    final IThrowableProxy cause = exception.getCause();
    if (cause != null) {
      writeException(json, "cause", cause);
    }
    final IThrowableProxy[] suppressedArray = exception.getSuppressed();
    if (suppressedArray != null && suppressedArray.length != 0) {
      json.label("suppressed");
      json.startList();
      for (final IThrowableProxy suppressedITP : suppressedArray) {
        writeException(json, null, suppressedITP);
      }
      json.endList();
    }

    json.endObject();
  }

  private void writeKeyValuePairs(final JsonWriter json, final ILoggingEvent event) {
    final List<KeyValuePair> list = event.getKeyValuePairs();
    if (list == null || list.isEmpty()) {
      return;
    }
    json.label("properties");
    json.startObject();
    for (final var kvp : list) {
      json.labelValueNotEmpty(kvp.key, kvp.value);
    }
    json.endObject();
  }

  private void writeMarkers(final JsonWriter json, final ILoggingEvent event) {
    final var markers = event.getMarkerList();
    if (markers == null || markers.isEmpty()) {
      return;
    }
    json.label("markers");
    json.startList();
    for (final var marker : markers) {
      json.value(marker);
    }
    json.endList();
  }

  private void writeMdc(final JsonWriter json, final ILoggingEvent event) {
    final Map<String, String> map = event.getMDCPropertyMap();
    if (map == null || map.isEmpty()) {
      return;
    }
    json.label("mdc");
    json.startObject();
    for (final var key : map.keySet()) {
      final var value = map.get(key);
      json.labelValueNotEmpty(key, value);
    }
    json.endObject();
  }

  private void writeStackTrace(final JsonWriter json, final StackTraceElementProxy[] trace,
    int commonFrames) {
    if (trace != null) {
      json.label("trace");
      json.startList();
      final int len = trace.length;

      if (commonFrames >= len) {
        commonFrames = 0;
      }

      for (int i = 0; i < len - commonFrames; i++) {
        final var methodCall = trace[i];
        final StackTraceElement ste = methodCall.getStackTraceElement();
        final var className = ste.getClassName();
        final var methodName = ste.getMethodName();
        final var fileName = ste.getFileName();
        final var lineNumber = ste.getLineNumber();
        final var s = new StringBuilder().append(className)
          .append('.')
          .append(methodName)
          .append('(')
          .append(fileName)
          .append(':')
          .append(lineNumber)
          .append(')')
          .toString();
        json.value(s);
      }

      json.endList();
    }
  }

}
