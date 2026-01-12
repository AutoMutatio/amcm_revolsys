package com.revolsys.web;

import java.io.IOException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.web.util.UrlPathHelper;

import com.revolsys.exception.WrappedIoException;
import com.revolsys.record.io.BufferedWriterEx;
import com.revolsys.util.Property;
import com.revolsys.util.UriBuilder;

public final class HttpServletUtils {

  public static boolean getBooleanParameter(final HttpServletRequest request,
    final String paramName) {
    final String value = request.getParameter(paramName);
    if (Property.hasValue(value)) {
      return Boolean.parseBoolean(value);
    }
    return false;
  }

  public static Boolean getBoolParameter(final HttpServletRequest request, final String paramName) {
    final String value = request.getParameter(paramName);
    if (Property.hasValue(value)) {
      return Boolean.parseBoolean(value);
    }
    return null;
  }

  public static UriBuilder getFullRequestUriBuilder(final HttpServletRequest request) {
    String scheme = request.getHeader("x-forwarded-proto");
    if (scheme == null) {
      scheme = request.getScheme();
    }
    final String serverName = request.getServerName();
    final String forwardedPort = request.getHeader("x-forwarded-port");
    final int serverPort;
    if (forwardedPort == null) {
      serverPort = request.getServerPort();
    } else {
      serverPort = Integer.parseInt(forwardedPort);
    }
    final StringBuilder url = new StringBuilder();
    url.append(scheme);
    url.append("://");
    url.append(serverName);

    if ("http".equals(scheme)) {
      if (serverPort != 80 && serverPort != -1) {
        url.append(":")
          .append(serverPort);
      }
    } else if ("https".equals(scheme)) {
      if (serverPort != 443 && serverPort != -1) {
        url.append(":")
          .append(serverPort);
      }
    }
    final String serverUrl = url.toString();
    final String originatingRequestUri = new UrlPathHelper().getOriginatingRequestUri(request);
    final String requestUri = originatingRequestUri;
    final String uri = serverUrl + requestUri;
    return new UriBuilder(uri);
  }

  public static BufferedWriterEx getWriter(final HttpServletResponse response) {
    try {
      final var out = response.getOutputStream();
      return BufferedWriterEx.forStream(out);
    } catch (final IOException e) {
      throw new WrappedIoException(e);
    }
  }

}
