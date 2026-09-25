package com.revolsys.rest;

import java.nio.charset.StandardCharsets;

import jakarta.servlet.http.HttpServletResponse;

import com.revolsys.collection.json.Json;

public class AbstractWebController {

  private static final String UTF_8 = StandardCharsets.UTF_8.toString();

  public static void setContentTypeJson(final HttpServletResponse response) {
    setContentTypeText(response, Json.MIME_TYPE_UTF8);
  }

  public static void setContentTypeText(final HttpServletResponse response,
    final String contentType) {
    response.setCharacterEncoding(UTF_8);
    response.setContentType(contentType);
  }

}
