package com.revolsys.http;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class FormUrlEncodedString {

  private final StringBuilder string = new StringBuilder();

  public FormUrlEncodedString addParameter(final String name, final String value) {
    if (!this.string.isEmpty()) {
      this.string.append('&');
    }
    this.string.append(URLEncoder.encode(name, StandardCharsets.UTF_8));
    this.string.append('=');
    this.string.append(URLEncoder.encode(value, StandardCharsets.UTF_8));
    return this;
  }

  @Override
  public String toString() {
    return this.string.toString();
  }
}
