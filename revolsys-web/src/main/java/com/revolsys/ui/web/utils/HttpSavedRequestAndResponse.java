package com.revolsys.ui.web.utils;

import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import com.revolsys.io.BaseCloseable;

public class HttpSavedRequestAndResponse implements BaseCloseable {
  final HttpServletRequest savedRequest = HttpServletUtils.getRequest();

  final HttpServletResponse savedResponse = HttpServletUtils.getResponse();

  public HttpSavedRequestAndResponse(final HttpServletRequest request,
    final HttpServletResponse response) {
    HttpServletUtils.setRequestAndResponse(request, response);
  }

  public HttpSavedRequestAndResponse(final ServletRequest servletRequest,
    final ServletResponse servletResponse) {
    this((HttpServletRequest)servletRequest, (HttpServletResponse)servletResponse);

  }

  @Override
  public void close() {
    if (this.savedRequest == null) {
      HttpServletUtils.clearRequestAndResponse();
    } else {
      HttpServletUtils.setRequestAndResponse(this.savedRequest, this.savedResponse);
    }
  }
}
