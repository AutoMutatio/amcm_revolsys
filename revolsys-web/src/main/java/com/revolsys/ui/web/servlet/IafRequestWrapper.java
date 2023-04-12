package com.revolsys.ui.web.servlet;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

public class IafRequestWrapper extends HttpServletRequestWrapper {

  public IafRequestWrapper(final HttpServletRequest request) {
    super(request);
  }

}
