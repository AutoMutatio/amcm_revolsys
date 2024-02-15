package com.revolsys.web.security;

import java.security.Principal;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

public class PrincipalHttpServletRequest extends HttpServletRequestWrapper {

  private final Principal principal;

  public PrincipalHttpServletRequest(final HttpServletRequest request, final Principal principal) {
    super(request);
    this.principal = principal;
  }

  @Override
  public Principal getUserPrincipal() {
    return this.principal;
  }

  @Override
  public String toString() {
    return getRequestURL().toString();
  }
}
