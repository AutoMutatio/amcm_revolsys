package com.revolsys.ui.web.config;

import java.io.IOException;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public interface Action {
  void init(ServletContext context) throws ServletException;

  void process(HttpServletRequest request, HttpServletResponse response)
    throws IOException, ServletException;
}
