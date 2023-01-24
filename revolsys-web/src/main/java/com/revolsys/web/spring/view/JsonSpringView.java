package com.revolsys.web.spring.view;

import java.io.Writer;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.view.AbstractView;

import com.revolsys.record.io.format.json.JsonObject;

public class JsonSpringView extends AbstractView {

  private final JsonObject message;

  public JsonSpringView(final JsonObject message) {
    this.message = message;
  }

  @Override
  protected void renderMergedOutputModel(final Map<String, Object> model,
    final HttpServletRequest request, final HttpServletResponse response) throws Exception {
    response.setContentType("application/json; charset=utf-8");
    try (
      Writer writer = response.getWriter()) {
      final String text = this.message.toJsonString(true);
      writer.write(text);
    }
  }

  @Override
  public String toString() {
    return this.message.toString();
  }
}
