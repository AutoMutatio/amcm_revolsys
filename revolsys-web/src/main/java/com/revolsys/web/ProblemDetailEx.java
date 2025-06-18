package com.revolsys.web;

import java.net.URI;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;

public class ProblemDetailEx extends ProblemDetail {

  public ProblemDetailEx() {

  }

  public ProblemDetailEx detail(final String detail) {
    setDetail(detail);
    return this;
  }

  public HttpStatus httpStatus() {
    final var status = getStatus();
    return HttpStatus.resolve(status);
  }

  public ProblemDetailEx instance(final Map<String, Object> properties) {
    setProperties(properties);
    return this;
  }

  public ProblemDetailEx instance(final URI instance) {
    setInstance(instance);
    return this;
  }

  public ProblemDetailEx property(final String key, final Object value) {
    setProperty(key, value);
    return this;
  }

  public ResponseEntity<?> responseEntity() {
    final var status = httpStatus();
    return responseEntity(status);
  }

  public ResponseEntity<?> responseEntity(final HttpStatus responseStatus) {
    return ResponseEntity.status(responseStatus)
      .body(this);
  }

  public ProblemDetailEx status(final HttpStatus status) {
    setStatus(status);
    return this;
  }

  public ProblemDetailEx status(final int status) {
    setStatus(status);
    return this;
  }

  public ProblemDetailEx title(final String title) {
    setTitle(title);
    return this;
  }

  public ProblemDetailEx type(final URI type) {
    setType(type);
    return this;
  }
}
