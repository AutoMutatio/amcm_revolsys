package com.revolsys.web;

import java.net.URI;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;

import com.revolsys.net.http.ApacheHttpException;

public record ProblemDetailBuilder(ProblemDetail problem) {

  public static ResponseEntity<?> responseEntity(final ApacheHttpException e) {
    return ProblemDetailBuilder.forStatus(e.getStatusCode())
      .detail(e.getContent())
      .responseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  public static ProblemDetailBuilder forStatus(final HttpStatus status) {
    return new ProblemDetailBuilder(ProblemDetail.forStatus(status));
  }

  public static ProblemDetailBuilder notFound(final String title) {
    return forStatus(HttpStatus.NOT_FOUND).title(title);
  }

  public static ProblemDetailBuilder forStatus(final int status) {
    return new ProblemDetailBuilder(ProblemDetail.forStatus(status));
  }

  public ProblemDetailBuilder detail(final String detail) {
    this.problem.setDetail(detail);
    return this;
  }

  public ProblemDetailBuilder instance(final Map<String, Object> properties) {
    this.problem.setProperties(properties);
    return this;
  }

  public ProblemDetailBuilder instance(final URI instance) {
    this.problem.setInstance(instance);
    return this;
  }

  public ResponseEntity<?> responseEntity() {
    return ResponseEntity.of(this.problem)
      .build();
  }

  public ResponseEntity<?> responseEntity(final HttpStatus responseStatus) {
    return ResponseEntity.status(responseStatus)
      .body(this.problem);
  }

  public ProblemDetailBuilder setProperty(final String key, final Object value) {
    this.problem.setProperty(key, value);
    return this;
  }

  public ProblemDetailBuilder status(final HttpStatus status) {
    this.problem.setStatus(status);
    return this;
  }

  public ProblemDetailBuilder status(final int status) {
    this.problem.setStatus(status);
    return this;
  }

  public ProblemDetailBuilder title(final String title) {
    this.problem.setTitle(title);
    return this;
  }

  public ProblemDetailBuilder type(final URI type) {
    this.problem.setType(type);
    return this;
  }
}
