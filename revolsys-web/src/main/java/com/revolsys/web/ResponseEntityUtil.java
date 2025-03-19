package com.revolsys.web;

import java.util.Objects;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.revolsys.collection.value.Single;
import com.revolsys.net.http.ApacheHttpException;

public class ResponseEntityUtil {
  public static ResponseEntity<?> notFound() {
    return new ProblemDetailEx().status(HttpStatus.NOT_FOUND)
      .responseEntity();
  }

  public static <T> ResponseEntity<T> of(final Single<T> body) {
    Objects.requireNonNull(body, "Body must not be null");
    return body.map(ResponseEntity::ok)
      .getOrDefault(() -> ResponseEntity.notFound()
        .build());
  }

  public static ResponseEntity<?> fromHttpException(final ApacheHttpException e) {
    final var content = e.getContent();
    final var statusCode = e.getStatusCode();
    return new ProblemDetailEx().status(statusCode)
      .detail(content)
      .responseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  public static ResponseEntity<?> emptyNotFound(final Single<ResponseEntity<?>> map) {
    return map.getOrDefault(ResponseEntityUtil::notFound);
  }
}
