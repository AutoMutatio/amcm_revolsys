package com.revolsys.record.io.format.odata;

import java.nio.CharBuffer;

import com.revolsys.reactive.chars.ValueProcessor;
import com.revolsys.record.io.format.json.JsonChararacterProcessor;
import com.revolsys.record.io.format.json.JsonObject;
import com.revolsys.record.io.format.json.JsonProcessor;
import com.revolsys.record.io.format.json.JsonStatus;
import com.revolsys.record.io.format.json.MultiToJsonProcessor;
import com.revolsys.record.io.format.json.ToJsonProcessor;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

public class ODataCollectionProcessor {
  public static Flux<JsonObject> create(final Flux<CharBuffer> source) {

    return Flux.create(s -> {
      final MultiToJsonProcessor<JsonObject> multiProcessor = new MultiToJsonProcessor<JsonObject>(
        mp -> {
          final ToJsonProcessor<JsonObject> toJson = new ToJsonProcessor<>(
            new ValueProcessor<JsonObject>() {

              @Override
              public void onCancel() {
                s.complete();
              }

              @Override
              public void onComplete() {
                s.complete();
              }

              @Override
              public boolean process(final JsonObject value) {
                s.next(value);
                return true;
              }

            });
          return new JsonProcessor() {
            @Override
            public void label(final JsonStatus status, final String label) {
              if ("value".equals(label)) {
                mp.processorPush(status, new JsonProcessor() {
                  @Override
                  public void beforeArrayValue(final JsonStatus status) {
                    mp.processorPush(status, toJson);
                  }
                });
              }
            }

          };
        });
      final JsonChararacterProcessor toJson = new JsonChararacterProcessor(multiProcessor);
      final Disposable subscription = source.doOnNext(buffer -> toJson.process(buffer))
        .doOnComplete(toJson::onComplete)
        .subscribe();
      s.onCancel(subscription);
      s.onDispose(subscription);

    });

  }
}
