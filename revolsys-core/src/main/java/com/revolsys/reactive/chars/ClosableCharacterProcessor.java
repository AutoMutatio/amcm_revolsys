package com.revolsys.reactive.chars;

import java.io.Closeable;
import java.io.IOException;

public class ClosableCharacterProcessor implements CharacterProcessor {
  private final CharacterProcessor processor;

  private final Closeable closeable;

  public ClosableCharacterProcessor(Closeable closeable, CharacterProcessor processor) {
    this.closeable = closeable;
    this.processor = processor;
  }

  public void close() {
    try {
      this.closeable.close();
    } catch (final IOException e) {
    }
  }

  @Override
  public void onCancel() {
    try {
      this.processor.onCancel();
    } finally {
      close();
    }
  }

  @Override
  public void onComplete() {
    try {
      this.processor.onComplete();
    } finally {
      close();
    }
  }

  @Override
  public void onError(Throwable e) {
    try {
      this.processor.onError(e);
    } finally {
      close();
    }
  }

  @Override
  public boolean process(char c) {
    return this.processor.process(c);
  }

}
