package com.revolsys.swing.logging;

import java.lang.ScopedValue.Carrier;
import java.util.function.Consumer;

import com.revolsys.log.LogbackUtil;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class ScopedAppender extends AppenderBase<ILoggingEvent> {
  private static class ScopedWrapper {
    private final Consumer<ILoggingEvent> appender;

    private boolean hasError;

    public ScopedWrapper(Consumer<ILoggingEvent> appender) {
      this.appender = appender;
    }

    public void append(final ILoggingEvent event) {
      if (event.getLevel()
        .equals(Level.ERROR)) {
        this.hasError = true;
      }
      appender.accept(event);
    }

    public boolean isHasError() {
      return hasError;
    }
  }

  private static final ScopedAppender INSTANCE;

  static {
    INSTANCE = new ScopedAppender();
    LogbackUtil.addRootAppender(INSTANCE);
  }

  public static final ScopedValue<ScopedWrapper> APPENDER = ScopedValue.newInstance();

  public static ScopedAppender instance() {
    return INSTANCE;
  }

  public static boolean isHasError() {
    if (APPENDER.isBound()) {
      return false;
    } else {
      return APPENDER.get()
        .isHasError();
    }
  }

  public static Carrier scoped(AppenderBase<ILoggingEvent> appender) {
    return ScopedValue.where(APPENDER, new ScopedWrapper((event) -> appender.doAppend(event)));
  }

  public static Carrier scoped(Consumer<ILoggingEvent> callback) {
    return ScopedValue.where(APPENDER, new ScopedWrapper(callback));
  }

  public ScopedAppender() {
    setName("scoped");
  }

  @Override
  protected void append(final ILoggingEvent event) {
    if (APPENDER.isBound()) {
      APPENDER.get()
        .append(event);
    }
  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub
    super.stop();
  }

}
