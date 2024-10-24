package com.revolsys.logging;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.revolsys.collection.map.LruMap;
import com.revolsys.exception.Exceptions;
import com.revolsys.exception.WrappedRuntimeException;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;

public class Logs {

  private static Map<String, Boolean> LOGGED_ERRORS = new LruMap<>(1000);

  private static void addMessage(final Set<String> messages, final String message) {
    if (message != null && message.length() > 0) {
      messages.add(message);
    }
  }

  public static void debug(final Class<?> clazz, final String message) {
    final String name = clazz.getName();
    debug(name, message);
  }

  public static void debug(final Class<?> clazz, final String message, final Throwable e) {
    final String name = clazz.getName();
    debug(name, message, e);
  }

  public static void debug(final Class<?> clazz, final Throwable e) {
    final String name = clazz.getName();
    debug(name, e);
  }

  public static void debug(final Object object, final String message) {
    final Class<?> clazz = object.getClass();
    debug(clazz, message);
  }

  public static void debug(final Object object, final String message, final Throwable e) {
    final Class<?> clazz = object.getClass();
    debug(clazz, message, e);
  }

  public static void debug(final Object object, final Throwable e) {
    final Class<?> clazz = object.getClass();
    debug(clazz, e);
  }

  public static void debug(final String name, final String message) {
    final Logger logger = logger(name);
    logger.debug(message);
  }

  public static void debug(final String name, final String message, final Throwable e) {
    final StringBuilder messageText = new StringBuilder();
    final Throwable logException = getMessageAndException(messageText, message, e);

    final Logger logger = logger(name);
    logger.debug(messageText.toString(), logException);
  }

  public static void debug(final String name, final Throwable e) {
    final String message = e.getMessage();
    debug(name, message, e);
  }

  public static void error(final Class<?> clazz, final String message) {
    final String name = clazz.getName();
    error(name, message);
  }

  public static void error(final Class<?> clazz, final String message, final Throwable e) {
    final String name = clazz.getName();
    error(name, message, e);
  }

  public static void error(final Class<?> clazz, final Throwable e) {
    final String message = e.getMessage();
    error(clazz, message, e);
  }

  public static void error(final Object object, final String message) {
    final Class<?> clazz = object.getClass();
    error(clazz, message);
  }

  public static void error(final Object object, final String message, final Throwable e) {
    final Class<?> clazz = object.getClass();
    error(clazz, message, e);
  }

  public static void error(final Object object, final Throwable e) {
    final Class<?> clazz = object.getClass();
    error(clazz, e);
  }

  public static void error(final String name, final String message) {
    final Logger logger = logger(name);
    logger.error(message);
  }

  public static void error(final String name, final String message, final Throwable e) {
    final StringBuilder messageText = new StringBuilder();
    final Throwable logException = getMessageAndException(messageText, message, e);

    final Logger logger = logger(name);
    logger.error(messageText.toString(), logException);
  }

  public static void error(final String name, final Throwable e) {
    final String message = e.getMessage();
    error(name, message, e);
  }

  public static void errorOnce(final Object object, final String message, final Throwable e) {
    final StringBuilder messageText = new StringBuilder();
    final Throwable logException = getMessageAndException(messageText, message, e);

    final String combinedMessage = messageText.toString();
    if (!LOGGED_ERRORS.containsKey(combinedMessage)) {
      LOGGED_ERRORS.put(combinedMessage, Boolean.TRUE);
      final Logger logger = logger(object);
      logger.error(combinedMessage, logException);
    }
  }

  public static LoggingEvent event(final Object object, final Level level, final String message) {
    final var logger = (ch.qos.logback.classic.Logger)logger(object);
    final var logClass = ch.qos.logback.classic.Logger.FQCN;
    return new LoggingEvent(logClass, logger, level, message, null, new Object[0]);
  }

  public static Throwable getMessageAndException(final StringBuilder messageText,
    final String message, final Throwable e) {
    Throwable logException = e;
    final Set<String> messages = new LinkedHashSet<>();
    addMessage(messages, message);
    while (logException instanceof WrappedRuntimeException) {
      final String wrappedMessage = logException.getMessage();
      addMessage(messages, wrappedMessage);
      final Throwable cause = logException.getCause();
      if (cause == null) {
        break;
      } else {
        logException = cause;
      }
    }
    if (logException instanceof SQLException) {
      final SQLException sqlException = (SQLException)logException;
      final List<Throwable> exceptions = new ArrayList<>();
      exceptions.add(sqlException);
      final int exceptionCount = exceptions.size();
      if (exceptionCount > 0) {
        logException = exceptions.remove(exceptionCount - 1);
        for (final Throwable throwable : exceptions) {
          if (throwable == sqlException) {
            messages.add(sqlException.getClass()
              .getName());
            final String wrappedMessage = sqlException.getMessage();
            addMessage(messages, wrappedMessage);
          } else {
            messages.add(Exceptions.toString(throwable));
          }
        }
      }
    }
    messages.remove(logException.getMessage());
    boolean first = true;
    for (final String m : messages) {
      if (first) {
        first = false;
      } else {
        messageText.append('\n');
      }
      messageText.append(m);
    }
    return logException;
  }

  public static void info(final Class<?> clazz, final String message) {
    final String name = clazz.getName();
    info(name, message);
  }

  public static void info(final Class<?> clazz, final String message, final Throwable e) {
    final String name = clazz.getName();
    info(name, message, e);
  }

  public static void info(final Class<?> clazz, final Throwable e) {
    final String message = e.getMessage();
    info(clazz, message, e);
  }

  public static void info(final Object object, final String message) {
    final Class<?> clazz = object.getClass();
    info(clazz, message);
  }

  public static void info(final String name, final String message) {
    final Logger logger = logger(name);
    logger.info(message);
  }

  public static void info(final String name, final String message, final Throwable e) {
    final StringBuilder messageText = new StringBuilder();
    final Throwable logException = getMessageAndException(messageText, message, e);

    final Logger logger = logger(name);
    logger.info(messageText.toString(), logException);
  }

  public static boolean isDebugEnabled(final Class<?> logCateogory) {
    final Logger logger = LoggerFactory.getLogger(logCateogory);
    return logger.isDebugEnabled();
  }

  public static boolean isDebugEnabled(final Object logCateogory) {
    final Class<?> logClass = logCateogory.getClass();
    return isDebugEnabled(logClass);
  }

  public static Logger logger(final Class<?> clazz) {
    final String name = clazz.getName();
    return logger(name);
  }

  public static Logger logger(final Object object) {
    if (object == null) {
      return logger(Logger.ROOT_LOGGER_NAME);
    } else if (object instanceof final CharSequence chars) {
      return logger(chars.toString());
    } else if (object instanceof final Class<?> clazz) {
      return logger(clazz);
    } else {
      final Class<? extends Object> clazz = object.getClass();
      return logger(clazz);
    }
  }

  public static Logger logger(final String name) {
    return LoggerFactory.getLogger(name);
  }

  public static void setUncaughtExceptionHandler() {
    setUncaughtExceptionHandler(Thread.class);
  }

  public static void setUncaughtExceptionHandler(final Class<?> logClass) {
    Thread.currentThread()
      .setUncaughtExceptionHandler((thread, exception) -> {
        Logs.error(logClass, exception);
      });
  }

  public static void warn(final Class<?> clazz, final String message) {
    final String name = clazz.getName();
    warn(name, message);
  }

  public static void warn(final Class<?> clazz, final String message, final Throwable e) {
    final String name = clazz.getName();
    warn(name, message, e);
  }

  public static void warn(final Class<?> clazz, final Throwable e) {
    final String name = clazz.getName();
    warn(name, e);
  }

  public static void warn(final Object object, final String message) {
    final Class<?> clazz = object.getClass();
    warn(clazz, message);
  }

  public static void warn(final Object object, final String message, final Throwable e) {
    final Class<?> clazz = object.getClass();
    warn(clazz, message, e);
  }

  public static void warn(final Object object, final Throwable e) {
    final Class<?> clazz = object.getClass();
    warn(clazz, e);
  }

  public static void warn(final String name, final String message) {
    final Logger logger = logger(name);
    logger.warn(message);
  }

  public static void warn(final String name, final String message, final Throwable e) {
    final StringBuilder messageText = new StringBuilder();
    final Throwable logException = getMessageAndException(messageText, message, e);

    final Logger logger = logger(name);
    logger.warn(messageText.toString(), logException);
  }

  public static void warn(final String name, final Throwable e) {
    final String message = e.getMessage();
    warn(name, message, e);
  }

}
