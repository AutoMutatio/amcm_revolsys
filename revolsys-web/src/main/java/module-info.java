module com.revolsys.web {

  requires java.desktop;

  requires java.management;

  requires java.sql;

  requires jakarta.servlet;

  requires static jakarta.websocket;

  requires org.apache.commons.logging;

  requires spring.beans;

  requires spring.core;

  requires spring.web;

  requires com.revolsys.core;

  exports com.revolsys.rest;

  exports com.revolsys.web;

  exports com.revolsys.websocket;

  exports com.revolsys.web.converter;

  exports com.revolsys.web.filter;

  exports com.revolsys.web.listener;

  exports com.revolsys.websocket.json;
}
