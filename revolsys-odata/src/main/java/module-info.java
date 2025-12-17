module com.revolsys.odata {

  requires java.xml;

  requires java.sql;

  requires jakarta.servlet;

  requires com.revolsys.core;

  requires org.apache.commons.codec;

  exports com.revolsys.odata.model;

  exports com.revolsys.odata.service.processor;

  exports org.apache.olingo.commons.api;

  exports org.apache.olingo.commons.api.edm;

  exports org.apache.olingo.commons.api.edm.provider;

  exports org.apache.olingo.commons.api.format;

  exports org.apache.olingo.commons.api.http;

  exports org.apache.olingo.server.api;

  exports org.apache.olingo.server.api.uri;

  exports org.apache.olingo.server.core.uri;

  exports org.apache.olingo.commons.core.edm;

}
