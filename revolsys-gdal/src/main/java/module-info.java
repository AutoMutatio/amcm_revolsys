module com.revolsys.gdal {
  requires java.base;

  requires java.desktop;

  requires java.sql;

  requires jakarta.annotation;

  requires gdal.jni;

  requires com.revolsys.core;

  exports com.revolsys.gdal;
}
