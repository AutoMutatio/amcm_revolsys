module com.revolsys.fgdb {
  requires java.base;

  requires java.desktop;

  requires java.sql;

  requires jakarta.annotation;

  requires filegdb.jni;

  requires com.revolsys.core;

  exports com.revolsys.gis.esri.gdb.file;
}
