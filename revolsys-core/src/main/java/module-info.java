module com.revolsys.core {

  requires java.base;

  requires java.desktop;

  requires java.net.http;

  requires java.sql;

  requires java.prefs;

  requires java.management;

  requires ch.qos.logback.classic;

  requires ch.qos.logback.core;

  requires jakarta.xml.bind;

  requires java.measure;

  requires jakarta.annotation;

  requires jakarta.servlet;

  requires tech.units.indriya;

  requires org.apache.commons.logging;

  requires org.apache.commons.imaging;

  requires org.docx4j.core;

  requires org.apache.xmpbox;

  requires org.apache.pdfbox;

  requires org.apache.pdfbox.io;

  requires org.apache.httpcomponents.httpcore;

  requires org.apache.httpcomponents.httpclient;

  requires com.ctc.wstx;

  requires org.apache.commons.collections4;

  requires org.slf4j;

  requires spring.aop;

  requires spring.context;

  requires spring.beans;

  requires spring.core;

  requires spring.expression;

  requires spring.jdbc;

  requires spring.tx;

  requires net.sf.jsqlparser;

  requires akiban.sql.parser;

  requires org.apache.commons.lang3;

  requires org.apache.commons.jexl3;

  exports com.revolsys.awt;

  exports com.revolsys.beans;

  exports com.revolsys.collection;

  exports com.revolsys.comparator;

  exports com.revolsys.connection;

  exports com.revolsys.crypto;

  exports com.revolsys.data;

  exports com.revolsys.date;

  exports com.revolsys.exception;

  exports com.revolsys.function;

  exports com.revolsys.http;

  exports com.revolsys.i18n;

  exports com.revolsys.io;

  exports com.revolsys.jdbc;

  exports com.revolsys.jmx;

  exports com.revolsys.log;

  exports com.revolsys.logging;

  exports com.revolsys.math;

  exports com.revolsys.maven;

  exports com.revolsys.net;

  exports com.revolsys.number;

  exports com.revolsys.parallel;

  exports com.revolsys.predicate;

  exports com.revolsys.process;

  exports com.revolsys.properties;

  exports com.revolsys.propertyeditor;

  exports com.revolsys.raster;

  exports com.revolsys.spring;

  exports com.revolsys.transaction;

  exports com.revolsys.util;

  exports com.revolsys.visitor;

  exports com.revolsys.webservice;

  exports com.revolsys.beans.propertyeditor;

  exports com.revolsys.collection.bplus;

  exports com.revolsys.collection.collection;

  exports com.revolsys.collection.iterator;

  exports com.revolsys.collection.json;

  exports com.revolsys.collection.list;

  exports com.revolsys.collection.map;

  exports com.revolsys.collection.range;

  exports com.revolsys.collection.set;

  exports com.revolsys.collection.value;

  exports com.revolsys.connection.file;

  exports com.revolsys.csformat.geoid.byn;

  exports com.revolsys.csformat.geoid.ngabgh;

  exports com.revolsys.csformat.geoid.usngsbin;

  exports com.revolsys.csformat.gridshift.gsb;

  exports com.revolsys.csformat.gridshift.nadcon5;

  exports com.revolsys.data.exception;

  exports com.revolsys.data.identifier;

  exports com.revolsys.data.refresh;

  exports com.revolsys.data.type;

  exports com.revolsys.elevation.cloud;

  exports com.revolsys.elevation.gridded;

  exports com.revolsys.elevation.tin;

  exports com.revolsys.elevation.cloud.las;

  exports com.revolsys.elevation.cloud.las.pointformat;

  exports com.revolsys.elevation.cloud.las.tools;

  exports com.revolsys.elevation.cloud.las.zip;

  exports com.revolsys.elevation.cloud.las.zip.context;

  exports com.revolsys.elevation.cloud.las.zip.v1;

  exports com.revolsys.elevation.cloud.las.zip.v2;

  exports com.revolsys.elevation.cloud.las.zip.v3;

  exports com.revolsys.elevation.gridded.esriascii;

  exports com.revolsys.elevation.gridded.esrifloatgrid;

  exports com.revolsys.elevation.gridded.img;

  exports com.revolsys.elevation.gridded.rasterizer;

  exports com.revolsys.elevation.gridded.scaledint;

  exports com.revolsys.elevation.gridded.usgsdem;

  exports com.revolsys.elevation.gridded.rasterizer.gradient;

  exports com.revolsys.elevation.gridded.scaledint.compressed;

  exports com.revolsys.elevation.tin.compactbinary;

  exports com.revolsys.elevation.tin.quadedge;

  exports com.revolsys.elevation.tin.tin;

  exports com.revolsys.elevation.tin.quadedge.intscale;

  exports com.revolsys.geometry.algorithm;

  exports com.revolsys.geometry.densify;

  exports com.revolsys.geometry.dissolve;

  exports com.revolsys.geometry.edgegraph;

  exports com.revolsys.geometry.event;

  exports com.revolsys.geometry.filter;

  exports com.revolsys.geometry.geoid;

  exports com.revolsys.geometry.geomgraph;

  exports com.revolsys.geometry.graph;

  exports com.revolsys.geometry.index;

  exports com.revolsys.geometry.io;

  exports com.revolsys.geometry.math;

  exports com.revolsys.geometry.model;

  exports com.revolsys.geometry.noding;

  exports com.revolsys.geometry.operation;

  exports com.revolsys.geometry.planargraph;

  exports com.revolsys.geometry.precision;

  exports com.revolsys.geometry.simplify;

  exports com.revolsys.geometry.util;

  exports com.revolsys.geometry.wkb;

  exports com.revolsys.geometry.algorithm.distance;

  exports com.revolsys.geometry.algorithm.linematch;

  exports com.revolsys.geometry.algorithm.locate;

  exports com.revolsys.geometry.algorithm.match;

  exports com.revolsys.geometry.coordinatesystem.io;

  exports com.revolsys.geometry.coordinatesystem.model;

  exports com.revolsys.geometry.coordinatesystem.operation;

  exports com.revolsys.geometry.coordinatesystem.model.datum;

  exports com.revolsys.geometry.coordinatesystem.model.systems;

  exports com.revolsys.geometry.coordinatesystem.model.unit;

  exports com.revolsys.geometry.coordinatesystem.operation.gridshift;

  exports com.revolsys.geometry.coordinatesystem.operation.projection;

  exports com.revolsys.geometry.geomgraph.index;

  exports com.revolsys.geometry.graph.algorithm;

  exports com.revolsys.geometry.graph.attribute;

  exports com.revolsys.geometry.graph.comparator;

  exports com.revolsys.geometry.graph.event;

  exports com.revolsys.geometry.graph.filter;

  exports com.revolsys.geometry.graph.geometry;

  exports com.revolsys.geometry.graph.linemerge;

  exports com.revolsys.geometry.graph.linestring;

  exports com.revolsys.geometry.graph.process;

  exports com.revolsys.geometry.graph.visitor;

  exports com.revolsys.geometry.index.bintree;

  exports com.revolsys.geometry.index.chain;

  exports com.revolsys.geometry.index.intervalrtree;

  exports com.revolsys.geometry.index.kdtree;

  exports com.revolsys.geometry.index.quadtree;

  exports com.revolsys.geometry.index.rstartree;

  exports com.revolsys.geometry.index.rtree;

  exports com.revolsys.geometry.index.strtree;

  exports com.revolsys.geometry.index.sweepline;

  exports com.revolsys.geometry.index.visitor;

  exports com.revolsys.geometry.model.coordinates;

  exports com.revolsys.geometry.model.editor;

  exports com.revolsys.geometry.model.impl;

  exports com.revolsys.geometry.model.metrics;

  exports com.revolsys.geometry.model.prep;

  exports com.revolsys.geometry.model.segment;

  exports com.revolsys.geometry.model.util;

  exports com.revolsys.geometry.model.vertex;

  exports com.revolsys.geometry.model.coordinates.comparator;

  exports com.revolsys.geometry.model.coordinates.filter;

  exports com.revolsys.geometry.model.coordinates.list;

  exports com.revolsys.geometry.noding.snapround;

  exports com.revolsys.geometry.operation.buffer;

  exports com.revolsys.geometry.operation.distance;

  exports com.revolsys.geometry.operation.distance3d;

  exports com.revolsys.geometry.operation.linemerge;

  exports com.revolsys.geometry.operation.overlay;

  exports com.revolsys.geometry.operation.polygonize;

  exports com.revolsys.geometry.operation.predicate;

  exports com.revolsys.geometry.operation.relate;

  exports com.revolsys.geometry.operation.simple;

  exports com.revolsys.geometry.operation.union;

  exports com.revolsys.geometry.operation.valid;

  exports com.revolsys.geometry.operation.buffer.validate;

  exports com.revolsys.geometry.operation.overlay.snap;

  exports com.revolsys.geometry.operation.overlay.validate;

  exports com.revolsys.geometry.planargraph.algorithm;

  exports com.revolsys.gis.converter;

  exports com.revolsys.gis.grid;

  exports com.revolsys.gis.grid.filter;

  exports com.revolsys.gis.parallel;

  exports com.revolsys.gis.parser;

  exports com.revolsys.gis.wms;

  exports com.revolsys.gis.converter.process;

  exports com.revolsys.gis.wms.capabilities;

  exports com.revolsys.io.channels;

  exports com.revolsys.io.endian;

  exports com.revolsys.io.file;

  exports com.revolsys.io.filter;

  exports com.revolsys.io.map;

  exports com.revolsys.io.page;

  exports com.revolsys.io.stream;

  exports com.revolsys.jdbc.exception;

  exports com.revolsys.jdbc.field;

  exports com.revolsys.jdbc.io;

  exports com.revolsys.jdbc.data.model.filter;

  exports com.revolsys.math.arithmeticcoding;

  exports com.revolsys.math.matrix;

  exports com.revolsys.net.http;

  exports com.revolsys.net.oauth;

  exports com.revolsys.net.urlcache;

  exports com.revolsys.net.http.exception;

  exports com.revolsys.net.protocol.folderconnection;

  exports com.revolsys.parallel.channel;

  exports com.revolsys.parallel.process;

  exports com.revolsys.parallel.channel.store;

  exports com.revolsys.raster.commonsimaging;

  exports com.revolsys.raster.imagio;

  exports com.revolsys.raster.io.format.jpg;

  exports com.revolsys.raster.io.format.pdf;

  exports com.revolsys.raster.io.format.tiff;

  exports com.revolsys.raster.io.format.tiff.builder;

  exports com.revolsys.raster.io.format.tiff.cmd;

  exports com.revolsys.raster.io.format.tiff.code;

  exports com.revolsys.raster.io.format.tiff.compression;

  exports com.revolsys.raster.io.format.tiff.image;

  exports com.revolsys.raster.io.format.tiff.directory.entry;

  exports com.revolsys.reactive.chars;

  exports com.revolsys.record;

  exports com.revolsys.record.code;

  exports com.revolsys.record.comparator;

  exports com.revolsys.record.filter;

  exports com.revolsys.record.io;

  exports com.revolsys.record.property;

  exports com.revolsys.record.query;

  exports com.revolsys.record.schema;

  exports com.revolsys.record.io.format;

  exports com.revolsys.record.io.format.csv;

  exports com.revolsys.record.io.format.directory;

  exports com.revolsys.record.io.format.geojson;

  exports com.revolsys.record.io.format.gml;

  exports com.revolsys.record.io.format.gpx;

  exports com.revolsys.record.io.format.html;

  exports com.revolsys.record.io.format.json;

  exports com.revolsys.record.io.format.kml;

  exports com.revolsys.record.io.format.mapguide;

  exports com.revolsys.record.io.format.moep;

  exports com.revolsys.record.io.format.odata;

  exports com.revolsys.record.io.format.saif;

  exports com.revolsys.record.io.format.scaledint;

  exports com.revolsys.record.io.format.shp;

  exports com.revolsys.record.io.format.tcx;

  exports com.revolsys.record.io.format.tsv;

  exports com.revolsys.record.io.format.vrt;

  exports com.revolsys.record.io.format.wkt;

  exports com.revolsys.record.io.format.xbase;

  exports com.revolsys.record.io.format.xlsx;

  exports com.revolsys.record.io.format.xml;

  exports com.revolsys.record.io.format.zip;

  exports com.revolsys.record.io.format.esri.rest;

  exports com.revolsys.record.io.format.esri.gdb.xml;

  exports com.revolsys.record.io.format.esri.gdb.xml.model;

  exports com.revolsys.record.io.format.esri.gdb.xml.type;

  exports com.revolsys.record.io.format.esri.gdb.xml.model.enums;

  exports com.revolsys.record.io.format.esri.rest.map;

  exports com.revolsys.record.io.format.gml.type;

  exports com.revolsys.record.io.format.openstreetmap.model;

  exports com.revolsys.record.io.format.saif.geometry;

  exports com.revolsys.record.io.format.saif.util;

  exports com.revolsys.record.io.format.xml.stax;

  exports com.revolsys.record.io.format.xml.wadl;

  exports com.revolsys.record.query.functions;

  exports com.revolsys.record.query.parser;

  exports com.revolsys.spring.config;

  exports com.revolsys.spring.factory;

  exports com.revolsys.spring.resource;

  exports com.revolsys.spring.util;

  exports com.revolsys.util.concurrent;

  exports com.revolsys.util.count;

  exports com.revolsys.util.metrics;
}
