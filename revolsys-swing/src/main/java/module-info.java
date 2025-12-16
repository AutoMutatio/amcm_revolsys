module com.revolsys.swing {

  requires java.compiler;

  requires java.desktop;

  requires java.sql;

  requires java.net.http;

  requires java.scripting;

  requires jakarta.annotation;

  requires jakarta.activation;

  requires ch.qos.logback.classic;

  requires ch.qos.logback.core;

  requires tech.units.indriya;

  requires org.apache.commons.collections4;

  requires com.revolsys.core;

  requires com.revolsys.jocl;

  requires spring.boot;

  requires jocl;

  requires swingx.all;

  requires batik.all;

  requires jsyntaxpane;

  requires org.apache.xmpbox;

  requires commons.collections;

  requires org.apache.fontbox;

  exports com.revolsys.swing;

  exports com.revolsys.swing.action;

  exports com.revolsys.swing.border;

  exports com.revolsys.swing.builder;

  exports com.revolsys.swing.component;

  exports com.revolsys.swing.desktop;

  exports com.revolsys.swing.dnd;

  exports com.revolsys.swing.events;

  exports com.revolsys.swing.field;

  exports com.revolsys.swing.i18n;

  exports com.revolsys.swing.io;

  exports com.revolsys.swing.layout;

  exports com.revolsys.swing.list;

  exports com.revolsys.swing.listener;

  exports com.revolsys.swing.logging;

  exports com.revolsys.swing.map;

  exports com.revolsys.swing.menu;

  exports com.revolsys.swing.parallel;

  exports com.revolsys.swing.preferences;

  exports com.revolsys.swing.scripting;

  exports com.revolsys.swing.table;

  exports com.revolsys.swing.toolbar;

  exports com.revolsys.swing.tree;

  exports com.revolsys.swing.undo;

  exports com.revolsys.swing.action.enablecheck;

  exports com.revolsys.swing.dnd.transferable;

  exports com.revolsys.swing.dnd.transferhandler;

  exports com.revolsys.swing.list.filter;

  exports com.revolsys.swing.list.renderer;

  exports com.revolsys.swing.map.border;

  exports com.revolsys.swing.map.component;

  exports com.revolsys.swing.map.form;

  exports com.revolsys.swing.map.layer;

  exports com.revolsys.swing.map.list;

  exports com.revolsys.swing.map.listener;

  exports com.revolsys.swing.map.overlay;

  exports com.revolsys.swing.map.print;

  exports com.revolsys.swing.map.view;

  exports com.revolsys.swing.map.layer.arcgisrest;

  exports com.revolsys.swing.map.layer.bing;

  exports com.revolsys.swing.map.layer.elevation;

  exports com.revolsys.swing.map.layer.grid;

  exports com.revolsys.swing.map.layer.mapguide;

  exports com.revolsys.swing.map.layer.menu;

  exports com.revolsys.swing.map.layer.ogc.wms;

  exports com.revolsys.swing.map.layer.openstreetmap;

  exports com.revolsys.swing.map.layer.pointcloud;

  exports com.revolsys.swing.map.layer.raster;

  exports com.revolsys.swing.map.layer.record;

  exports com.revolsys.swing.map.layer.tile;

  exports com.revolsys.swing.map.layer.webmercatortilecache;

  exports com.revolsys.swing.map.layer.elevation.gridded;

  exports com.revolsys.swing.map.layer.elevation.tin;

  exports com.revolsys.swing.map.layer.elevation.gridded.renderer;

  exports com.revolsys.swing.map.layer.elevation.gridded.renderer.jocl;

  exports com.revolsys.swing.map.layer.record.component;

  exports com.revolsys.swing.map.layer.record.renderer;

  exports com.revolsys.swing.map.layer.record.style;

  exports com.revolsys.swing.map.layer.record.table;

  exports com.revolsys.swing.map.layer.record.component.recordmerge;

  exports com.revolsys.swing.map.layer.record.renderer.shape;

  exports com.revolsys.swing.map.layer.record.style.marker;

  exports com.revolsys.swing.map.layer.record.style.panel;

  exports com.revolsys.swing.map.layer.record.table.model;

  exports com.revolsys.swing.map.layer.record.table.predicate;

  exports com.revolsys.swing.map.overlay.record;

  exports com.revolsys.swing.map.overlay.record.geometryeditor;

  exports com.revolsys.swing.map.view.graphics;

  exports com.revolsys.swing.map.view.pdf;

  exports com.revolsys.swing.table.counts;

  exports com.revolsys.swing.table.editor;

  exports com.revolsys.swing.table.filter;

  exports com.revolsys.swing.table.geometry;

  exports com.revolsys.swing.table.highlighter;

  exports com.revolsys.swing.table.json;

  exports com.revolsys.swing.table.lambda;

  exports com.revolsys.swing.table.object;

  exports com.revolsys.swing.table.predicate;

  exports com.revolsys.swing.table.record;

  exports com.revolsys.swing.table.renderer;

  exports com.revolsys.swing.table.selection;

  exports com.revolsys.swing.table.lambda.column;

  exports com.revolsys.swing.table.record.editor;

  exports com.revolsys.swing.table.record.filter;

  exports com.revolsys.swing.table.record.model;

  exports com.revolsys.swing.table.record.renderer;

  exports com.revolsys.swing.tree.dnd;

  exports com.revolsys.swing.tree.node;

  exports com.revolsys.swing.tree.node.coordinatesystem;

  exports com.revolsys.swing.tree.node.file;

  exports com.revolsys.swing.tree.node.layer;

  exports com.revolsys.swing.tree.node.record;
}
