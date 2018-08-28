/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */

package com.revolsys.core.test.geometry.test.old.io;

import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.model.impl.PointDouble;
import com.revolsys.geometry.model.impl.PointDoubleXY;
import com.revolsys.geometry.wkb.ParseException;
import com.revolsys.geometry.wkb.WKTReader;

import junit.framework.TestCase;

/**
 * Test for {@link WKTReader}
 *
 * @version 1.7
 */
public class WKTReaderTest extends TestCase {

  private final GeometryFactory geometryFactory = GeometryFactory.fixed2d(0, 1.0, 1.0);

  WKTReader reader = new WKTReader(this.geometryFactory);

  public WKTReaderTest(final String name) {
    super(name);
  }

  private void assertReaderEquals(final String expected, final String sourceWkt)
    throws ParseException {
    final Geometry actualGeometry = this.reader.read(sourceWkt);
    final String actualWkt = actualGeometry.toEwkt();
    assertEquals(expected, actualWkt);
  }

  public void testReadGeometryCollection() throws Exception {

    assertEquals("GEOMETRYCOLLECTION(POINT(10 10),POINT(30 30),LINESTRING(15 15,20 20))",
      this.reader.read("GEOMETRYCOLLECTION (POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))")
        .toEwkt());
    assertEquals("GEOMETRYCOLLECTION(POINT(10 10),LINEARRING EMPTY,LINESTRING(15 15,20 20))",
      this.reader.read("GEOMETRYCOLLECTION(POINT(10 10),LINEARRING EMPTY,LINESTRING(15 15, 20 20))")
        .toEwkt());
    assertReaderEquals(
      "GEOMETRYCOLLECTION(POINT(10 10),LINEARRING(10 10,20 20,30 40,10 10),LINESTRING(15 15,20 20))",
      "GEOMETRYCOLLECTION(POINT(10 10),LINEARRING(10 10,20 20,30 40,10 10),LINESTRING(15 15,20 20))");
    assertEquals("GEOMETRYCOLLECTION EMPTY", this.reader.read("GEOMETRYCOLLECTION EMPTY").toEwkt());
  }

  public void testReadLargeNumbers() throws Exception {
    final GeometryFactory geometryFactory = GeometryFactory.fixed2d(0, 1E9, 1E9);
    final WKTReader reader = new WKTReader(geometryFactory);
    final Geometry point1 = reader.read("POINT(123456789.01234567890 10)");
    final Point point2 = geometryFactory.point(new PointDoubleXY(123456789.01234567890, 10));
    assertEquals(point1.getPoint().getX(), point2.getPoint().getX(), 1E-7);
    assertEquals(point1.getPoint().getY(), point2.getPoint().getY(), 1E-7);
  }

  public void testReadLinearRing() throws Exception {
    try {
      this.reader.read("LINEARRING(10 10,20 20,30 40,10 99)");
    } catch (final IllegalArgumentException e) {
      assertTrue(e.getMessage().indexOf("not form a closed linestring") > -1);
    }

    assertEquals("LINEARRING(10 10,20 20,30 40,10 10)",
      this.reader.read("LINEARRING(10 10,20 20,30 40,10 10)").toEwkt());

    assertEquals("LINEARRING EMPTY", this.reader.read("LINEARRING EMPTY").toEwkt());
  }

  public void testReadLineString() throws Exception {

    assertEquals("LINESTRING(10 10,20 20,30 40)",
      this.reader.read("LINESTRING(10 10,20 20,30 40)").toEwkt());

    assertEquals("LINESTRING EMPTY", this.reader.read("LINESTRING EMPTY").toEwkt());
  }

  public void testReadMultiLineString() throws Exception {

    assertEquals("MULTILINESTRING((10 10,20 20),(15 15,30 15))",
      this.reader.read("MULTILINESTRING((10 10,20 20),(15 15,30 15))").toEwkt());

    assertEquals("LINESTRING EMPTY", this.reader.read("MULTILINESTRING EMPTY").toEwkt());
  }

  public void testReadMultiPoint() throws Exception {

    assertEquals("MULTIPOINT((10 10),(20 20))",
      this.reader.read("MULTIPOINT((10 10),(20 20))").toEwkt());

    assertEquals("POINT EMPTY", this.reader.read("MULTIPOINT EMPTY").toEwkt());
  }

  public void testReadMultiPolygon() throws Exception {

    assertEquals("MULTIPOLYGON(((10 10,10 20,20 20,20 15,10 10)),((60 60,70 70,80 60,60 60)))",
      this.reader
        .read("MULTIPOLYGON(((10 10, 10 20, 20 20, 20 15, 10 10)), ((60 60, 70 70, 80 60, 60 60)))")
        .toEwkt());

    assertEquals("POLYGON EMPTY", this.reader.read("MULTIPOLYGON EMPTY").toEwkt());
  }

  public void testReadNaN() throws Exception {

    assertEquals("POINT(10 10)", this.reader.read("POINT(10 10 NaN)").toEwkt());

    assertEquals("POINT(10 10)", this.reader.read("POINT(10 10 nan)").toEwkt());
    assertEquals("POINT(10 10)", this.reader.read("POINT(10 10 NAN)").toEwkt());
  }

  public void testReadPoint() throws Exception {

    assertEquals("POINT(10 10)", this.reader.read("POINT(10 10)").toEwkt());

    assertEquals("POINT EMPTY", this.reader.read("POINT EMPTY").toEwkt());
  }

  public void testReadPolygon() throws Exception {

    assertEquals("POLYGON((10 10,10 20,20 20,20 15,10 10))",
      this.reader.read("POLYGON((10 10,10 20,20 20,20 15,10 10))").toEwkt());

    assertEquals("POLYGON EMPTY", this.reader.read("POLYGON EMPTY").toEwkt());
  }

  public void testReadZ() throws Exception {
    assertEquals(new PointDouble(1, 2, 3), this.reader.read("POINT(1 2 3)").getPoint());
  }

}
