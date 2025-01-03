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
package com.revolsys.geometry.model;

/**
 *  Constants representing the different topological locations
 *  which can occur in a {@link Geometry}.
 *  The constants are also used as the row and column indices
 *  of DE-9IM {@link IntersectionMatrix}es.
 *
 *@version 1.7
 */
public enum Location {
  /**
               * The location value for the boundary of a geometry.
               * Also, DE-9IM row index of the boundary of the first geometry and column index of
               *  the boundary of the second geometry.
               */
  BOUNDARY(1),
  /**
  * The location value for the exterior of a geometry.
  * Also, DE-9IM row index of the exterior of the first geometry and column index of
  *  the exterior of the second geometry.
  */
  EXTERIOR(2),
  /**
  * The location value for the interior of a geometry.
  * Also, DE-9IM row index of the interior of the first geometry and column index of
  *  the interior of the second geometry.
  */
  INTERIOR(0),
  /**
  *  Used for uninitialized location values.
  */
  NONE(-1);

  /**
   *  Converts the location value to a location symbol, for example, <code>EXTERIOR => 'e'</code>
   *  .
   *
   *@param  locationValue  either EXTERIOR, BOUNDARY, INTERIOR or NONE
   *@return                either 'e', 'b', 'i' or '-'
   */
  public static char toLocationSymbol(final Location locationValue) {
    return locationValue.toString().charAt(0);
  }

  private final int index;

  private Location(final int index) {
    this.index = index;
  }

  public int getIndex() {
    return this.index;
  }

  public boolean isBoundary() {
    return this == BOUNDARY;
  }

  public boolean isDisjoint() {
    return isExterior() || isNone();
  }

  public boolean isExterior() {
    return this == EXTERIOR;
  }

  public boolean isInterior() {
    return this == INTERIOR;
  }

  public boolean isIntersects() {
    return isInterior() || isBoundary();
  }

  public boolean isNone() {
    return this == NONE;
  }

  @Override
  public String toString() {
    switch (this) {
      case EXTERIOR:
        return "e";
      case BOUNDARY:
        return "b";
      case INTERIOR:
        return "i";
      default:
        return "-";
    }
  }
}
