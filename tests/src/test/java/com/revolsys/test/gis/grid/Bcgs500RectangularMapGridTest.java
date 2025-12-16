package com.revolsys.test.gis.grid;

import com.revolsys.gis.grid.Bcgs500RectangularMapGrid;
import com.revolsys.gis.grid.BcgsConstants;
import com.revolsys.gis.grid.GridUtil;
import com.revolsys.gis.grid.RectangularMapGrid;

public class Bcgs500RectangularMapGridTest extends Bcgs1000RectangularMapGridTest {
  private static final RectangularMapGrid GRID = new Bcgs500RectangularMapGrid();

  private static final double TILE_HEIGHT = BcgsConstants.HEIGHT_500;

  private static final double TILE_WIDTH = BcgsConstants.WIDTH_500;

  @Override
  protected void doTestBcgs1000ByName(final String parentTileName, final double parentLon,
    final double parentLat) {
    for (int number = 1; number <= 4; number++) {
      final double lon = parentLon - GridUtil.getNumberCol4(number) * TILE_WIDTH;
      final double lat = parentLat + GridUtil.getNumberRow4(number) * TILE_HEIGHT;

      final String tileName = parentTileName + "." + number;
      doTestBcgs500ByName(tileName, lon, lat);
    }
  }

  protected void doTestBcgs500ByName(final String parentTileName, final double parentLon,
    final double parentLat) {
    checkTileByName(GRID, parentTileName, parentLon, parentLat, TILE_WIDTH, TILE_HEIGHT);
  }
}
