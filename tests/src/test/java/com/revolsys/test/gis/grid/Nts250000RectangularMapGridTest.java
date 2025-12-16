package com.revolsys.test.gis.grid;

import com.revolsys.gis.grid.GridUtil;
import com.revolsys.gis.grid.Nts250000RectangularMapGrid;
import com.revolsys.gis.grid.NtsConstants;
import com.revolsys.gis.grid.RectangularMapGrid;

public class Nts250000RectangularMapGridTest extends Nts1000000RectangularMapGridTest {
  private static final RectangularMapGrid GRID = new Nts250000RectangularMapGrid();

  private static final double TILE_HEIGHT = NtsConstants.HEIGHT_250000;

  private static final double TILE_WIDTH = NtsConstants.WIDTH_250000;

  @Override
  protected void doTestNts1000000ByName(final String parentTileName, final double parentLon,
    final double parentLat) {
    for (char letter = 'A'; letter <= 'P'; letter++) {
      final double lon = parentLon - GridUtil.getLetter16Col(letter) * TILE_WIDTH;
      final double lat = parentLat + GridUtil.getLetter16Row(letter) * TILE_HEIGHT;

      final String tileName = parentTileName + letter;

      doTestNts250000ByName(tileName, lon, lat);
    }

  }

  protected void doTestNts250000ByName(final String parentTileName, final double parentLon,
    final double parentLat) {
    checkTileByName(GRID, parentTileName, parentLon, parentLat, TILE_WIDTH, TILE_HEIGHT);
  }
}
