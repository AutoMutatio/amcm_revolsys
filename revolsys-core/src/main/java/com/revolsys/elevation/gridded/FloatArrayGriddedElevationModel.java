package com.revolsys.elevation.gridded;

import com.revolsys.geometry.model.BoundingBox;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.geometry.model.editor.LineStringEditor;
import com.revolsys.grid.FloatArrayGrid;

public class FloatArrayGriddedElevationModel extends FloatArrayGrid
  implements GriddedElevationModel {

  public FloatArrayGriddedElevationModel(final double x, final double y, final int gridWidth,
    final int gridHeight, final double gridCellWidth, final double gridCellHeight,
    final float[] values) {
    super(x, y, gridWidth, gridHeight, gridCellWidth, gridCellHeight, values);
  }

  public FloatArrayGriddedElevationModel(final double x, final double y, final int gridWidth,
    final int gridHeight, final double gridCellSize, final float[] values) {
    super(x, y, gridWidth, gridHeight, gridCellSize, values);
  }

  public FloatArrayGriddedElevationModel(final GeometryFactory geometryFactory,
    final BoundingBox boundingBox, final int gridWidth, final int gridHeight,
    final double gridCellWidth, final double gridCellHeight, final float[] values) {
    super(geometryFactory, boundingBox, gridWidth, gridHeight, gridCellWidth, gridCellHeight,
      values);
  }

  public FloatArrayGriddedElevationModel(final GeometryFactory geometryFactory,
    final BoundingBox boundingBox, final int gridWidth, final int gridHeight,
    final double gridCellSize, final float[] values) {
    super(geometryFactory, boundingBox, gridWidth, gridHeight, gridCellSize, values);
  }

  public FloatArrayGriddedElevationModel(final GeometryFactory geometryFactory, final double x,
    final double y, final int gridWidth, final int gridHeight, final double gridCellSize) {
    super(geometryFactory, x, y, gridWidth, gridHeight, gridCellSize);
  }

  public FloatArrayGriddedElevationModel(final GeometryFactory geometryFactory, final double x,
    final double y, final int gridWidth, final int gridHeight, final double gridCellWidth,
    final double gridCellHeight) {
    super(geometryFactory, x, y, gridWidth, gridHeight, gridCellWidth, gridCellHeight);
  }

  public FloatArrayGriddedElevationModel(final GeometryFactory geometryFactory, final double x,
    final double y, final int gridWidth, final int gridHeight, final double gridCellWidth,
    final double gridCellHeight, final float[] values) {
    super(geometryFactory, x, y, gridWidth, gridHeight, gridCellWidth, gridCellHeight, values);
  }

  public FloatArrayGriddedElevationModel(final GeometryFactory geometryFactory, final double x,
    final double y, final int gridWidth, final int gridHeight, final double gridCellSize,
    final float[] values) {
    super(geometryFactory, x, y, gridWidth, gridHeight, gridCellSize, values);
  }

  @Override
  public LineStringEditor getNullBoundaryPoints() {
    final GeometryFactory geometryFactory = getGeometryFactory();
    final LineStringEditor points = new LineStringEditor(geometryFactory);
    final double minX = getGridMinX();
    final double minY = getGridMinY();

    final double gridCellWidth = getGridCellWidth();
    final double gridCellHeight = getGridCellHeight();
    final int gridHeight = getGridHeight();
    final int gridWidth = getGridWidth();
    final float[] elevations = this.cells;
    int index = 0;
    final int[] offsets = {
      -1, 0, 1
    };
    for (int gridY = 0; gridY < gridHeight; gridY++) {
      for (int gridX = 0; gridX < gridWidth; gridX++) {
        final float elevation = elevations[index];
        if (!Double.isFinite(elevation)) {
          int countZ = 0;
          double sumZ = 0;
          for (final int offsetY : offsets) {
            if (!(gridY == 0 && offsetY == -1 || gridY == gridHeight - 1 && offsetY == 1)) {
              final int offsetIndex = index + offsetY * gridWidth;
              for (final int offsetX : offsets) {
                if (!(gridX == 0 && offsetX == -1 || gridX == gridWidth - 1 && offsetX == 1)) {
                  final float elevationNeighbour = elevations[offsetIndex + offsetX];
                  if (Double.isFinite(elevationNeighbour)) {
                    sumZ += elevationNeighbour;
                    countZ++;
                  }
                }
              }
            }
          }

          if (countZ > 0) {
            final double x = minX + gridCellWidth * gridX;
            final double y = minY + gridCellHeight * gridY;
            final double z = sumZ / countZ;
            points.appendVertex(x, y, z);
          }
        }
        index++;
      }
    }
    return points;
  }

  @Override
  public FloatArrayGriddedElevationModel newGrid(final GeometryFactory geometryFactory,
    final double x, final double y, final int width, final int height, final double cellSize) {
    return new FloatArrayGriddedElevationModel(geometryFactory, x, y, width, height, cellSize);
  }

  @Override
  public FloatArrayGriddedElevationModel newGrid(final int gridWidth, final int gridHeight,
    final double gridCellWidth, final double gridCellHeight, final float[] newValues) {
    final GeometryFactory geometryFactory = getGeometryFactory();
    final BoundingBox boundingBox = getBoundingBox();
    return new FloatArrayGriddedElevationModel(geometryFactory, boundingBox, gridWidth, gridHeight,
      gridCellWidth, gridCellHeight, newValues);
  }

  @Override
  public FloatArrayGriddedElevationModel resample(final int newGridCellSize) {
    return (FloatArrayGriddedElevationModel)super.resample(newGridCellSize);
  }
}
