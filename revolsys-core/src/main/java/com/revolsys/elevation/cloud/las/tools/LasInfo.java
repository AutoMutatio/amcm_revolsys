package com.revolsys.elevation.cloud.las.tools;

import com.revolsys.collection.json.JsonWriter;
import com.revolsys.collection.map.MapEx;
import com.revolsys.elevation.cloud.PointCloud;
import com.revolsys.elevation.cloud.las.LasPointCloud;
import com.revolsys.record.io.BufferedWriterEx;
import com.revolsys.spring.resource.PathResource;
import com.revolsys.util.BaseCloseable;

public class LasInfo implements Runnable, BaseCloseable {

  public static void main(final String... args) {
    final String fileName = args[0];
    try (
      final LasInfo lasInfo = new LasInfo(fileName)) {
      lasInfo.run();
    }
  }

  private LasPointCloud pointCloud;

  public LasInfo(final String fileName) {
    this.pointCloud = PointCloud.newPointCloud(new PathResource(fileName));
  }

  @Override
  public void close() {
    final LasPointCloud pointCloud = this.pointCloud;
    if (pointCloud != null) {
      this.pointCloud = null;
      pointCloud.close();
    }
  }

  @Override
  public void run() {
    final MapEx lasInfo = this.pointCloud.toMap();

    try (
      var writer = BufferedWriterEx.forStream(System.out);
      JsonWriter jsonWriter = new JsonWriter(writer, true);) {
      jsonWriter.write(lasInfo);
    }
  }
}
