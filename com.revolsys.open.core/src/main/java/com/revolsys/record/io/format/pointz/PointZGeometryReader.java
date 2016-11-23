package com.revolsys.record.io.format.pointz;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import com.revolsys.collection.iterator.AbstractIterator;
import com.revolsys.geometry.io.GeometryReader;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.io.FileUtil;
import com.revolsys.io.endian.EndianInput;
import com.revolsys.io.endian.EndianInputStream;
import com.revolsys.spring.resource.Resource;
import com.revolsys.util.Exceptions;

public class PointZGeometryReader extends AbstractIterator<Geometry> implements GeometryReader {
  private final Resource resource;

  private EndianInput in;

  private double scaleXy;

  private double scaleZ;

  private GeometryFactory geometryFactory;

  public PointZGeometryReader(final Resource resource) {
    this.resource = resource;
  }

  @Override
  protected void closeDo() {
    super.closeDo();
    FileUtil.closeSilent(this.in);
    this.in = null;
  }

  @Override
  public GeometryFactory getGeometryFactory() {
    return this.geometryFactory;
  }

  @Override
  protected Geometry getNext() {
    try {
      final int xInt = this.in.readInt();
      if (xInt == -1) {
        throw new NoSuchElementException();
      } else {
        final int yInt = this.in.readInt();
        final int zInt = this.in.readInt();
        final double x = xInt / this.scaleXy;
        final double y = yInt / this.scaleXy;
        final double z = zInt / this.scaleZ;
        return this.geometryFactory.point(x, y, z);
      }
    } catch (final EOFException e) {
      throw new NoSuchElementException();
    } catch (final IOException e) {
      throw Exceptions.wrap("Error reading: " + PointZGeometryReader.this.resource, e);
    }
  }

  @Override
  protected void initDo() {
    try {
      super.initDo();
      this.in = this.resource.newBufferedInputStream(EndianInputStream::new);
      final String fileType = this.in.readString(6, StandardCharsets.UTF_8);
      if (!PointZIoFactory.FILE_TYPE_POINTZ.equals(fileType)) {
        throw new IllegalArgumentException("File must start with the text: "
          + PointZIoFactory.FILE_TYPE_POINTZ + " not " + fileType);
      }
      final byte versionByte1 = this.in.readByte();
      final byte versionByte2 = this.in.readByte();
      if (versionByte1 == ' ' && versionByte2 == ' ') {
        String version;
        final byte[] bytes = new byte[8];
        bytes[0] = versionByte1;
        bytes[1] = versionByte2;
        final int readCount = this.in.read(bytes, 2, 6);
        int i = 2;
        for (; i < readCount; i++) {
          final byte character = bytes[i];
          if (character == 0) {
            break;
          }
        }
        version = new String(bytes, 0, i, StandardCharsets.UTF_8);
      } else {
        final short version = (short)((versionByte1 << 8) + (versionByte2 << 0));
      }
      final int coordinateSystemId = this.in.readInt();
      this.scaleXy = this.in.readDouble();

      this.scaleZ = this.in.readDouble();
      this.geometryFactory = GeometryFactory.fixed(coordinateSystemId, this.scaleXy, this.scaleZ);
    } catch (final IOException e) {
      try {
        throw Exceptions.wrap("Error opening: " + PointZGeometryReader.this.resource, e);
      } finally {
        close();
      }
    }
  }
}