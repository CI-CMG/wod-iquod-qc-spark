package edu.colorado.cires.wod.iquodqc.common.interpolation;

import org.jetbrains.annotations.NotNull;

class InterpolationPoint implements Comparable<InterpolationPoint> {

  private final int xIndex;
  private final int yIndex;
  private final int zIndex;

  private final double x;
  private final double y;
  private final double z;
  private final double value;

  public InterpolationPoint(int xIndex, int yIndex, int zIndex, double x, double y, double z, double value) {
    this.xIndex = xIndex;
    this.yIndex = yIndex;
    this.zIndex = zIndex;
    this.x = x;
    this.y = y;
    this.z = z;
    this.value = value;
  }

  public int getxIndex() {
    return xIndex;
  }

  public int getyIndex() {
    return yIndex;
  }

  public int getzIndex() {
    return zIndex;
  }

  public double getX() {
    return x;
  }

  public double getY() {
    return y;
  }

  public double getZ() {
    return z;
  }

  public double getValue() {
    return value;
  }

  @Override
  public int compareTo(@NotNull InterpolationPoint o) {
    if (xIndex == o.xIndex) {
      if (yIndex == o.yIndex) {
        if (zIndex == o.zIndex) {
          return Double.compare(value, o.value);
        }
        Integer.compare(zIndex, o.zIndex);
      }
      return Integer.compare(yIndex, o.yIndex);
    }
    return Integer.compare(xIndex, o.xIndex);
  }
}
