package edu.colorado.cires.wod.iquodqc.common.interpolation;

import org.apache.commons.math3.analysis.interpolation.TricubicInterpolator;

class HoleInterpolator {

  private final double[] x;
  private final double[] y;
  private final double[] z;
  private final double[][][] values;

  public HoleInterpolator(double[] x, double[] y, double[] z, double[][][] values) {
    this.x = x;
    this.y = y;
    this.z = z;
    this.values = values;
  }

  public double[] getX() {
    return x;
  }

  public double[] getY() {
    return y;
  }

  public double[] getZ() {
    return z;
  }

  public double[][][] getValues() {
    return values;
  }

  public double interpolate(InterpolationPoint point) {
    return new TricubicInterpolator().interpolate(this.x, this.y, this.z, values).value(point.getX(), point.getY(), point.getZ());
  }
}
