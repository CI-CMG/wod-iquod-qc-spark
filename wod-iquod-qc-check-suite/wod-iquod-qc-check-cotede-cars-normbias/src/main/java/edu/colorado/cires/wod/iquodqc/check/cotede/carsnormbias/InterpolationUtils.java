package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;

public class InterpolationUtils {

  public static double interpolate3D(double[][][] data, double[] latVals, double[] lonVals, double[] depthVals, double lat, double lon, double depth) {
    return interpolate1D(
        new double[] {
            interpolate2D(data[0], latVals, lonVals, lat, lon),
            interpolate2D(data[1], latVals, lonVals, lat, lon)
        },
        depthVals,
        depth
    );
  }

  public static double interpolate2D(double[][] data, double[] xValues, double[] yValues, double x, double y) {
    return interpolate1D(
        new double[] {
            
        },
        yValues,
        y
    );
  }

  public static double interpolate1D(double[] data, double[] xValues, double x) {
    return new LinearInterpolator().interpolate(xValues, data)
        .value(x);
  }

}
