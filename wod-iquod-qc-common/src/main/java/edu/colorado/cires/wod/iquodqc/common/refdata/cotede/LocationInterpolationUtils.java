package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import edu.colorado.cires.wod.iquodqc.common.InterpolationUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.analysis.interpolation.PiecewiseBicubicSplineInterpolatingFunction;
import org.apache.commons.math3.analysis.interpolation.PiecewiseBicubicSplineInterpolator;

public final class LocationInterpolationUtils {

  private static final int MIN_NUM_POINTS = 5;

  private static List<DepthAtPosition> getDepths(
      EtopoDataAccessor dataHolder,
      int minIndexLat,
      int maxIndexLat,
      int minIndexLon,
      int maxIndexLon) {

    List<DepthAtPosition> positionDepths = new ArrayList<>();

    double[] latitudes = dataHolder.getLatitudes();
    double[] longitudes = dataHolder.getLongitudes();
    float[][] oceanDepthData = dataHolder.getDepths(minIndexLat, maxIndexLat, minIndexLon, maxIndexLon);

    for (int lonI = 0; lonI < maxIndexLon - minIndexLon; lonI++) {
      for (int latI = 0; latI < maxIndexLat - minIndexLat; latI++) {
        float depth = oceanDepthData[lonI][latI];
        int latIndex = minIndexLat + latI;
        int lonIndex = minIndexLon + lonI;
        if (lonIndex < 0) {
          lonIndex = longitudes.length + lonIndex;
        } else if (lonIndex >= longitudes.length) {
          lonIndex = lonIndex - longitudes.length;
        }

        double lon = longitudes[lonIndex];
        double lat = latitudes[latIndex];

        DepthAtPosition dap = new DepthAtPosition(
            lonIndex,
            latIndex,
            depth,
            lon,
            lat
        );
        positionDepths.add(dap);
      }
    }

    return positionDepths;
  }


  public static double depthInterpolation(EtopoDataAccessor dataHolder, double longitude, double latitude) {

    double[] latitudes = dataHolder.getLatitudes();
    double[] longitudes = dataHolder.getLongitudes();

    int closestLatIndex = InterpolationUtils.closestIndex(latitudes, latitude);
    int closestLonIndex = InterpolationUtils.closestIndex(longitudes, longitude);

    int minIndexLat = Math.max(0, closestLatIndex - MIN_NUM_POINTS);
    int maxIndexLat = Math.min(latitudes.length - 1, closestLatIndex + MIN_NUM_POINTS);
    int minIndexLon = closestLonIndex - MIN_NUM_POINTS;
    int maxIndexLon = closestLonIndex + MIN_NUM_POINTS;

    List<DepthAtPosition> positionDepths = getDepths(dataHolder, minIndexLat, maxIndexLat, minIndexLon, maxIndexLon);

    InterpolatorArrays interpolatorArrays = prepareInterpolator(positionDepths);
    return -1D * interpolate(interpolatorArrays, longitude, latitude);
  }

  private static double interpolate(InterpolatorArrays interpolatorArrays, double lon, double lat) {
    PiecewiseBicubicSplineInterpolatingFunction intFunc = new PiecewiseBicubicSplineInterpolator()
        .interpolate(interpolatorArrays.x, interpolatorArrays.y, interpolatorArrays.values);
    return intFunc.value(lon, lat);
  }

  private static InterpolatorArrays prepareInterpolator(List<DepthAtPosition> positionDepths) {

    Map<List<Integer>, DepthAtPosition> lookup = new HashMap<>();
    positionDepths.sort(Comparator.comparingDouble(DepthAtPosition::getLongitude));
    LinkedHashSet<Integer> xIndexes = new LinkedHashSet<>();
    for (int i = 0; i < positionDepths.size(); i++) {
      DepthAtPosition dap = positionDepths.get(i);
      xIndexes.add(dap.getLonIndex());
      lookup.put(Arrays.asList(dap.getLonIndex(), dap.getLatIndex()), dap);
    }

    LinkedHashSet<Integer> yIndexes = new LinkedHashSet<>();
    positionDepths.sort(Comparator.comparingDouble(DepthAtPosition::getLatitude));
    for (int i = 0; i < positionDepths.size(); i++) {
      DepthAtPosition dap = positionDepths.get(i);
      yIndexes.add(dap.getLatIndex());
    }

    double[] x = new double[xIndexes.size()];
    double[] y = new double[yIndexes.size()];
    double[][] values = new double[x.length][y.length];

    int xi = 0;
    for (int lonIndex : xIndexes) {
      int yi = 0;
      for (int latIndex : yIndexes) {
        DepthAtPosition dap = lookup.get(Arrays.asList(lonIndex, latIndex));
        x[xi] = dap.getLongitude();
        y[yi] = dap.getLatitude();
        values[xi][yi] = dap.getDepth();
        yi++;
      }
      xi++;
    }
    return new InterpolatorArrays(x, y, values);
  }

  private static class InterpolatorArrays {

    public final double[] x;
    public final double[] y;
    public final double[][] values;

    private InterpolatorArrays(double[] x, double[] y, double[][] values) {
      this.x = x;
      this.y = y;
      this.values = values;
    }
  }

  private LocationInterpolationUtils() {

  }
}
