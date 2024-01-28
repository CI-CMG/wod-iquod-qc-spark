package edu.colorado.cires.wod.iquodqc.common;

import java.util.Arrays;
import java.util.OptionalDouble;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.geotools.referencing.CRS;
import org.geotools.referencing.GeodeticCalculator;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public final class InterpolationUtils {

  private static final GeodeticCalculator gc;

  static {
    try {
      gc = new GeodeticCalculator(CRS.decode("EPSG:4326"));
    } catch (FactoryException e) {
      throw new RuntimeException("Unable to determine CRS", e);
    }
  }

  public static int closestIndexAssumeSorted(float[] coordinateList, double point) {
    float minValue = coordinateList[0];
    float maxValue = coordinateList[coordinateList.length - 1];
    float increment = Math.abs(coordinateList[1] - coordinateList[0]);
    int closestIndex = (int) Math.round((point - minValue) / increment);
    return Math.max(0, Math.min((int) ((maxValue - minValue) / increment), closestIndex));
  }

  public static int closestIndex(float[] coordinateList, double point) {
    int minIndex = -1;
    double minDiff = -1;
    for (int i = 0; i < coordinateList.length; i++) {
      double value = coordinateList[i];
      double diff = Math.abs(value - point);
      if (minIndex == -1 || diff < minDiff) {
        minIndex = i;
        minDiff = diff;
      }
    }
    return minIndex;
  }
  
  public static int closestIndex(double[] coordinateList, double point) {
    int minIndex = -1;
    double minDiff = -1;
    for (int i = 0; i < coordinateList.length; i++) {
      double value = coordinateList[i];
      double diff = Math.abs(value - point);
      if (minIndex == -1 || diff < minDiff) {
        minIndex = i;
        minDiff = diff;
      }
    }
    return minIndex;
  }

  public static int[] getIndexAndNext(float[] coordinateList, double point) {
    int index1 = closestIndex(coordinateList, point);
    if (index1 == 0) {
      return new int[]{0, 1};
    }
    int index2;
    if (index1 == coordinateList.length - 1 || coordinateList[index1] > point) {
      index2 = index1 - 1;
    } else {
      index2 = index1 + 1;
    }

    if (index1 > index2) {
      return new int[]{index2, index1};
    }
    return new int[]{index1, index2};
  }

  public static OptionalDouble interpolate(double z, PolynomialSplineFunction f) {
    if (f.isValidPoint(z)) {
      // check this first to prevent an unnecessary array copy
      return OptionalDouble.of(f.value(z));
    } else {
      // same behavior as np.interp
      double[] knots = f.getKnots();
      if (z <= knots[0]) {
        return OptionalDouble.of(f.value(knots[0]));
      }
    }
    return OptionalDouble.empty();
  }


  public static double distanceM(double lon1, double lat1, double lon2, double lat2) {
    gc.setStartingGeographicPoint(lon1, lat1);
    gc.setDestinationGeographicPoint(lon2, lat2);
    return gc.getOrthodromicDistance();
  }

  private InterpolationUtils() {

  }
}
