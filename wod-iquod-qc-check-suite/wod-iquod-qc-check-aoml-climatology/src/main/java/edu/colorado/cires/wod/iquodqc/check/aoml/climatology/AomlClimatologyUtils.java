package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.geotools.referencing.CRS;
import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import ucar.ma2.Array;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;

final class AomlClimatologyUtils {

  private static final CoordinateReferenceSystem EPSG_4326;

  static {
    try {
      EPSG_4326 = CRS.decode("EPSG:4326");
    } catch (FactoryException e) {
      throw new RuntimeException("Unable to determine CRS", e);
    }
  }

  static int closestIndex(float[] coordinateList, double point) {
    int minIndex = -1;
    double minDiff = -1;
    for (int i = 0; i < coordinateList.length; i++) {
      double diff = Math.abs(coordinateList[i] - point);
      if (minIndex == -1 || diff < minDiff) {
        minIndex = i;
        minDiff = diff;
      }
    }
    return minIndex;
  }

  static int[] getIndexAndNext(float[] coordinateList, double point) {
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

  static void getTemps(List<TempAtPosition> positionTemps, NetcdfFile netFile, String tType, WoaDataHolder dataHolder, int minIndexLat,
      int maxIndexLat, int minIndexLon, int maxIndexLon, int minIndexDepth, int maxIndexDepth) {
    Array oceanDepthData;
    try {
      oceanDepthData = Objects.requireNonNull(netFile.findVariable(tType))
          .read(new int[]{0, minIndexDepth, minIndexLat, minIndexLon}, new int[]{1, maxIndexDepth - minIndexDepth + 1, maxIndexLat - minIndexLat + 1, maxIndexLon - minIndexLon + 1});
    } catch (InvalidRangeException | IOException e) {
      throw new RuntimeException("Unable to read NetCdf data", e);
    }

    IndexIterator it = oceanDepthData.getIndexIterator();
    while (it.hasNext()) {
      float temp = it.getFloatNext();
      if (!Float.isNaN(temp)) {
        int realDepthIndex = minIndexDepth + it.getCurrentCounter()[1];
        int realLatIndex = minIndexLat + it.getCurrentCounter()[2];
        int realLonIndex = minIndexLon + it.getCurrentCounter()[3];
        TempAtPosition tap = new TempAtPosition(
            realDepthIndex,
            realLonIndex,
            realLatIndex,
            dataHolder.getDepths()[realDepthIndex],
            dataHolder.getLongitudes()[realLonIndex],
            dataHolder.getLatitudes()[realLatIndex],
            temp);
        positionTemps.add(tap);
      }
    }
  }

  static List<TempAtPosition> getTempsAtLocationIndex(NetcdfFile netFile, WoaDataHolder dataHolder, String tType, int lonIndex, int latIndex) {
    Array oceanDepthData;
    try {
      oceanDepthData = Objects.requireNonNull(netFile.findVariable(tType))
          .read(new int[]{0, 0, latIndex, lonIndex}, new int[]{1, dataHolder.getDepths().length, 1, 1});
    } catch (InvalidRangeException | IOException e) {
      throw new RuntimeException("Unable to read NetCdf data", e);
    }

    List<TempAtPosition> knots = new ArrayList<>();

    IndexIterator it = oceanDepthData.getIndexIterator();
    while (it.hasNext()) {
      float temp = it.getFloatNext();
      int depthIndex = it.getCurrentCounter()[1];
      if (!Float.isNaN(temp)) {
        TempAtPosition tap = new TempAtPosition(
            depthIndex,
            lonIndex,
            latIndex,
            dataHolder.getDepths()[depthIndex],
            dataHolder.getLongitudes()[lonIndex],
            dataHolder.getLatitudes()[latIndex],
            temp);
        knots.add(tap);
      }
    }

    return knots;
  }

  static OptionalDouble temperatureInterpolationProcess(NetcdfFile netFile, String tType, WoaDataHolder dataHolder, double longitude, double latitude,
      double depth, boolean clipZero) {
    int[] depthIndexes = getIndexAndNext(dataHolder.getDepths(), depth);
    if (depth > dataHolder.getDepths()[depthIndexes[1]]) {
      return OptionalDouble.empty();
    }

    int minIndexLat = closestIndex(dataHolder.getLatitudes(), latitude - 1f);
    int maxIndexLat = closestIndex(dataHolder.getLatitudes(), latitude + 1f);
    int minIndexLon = closestIndex(dataHolder.getLongitudes(), longitude - 1f);
    int maxIndexLon = closestIndex(dataHolder.getLongitudes(), longitude + 1f);

    int minIndexDepth = depthIndexes[0];
    int maxIndexDepth = depthIndexes[1];

    List<TempAtPosition> positionTemps = new ArrayList<>();

    getTemps(positionTemps, netFile, tType, dataHolder, minIndexLat, maxIndexLat, minIndexLon, maxIndexLon, minIndexDepth, maxIndexDepth);

    // near antimeridian
    if (minIndexLon == 0) {
      getTemps(positionTemps, netFile, tType, dataHolder, minIndexLat, maxIndexLat, dataHolder.getLongitudes().length - maxIndexLon - 1,
          dataHolder.getLongitudes().length - 1, minIndexDepth, maxIndexDepth);
    }
    if (maxIndexLon == dataHolder.getLongitudes().length - 1) {
      getTemps(positionTemps, netFile, tType, dataHolder, minIndexLat, maxIndexLat, 0, dataHolder.getLongitudes().length - 1 - minIndexLon,
          minIndexDepth, maxIndexDepth);
    }

    TempAtPosition nearest = positionTemps.stream().reduce(null, (tap1, tap2) -> {
      tap2.setDistanceM(distanceM(longitude, latitude, tap2.getLongitude(), tap2.getLatitude()));
      if (tap1 == null) {
        return tap2;
      }
      if (tap2.getDistanceM() < tap1.getDistanceM()) {
        return tap2;
      }
      return tap1;
    });

    double rad = DistanceUtils.distHaversineRAD(latitude, longitude, nearest.getLatitude(), nearest.getLongitude());
    if (rad > 0.25) {
      return OptionalDouble.empty();
    }

    List<TempAtPosition> knots = getTempsAtLocationIndex(netFile, dataHolder, tType, nearest.getLonIndex(), nearest.getLatIndex());

    PolynomialSplineFunction intFunc = new LinearInterpolator().interpolate(
        knots.stream().mapToDouble(tap -> clipZero ? Math.max(0d, tap.getDepth()) : tap.getDepth()).toArray(),
        knots.stream().mapToDouble(TempAtPosition::getTemperature).toArray());
    return interpolate(depth, intFunc);
  }


  private static OptionalDouble interpolate(double z, PolynomialSplineFunction f) {
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


  static double distanceM(double lon1, double lat1, double lon2, double lat2) {
    GeodeticCalculator gc = new GeodeticCalculator(EPSG_4326);
    gc.setStartingGeographicPoint(lon1, lat1);
    gc.setDestinationGeographicPoint(lon2, lat2);
    return gc.getOrthodromicDistance();
  }

  private AomlClimatologyUtils() {

  }
}
