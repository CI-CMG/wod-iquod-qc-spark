package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import edu.colorado.cires.wod.iquodqc.common.InterpolationUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.geotools.referencing.GeodeticCalculator;
import org.jetbrains.annotations.Nullable;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import ucar.ma2.Array;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;

final class AomlClimatologyUtils {


  static void getTemps(List<TempAtPosition> positionTemps, NetcdfFile netFile, String tType, WoaDataHolder dataHolder, int minIndexLat,
      int maxIndexLat, int minIndexLon, int maxIndexLon, int minIndexDepth, int maxIndexDepth) {
    Array oceanDepthData;
    try {
      oceanDepthData = Objects.requireNonNull(netFile.findVariable(tType))
          .read(new int[]{0, minIndexDepth, minIndexLat, minIndexLon},
              new int[]{1, maxIndexDepth - minIndexDepth + 1, maxIndexLat - minIndexLat + 1, maxIndexLon - minIndexLon + 1});
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

  static List<TempAtPosition> getTempsAtLocationIndex(List<TempAtPosition> tempAtPositions, int lonIndex, int latIndex) {
    return tempAtPositions.stream()
        .filter(tempAtPosition -> tempAtPosition.getLonIndex() == lonIndex && tempAtPosition.getLatIndex() == latIndex)
        .collect(Collectors.toList());
  }
  
  static List<TempAtPosition> getPositionTempsAtAllDepths(NetcdfFile netFile, String tType, WoaDataHolder dataHolder, double longitude, double latitude, GeodeticCalculator calculator) {
    int minIndexLat = InterpolationUtils.closestIndex(dataHolder.getLatitudes(), latitude - 1f);
    int maxIndexLat = InterpolationUtils.closestIndex(dataHolder.getLatitudes(), latitude + 1f);
    int minIndexLon = InterpolationUtils.closestIndex(dataHolder.getLongitudes(), longitude - 1f);
    int maxIndexLon = InterpolationUtils.closestIndex(dataHolder.getLongitudes(), longitude + 1f);
    int minIndexDepth = 0;
    int maxIndexDepth = dataHolder.getDepths().length - 1;
    
    List<TempAtPosition> tempAtPositions = new ArrayList<>(0);
    
    getTemps(tempAtPositions, netFile, tType, dataHolder, minIndexLat, maxIndexLat, minIndexLon, maxIndexLon, minIndexDepth, maxIndexDepth);

    if (minIndexLon == 0) {
      getTemps(tempAtPositions, netFile, tType, dataHolder, minIndexLat, maxIndexLat, dataHolder.getLongitudes().length - maxIndexLon - 1,
          dataHolder.getLongitudes().length - 1, minIndexDepth, maxIndexDepth);
    }
    if (maxIndexLon == dataHolder.getLongitudes().length - 1) {
      getTemps(tempAtPositions, netFile, tType, dataHolder, minIndexLat, maxIndexLat, 0, dataHolder.getLongitudes().length - 1 - minIndexLon,
          minIndexDepth, maxIndexDepth);
    }
    
    return tempAtPositions.stream()
        .peek(tempAtPosition -> tempAtPosition.setDistanceM(
            InterpolationUtils.distanceM(longitude, latitude, tempAtPosition.getLongitude(), tempAtPosition.getLatitude(), calculator)
        )).collect(Collectors.toList());
  }
  
  static @Nullable PolynomialSplineFunction fitInterpolator(List<TempAtPosition> positionTemps, double longitude, double latitude, double depth,
      boolean clipZero) {

    // could be caused by NaNs in the NC file
    if (positionTemps.isEmpty()) {  
      return null;
    }

    TempAtPosition nearest = positionTemps.stream()
        .reduce(null, (tap1, tap2) -> {
      if (tap1 == null) {
        return tap2;
      }
      double depthDistTap1 = Math.abs(depth - tap1.getDepth());
      double depthDistTap2 = Math.abs(depth - tap2.getDepth());
      if (depthDistTap2 < depthDistTap1) {
        return tap2;
      }
      if (tap2.getDistanceM() < tap1.getDistanceM()) {
        return tap2;
      }
      return tap1;
    });

    if (nearest == null) {
      StringBuilder sb = new StringBuilder("nearest is null: ")
          .append("\nlatitude: ").append(latitude)
          .append("\nlongitude: ").append(longitude)
          .append("\npositionTemps: ").append(positionTemps);
      throw new RuntimeException(sb.toString());
    }

    double rad = DistanceUtils.distHaversineRAD(latitude, longitude, nearest.getLatitude(), nearest.getLongitude());
    if (rad > 0.25) {
      return null;
    }

    List<TempAtPosition> knots = getTempsAtLocationIndex(positionTemps, nearest.getLonIndex(), nearest.getLatIndex());

    double[] depths = knots.stream().mapToDouble(tap -> clipZero ? Math.max(0d, tap.getDepth()) : tap.getDepth()).toArray();
    double[] temps = knots.stream().mapToDouble(TempAtPosition::getTemperature).toArray();

    if (depths.length == 1) {
      return null;
    }

    return new LinearInterpolator().interpolate(depths, temps);
  }

  static OptionalDouble temperatureInterpolationProcess(NetcdfFile netFile, String tType, WoaDataHolder dataHolder, double longitude, double latitude,
      double depth, boolean clipZero) {
    int[] depthIndexes = InterpolationUtils.getIndexAndNext(dataHolder.getDepths(), depth);
    if (depth > dataHolder.getDepths()[depthIndexes[1]]) {
      return OptionalDouble.empty();
    }

    int minIndexLat = InterpolationUtils.closestIndex(dataHolder.getLatitudes(), latitude - 1f);
    int maxIndexLat = InterpolationUtils.closestIndex(dataHolder.getLatitudes(), latitude + 1f);
    int minIndexLon = InterpolationUtils.closestIndex(dataHolder.getLongitudes(), longitude - 1f);
    int maxIndexLon = InterpolationUtils.closestIndex(dataHolder.getLongitudes(), longitude + 1f);

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

    // could be caused by NaNs in the NC file
    if (positionTemps.isEmpty()) {
      return OptionalDouble.empty();
    }

    TempAtPosition nearest = positionTemps.stream().reduce(null, (tap1, tap2) -> {
      tap2.setDistanceM(InterpolationUtils.distanceM(longitude, latitude, tap2.getLongitude(), tap2.getLatitude()));
      if (tap1 == null) {
        return tap2;
      }
      if (tap2.getDistanceM() < tap1.getDistanceM()) {
        return tap2;
      }
      return tap1;
    });

    if (nearest == null) {
      StringBuilder sb = new StringBuilder("nearest is null: ")
          .append("\nlatitude: ").append(latitude)
          .append("\nlongitude: ").append(longitude)
          .append("\ndepth: ").append(depth)
          .append("\npositionTemps: ").append(positionTemps)
          .append("\nminIndexLat: ").append(minIndexLat)
          .append("\nmaxIndexLat: ").append(maxIndexLat)
          .append("\nminIndexLon: ").append(minIndexLon)
          .append("\nmaxIndexLon: ").append(maxIndexLon)
          .append("\ndepthIndexes").append(Arrays.toString(depthIndexes));
      throw new RuntimeException(sb.toString());
    }

    double rad = DistanceUtils.distHaversineRAD(latitude, longitude, nearest.getLatitude(), nearest.getLongitude());
    if (rad > 0.25) {
      return OptionalDouble.empty();
    }

    List<TempAtPosition> knots = getTempsAtLocationIndex(netFile, dataHolder, tType, nearest.getLonIndex(), nearest.getLatIndex());

    double[] depths = knots.stream().mapToDouble(tap -> clipZero ? Math.max(0d, tap.getDepth()) : tap.getDepth()).toArray();
    double[] temps = knots.stream().mapToDouble(TempAtPosition::getTemperature).toArray();

    if (depths.length == 1) {
      return OptionalDouble.empty();
    }

    PolynomialSplineFunction intFunc = new LinearInterpolator().interpolate(depths, temps);
    return InterpolationUtils.interpolate(depth, intFunc);
  }

  private AomlClimatologyUtils() {

  }
}
