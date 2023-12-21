package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import edu.colorado.cires.wod.iquodqc.common.InterpolationUtils;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.math3.analysis.interpolation.TricubicInterpolatingFunction;
import org.apache.commons.math3.analysis.interpolation.TricubicInterpolator;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.util.Precision;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class WoaGetter {

  private static final String MEAN = "t_mn";
  private static final String NUMBER_OF_OBSERVATIONS = "t_dd";
  private static final String STANDARD_DEVIATION = "t_sd";
  private static final String STANDARD_ERROR = "t_se";
  private static final String LATITUDE = "lat";
  private static final String LONGITUDE = "lon";
  private static final String DEPTH = "depth";

  private static final int MIN_NUM_POINTS = 5;

  private enum Season {
    WINTER,
    SPRING,
    SUMMER,
    FALL
  }

  private final WoaParameters parameters;
  private final Index winterIndex;
  private final Index springIndex;
  private final Index summerIndex;
  private final Index fallIndex;

  public WoaGetter(WoaParameters parameters) {
    this.parameters = parameters;
    winterIndex = new Index(parameters.getS1Path());
    springIndex = new Index(parameters.getS2Path());
    summerIndex = new Index(parameters.getS3Path());
    fallIndex = new Index(parameters.getS4Path());
  }

  public Woa getWoa(long epochMillisTimestamp, double depth, double longitude, double latitude) {
    Season season = getSeason(epochMillisTimestamp);
    Index index = getNcFile(season);

    int closestLatIndex = InterpolationUtils.closestIndex(index.getLatitudes(), latitude);
    int closestLonIndex = InterpolationUtils.closestIndex(index.getLongitudes(), longitude);
    int closestDepthIndex = InterpolationUtils.closestIndex(index.getDepths(), depth);

    int minIndexLat = Math.max(0, closestLatIndex - MIN_NUM_POINTS);
    int maxIndexLat = Math.min(index.getLatitudes().length - 1, closestLatIndex + MIN_NUM_POINTS);
    int minIndexLon = closestLonIndex - MIN_NUM_POINTS;
    int maxIndexLon = closestLonIndex + MIN_NUM_POINTS;
    int minIndexDepth = Math.max(0, closestDepthIndex - MIN_NUM_POINTS);
    int maxIndexDepth = Math.min(index.getDepths().length - 1, closestDepthIndex + MIN_NUM_POINTS);

    List<ValueAtPosition> meanVaps = getValues(MEAN, index, minIndexDepth, maxIndexDepth, minIndexLat, maxIndexLat, minIndexLon, maxIndexLon);
    List<ValueAtPosition> stdDevVaps = getValues(STANDARD_DEVIATION, index, minIndexDepth, maxIndexDepth, minIndexLat, maxIndexLat, minIndexLon,
        maxIndexLon);
    List<ValueAtPosition> obsVaps = getValues(NUMBER_OF_OBSERVATIONS, index, minIndexDepth, maxIndexDepth, minIndexLat, maxIndexLat, minIndexLon,
        maxIndexLon);
    List<ValueAtPosition> errorVaps = getValues(STANDARD_ERROR, index, minIndexDepth, maxIndexDepth, minIndexLat, maxIndexLat, minIndexLon,
        maxIndexLon);

    InterpolatorArrays meanIA = prepareInterpolator(meanVaps);
    InterpolatorArrays stdDevIA = prepareInterpolator(stdDevVaps);
    InterpolatorArrays obsIA = prepareInterpolator(obsVaps);
    InterpolatorArrays errorIA = prepareInterpolator(errorVaps);

    double mean = interpolate(meanIA, depth, latitude, longitude);
    double standardDeviation = interpolate(stdDevIA, depth, latitude, longitude);
    double numberOfObservations = interpolate(obsIA, depth, latitude, longitude);
    double standardError = interpolate(errorIA, depth, latitude, longitude);

    return new Woa(mean, standardDeviation, numberOfObservations, standardError);
  }

  private static List<ValueAtPosition> getValues(
      String variableName,
      Index index,
      int minIndexDepth,
      int maxIndexDepth,
      int minIndexLat,
      int maxIndexLat,
      int minIndexLon,
      int maxIndexLon) {

    float[] longitudes = index.longitudes;
    float[] latitudes = index.latitudes;
    float[] depths = index.depths;

    List<ValueAtPosition> values = new ArrayList<>();

    try (NetcdfFile nc = NetcdfFiles.open(index.getNcPath().toString())) {
      Variable variable = Objects.requireNonNull(nc.findVariable(variableName));
      double fill = variable.findAttribute("_FillValue").getNumericValue().doubleValue();

      for (int depthI = minIndexDepth; depthI <= maxIndexDepth; depthI++) {
        for (int latI = minIndexLat; latI <= maxIndexLat; latI++) {
          for (int lonI = minIndexLon; lonI <= maxIndexLon; lonI++) {

            int netCdfDepth = depthI;
            int netCdfLon;
            int netCdfLat = latI;
            if (lonI < 0) {
              netCdfLon = longitudes.length + lonI;
            } else if (lonI >= longitudes.length) {
              netCdfLon = lonI - longitudes.length;
            } else {
              netCdfLon = lonI;
            }
            float value = variable.read(new int[]{0, netCdfDepth, netCdfLat, netCdfLon}, new int[]{1, 1, 1, 1}).getFloat(0);

            if (Precision.equals(value, fill, 0.000001d)) {
              value = Float.NaN;
            }

            ValueAtPosition vap = new ValueAtPosition(
                netCdfLon, netCdfLat,
                netCdfDepth, longitudes[netCdfLon], latitudes[netCdfLat], depths[netCdfDepth], value
            );
            values.add(vap);
          }
        }
      }

    } catch (IOException | InvalidRangeException e) {
      throw new RuntimeException("Unable to read data", e);
    }

    return values;
  }

  private static InterpolatorArrays prepareInterpolator(List<ValueAtPosition> vaps) {

    Map<List<Integer>, ValueAtPosition> lookup = new HashMap<>();

    vaps.sort(Comparator.comparingDouble(ValueAtPosition::getDepth));
    LinkedHashSet<Integer> depthIndexes = new LinkedHashSet<>();
    for (int i = 0; i < vaps.size(); i++) {
      ValueAtPosition vap = vaps.get(i);
      depthIndexes.add(vap.getDepthIndex());
      lookup.put(Arrays.asList(vap.getDepthIndex(), vap.getLatIndex(), vap.getLonIndex()), vap);
    }

    LinkedHashSet<Integer> latIndexes = new LinkedHashSet<>();
    vaps.sort(Comparator.comparingDouble(ValueAtPosition::getLatitude));
    for (int i = 0; i < vaps.size(); i++) {
      ValueAtPosition vap = vaps.get(i);
      latIndexes.add(vap.getLatIndex());
    }

    LinkedHashSet<Integer> lonIndexes = new LinkedHashSet<>();
    vaps.sort(Comparator.comparingDouble(ValueAtPosition::getLongitude));
    for (int i = 0; i < vaps.size(); i++) {
      ValueAtPosition vap = vaps.get(i);
      lonIndexes.add(vap.getLonIndex());
    }

//    Set<Integer> poisonDepths = new HashSet<>();
//    Set<Integer> poisonLats = new HashSet<>();
//    Set<Integer> poisonLons = new HashSet<>();

//    for (int depthIndex : depthIndexes) {
//      for (int latIndex : latIndexes) {
//        for (int lonIndex : lonIndexes) {
//          ValueAtPosition vap = lookup.get(Arrays.asList(depthIndex, latIndex, lonIndex));
//          if (vap == null) {
//            poisonDepths.add(depthIndex);
//            poisonLats.add(latIndex);
//            poisonLons.add(lonIndex);
//          }
//        }
//      }
//    }
//
//    depthIndexes.removeAll(poisonDepths);
//    latIndexes.removeAll(poisonLats);
//    lonIndexes.removeAll(poisonLons);

    double[] depth = new double[depthIndexes.size()];
    double[] lat = new double[latIndexes.size()];
    double[] lon = new double[lonIndexes.size()];
    double[][][] values = new double[depth.length][lat.length][lon.length];

    int depthI = 0;
    for (Integer depthIndex : depthIndexes) {
      int latI = 0;
      for (Integer latIndex : latIndexes) {
        int lonI = 0;
        for (Integer lonIndex : lonIndexes) {
          ValueAtPosition vap = lookup.get(Arrays.asList(depthIndex, latIndex, lonIndex));
          depth[depthI] = vap.getDepth();
          lat[latI] = vap.getLatitude();
          lon[lonI] = vap.getLongitude();
          values[depthI][latI][lonI] = vap.getValue();
          lonI++;
        }
        latI++;
      }
      depthI++;
    }
    return new InterpolatorArrays(depth, lat, lon, values);
  }

  private static double interpolate(InterpolatorArrays interpolatorArrays, double depth, double lat, double lon) {
    TricubicInterpolatingFunction intFunc = new TricubicInterpolator()
        .interpolate(interpolatorArrays.depth, interpolatorArrays.lat, interpolatorArrays.lon, interpolatorArrays.values);
    try {
      return intFunc.value(depth, lat, lon);
    } catch (OutOfRangeException e) {
      return Double.NaN;
    }
  }

  private static Season getSeason(long epochMillisTimestamp) {
    LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillisTimestamp), ZoneId.of("UTC"));
    switch (ldt.getMonth()) {
      case JANUARY:
      case FEBRUARY:
      case MARCH:
        return Season.WINTER;
      case APRIL:
      case MAY:
      case JUNE:
        return Season.SPRING;
      case JULY:
      case AUGUST:
      case SEPTEMBER:
        return Season.SUMMER;
      case OCTOBER:
      case NOVEMBER:
      case DECEMBER:
        return Season.FALL;
      default:
        throw new IllegalStateException("Unable to determine season: " + ldt.getMonth());
    }
  }

  private Index getNcFile(Season season) {
    switch (season) {
      case WINTER:
        return winterIndex;
      case SPRING:
        return springIndex;
      case SUMMER:
        return summerIndex;
      case FALL:
        return fallIndex;
      default:
        throw new IllegalStateException("Unable to determine season file: " + season);
    }
  }

  private static class Index {

    private final float[] longitudes;
    private final float[] latitudes;
    private final float[] depths;
    private final Path ncPath;

    public Index(Path ncPath) {
      this.ncPath = ncPath;
      try (NetcdfFile nc = NetcdfFiles.open(ncPath.toString())) {
        longitudes = (float[]) Objects.requireNonNull(nc.findVariable(LONGITUDE)).read().copyTo1DJavaArray();
        latitudes = (float[]) Objects.requireNonNull(nc.findVariable(LATITUDE)).read().copyTo1DJavaArray();
        depths = (float[]) Objects.requireNonNull(nc.findVariable(DEPTH)).read().copyTo1DJavaArray();
      } catch (IOException e) {
        throw new RuntimeException("Unable to open NetCDF file: " + ncPath, e);
      }
    }

    public float[] getLongitudes() {
      return longitudes;
    }

    public float[] getLatitudes() {
      return latitudes;
    }

    public float[] getDepths() {
      return depths;
    }

    public Path getNcPath() {
      return ncPath;
    }
  }

  private static class InterpolatorArrays {

    public final double[] depth;
    public final double[] lat;
    public final double[] lon;
    public final double[][][] values;

    private InterpolatorArrays(double[] depth, double[] lat, double[] lon, double[][][] values) {
      this.depth = depth;
      this.lat = lat;
      this.lon = lon;
      this.values = values;
    }
  }

  private static class ValueAtPosition {

    private final int lonIndex;
    private final int latIndex;
    private final int depthIndex;
    private final float depth;
    private final float longitude;
    private final float latitude;
    private final float value;
    private double distanceM;

    public ValueAtPosition(int lonIndex, int latIndex, int depthIndex, float longitude, float latitude, float depth, float value) {
      this.lonIndex = lonIndex;
      this.latIndex = latIndex;
      this.depthIndex = depthIndex;
      this.depth = depth;
      this.longitude = longitude;
      this.latitude = latitude;
      this.value = value;
    }

    public int getLonIndex() {
      return lonIndex;
    }

    public int getLatIndex() {
      return latIndex;
    }

    public float getDepth() {
      return depth;
    }

    public float getLongitude() {
      return longitude;
    }

    public float getLatitude() {
      return latitude;
    }

    public double getDistanceM() {
      return distanceM;
    }

    public void setDistanceM(double distanceM) {
      this.distanceM = distanceM;
    }

    public int getDepthIndex() {
      return depthIndex;
    }

    public float getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ValueAtPosition that = (ValueAtPosition) o;
      return lonIndex == that.lonIndex && latIndex == that.latIndex && depthIndex == that.depthIndex && Double.compare(that.depth, depth) == 0
          && Double.compare(that.longitude, longitude) == 0 && Double.compare(that.latitude, latitude) == 0
          && Float.compare(that.value, value) == 0 && Double.compare(that.distanceM, distanceM) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(lonIndex, latIndex, depthIndex, depth, longitude, latitude, value, distanceM);
    }

    @Override
    public String toString() {
      return "ValueAtPosition{" +
          "lonIndex=" + lonIndex +
          ", latIndex=" + latIndex +
          ", depthIndex=" + depthIndex +
          ", depth=" + depth +
          ", longitude=" + longitude +
          ", latitude=" + latitude +
          ", value=" + value +
          ", distanceM=" + distanceM +
          '}';
    }
  }

}
