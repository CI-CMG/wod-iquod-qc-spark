package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import edu.colorado.cires.wod.iquodqc.common.InterpolationUtils;
import java.io.IOException;
import java.nio.file.Path;
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

public abstract class StatsGetter<T extends Stats> {

  private static final int MIN_NUM_POINTS = 2;

  private TricubicInterpolatingFunction meanInterpolator = null;
  private TricubicInterpolatingFunction stdInterpolator = null;
  private Integer minIndexLat = null;
  private Integer maxIndexLat = null;
  private Integer minIndexLon = null;
  private Integer maxIndexLon = null;

  private final StatsGetterProperties statsGetterProperties;

  protected StatsGetter(StatsGetterProperties statsGetterProperties) {
    this.statsGetterProperties = statsGetterProperties;
  }

  private static int[] getNearestIndices(float[] referenceValues, double value) {
    for (int i = 0; i < referenceValues.length - 1; i++) {
      double currentDepth = referenceValues[i];

      if (value == currentDepth) {
        return new int[]{i};
      }

      int nextIndex = i + 1;
      double nextDepth = referenceValues[i + 1];

      if (value == nextDepth) {
        return new int[]{nextIndex};
      }

      if (value > currentDepth && value < nextDepth) {
        return new int[]{i, nextIndex};
      }
    }

    throw new IllegalStateException("Nearest indices not found");
  }

  List<ValueAtPosition> getValues(
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

            float value;
            if (variable.getShape().length == 3) {
              value = variable.read(new int[]{netCdfDepth, netCdfLat, netCdfLon}, new int[]{1, 1, 1}).getFloat(0);
            } else if (variable.getShape().length == 4) {;
              value = variable.read(new int[]{0, netCdfDepth, netCdfLat, netCdfLon}, new int[]{1, 1, 1, 1}).getFloat(0);
            } else {
              throw new IllegalStateException("Invalid variable dimensions");
            }

            if (Precision.equals(value, fill, 0.000001d)) {
              value = Float.NaN;
            }

            value = transformValue(value, variable);

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

  protected abstract float transformValue(float value, Variable variable);

  protected static TricubicInterpolatingFunction prepareInterpolator(List<ValueAtPosition> vaps) {

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

    return new TricubicInterpolator()
        .interpolate(depth, lat, lon, values);
  }

  private static double interpolate(TricubicInterpolatingFunction intFunc, double depth, double lat, double lon) {
    try {
      return intFunc.value(depth, lat, lon);
    } catch (OutOfRangeException e) {
      return Double.NaN;
    }
  }

  protected abstract Index getNcFile(long epochMillisTimestamp);

  public T getStats(long epochMillisTimestamp, double depth, double longitude, double latitude) {
    Index index = getNcFile(epochMillisTimestamp);

    if (minIndexLat == null || maxIndexLat == null) {
      int closestLatIndex = InterpolationUtils.closestIndexAssumeSorted(index.getLatitudes(), latitude);
      minIndexLat = Math.max(0, closestLatIndex - MIN_NUM_POINTS);
      maxIndexLat = Math.min(index.getLatitudes().length - 1, closestLatIndex + MIN_NUM_POINTS);
    }

    if (minIndexLon == null || maxIndexLon == null) {
      int closestLonIndex = InterpolationUtils.closestIndexAssumeSorted(index.getLongitudes(), longitude);
      minIndexLon = closestLonIndex - MIN_NUM_POINTS;
      maxIndexLon = closestLonIndex + MIN_NUM_POINTS;
    }

    if (meanInterpolator == null) {
      meanInterpolator = prepareInterpolator(
          getValues(
              statsGetterProperties.getMean(),
              index,
              0,
              index.getDepths().length - 1,
              minIndexLat,
              maxIndexLat,
              minIndexLon,
              maxIndexLon
          )
      );
    }

    if (stdInterpolator == null) {
      stdInterpolator = prepareInterpolator(
          getValues(
              statsGetterProperties.getStandardDeviation(),
              index,
              0,
              index.getDepths().length - 1,
              minIndexLat,
              maxIndexLat,
              minIndexLon,
              maxIndexLon
          )
      );
    }

    Stats stats = new Stats(
        getStatField(
            depth,
            longitude,
            latitude,
            meanInterpolator
        ),
        getStatField(
            depth,
            longitude,
            latitude,
            stdInterpolator
        )
    );

    return processAdditionalFields(
        index,
        depth,
        longitude,
        latitude,
        minIndexLat,
        maxIndexLat,
        minIndexLon,
        maxIndexLon,
        stats
    );
  }

  protected abstract T processAdditionalFields(
      Index index,
      double depth,
      double longitude,
      double latitude,
      int minIndexLat,
      int maxIndexLat,
      int minIndexLon,
      int maxIndexLon,
      Stats baseStats
  );

  protected double getStatField(double depth, double longitude, double latitude, TricubicInterpolatingFunction intFunc) {
    return StatsGetter.interpolate(intFunc, depth, latitude, longitude);
  }

  protected static class Index {

    private final float[] longitudes;
    private final float[] latitudes;
    private final float[] depths;
    private final Path ncPath;

    public Index(Path ncPath, StatsGetterProperties statsGetterProperties) {
      this.ncPath = ncPath;
      try (NetcdfFile nc = NetcdfFiles.open(ncPath.toString())) {
        longitudes = (float[]) Objects.requireNonNull(nc.findVariable(statsGetterProperties.getLongitude())).read().copyTo1DJavaArray();
        latitudes = (float[]) Objects.requireNonNull(nc.findVariable(statsGetterProperties.getLatitude())).read().copyTo1DJavaArray();
        depths = (float[]) Objects.requireNonNull(nc.findVariable(statsGetterProperties.getDepth())).read().copyTo1DJavaArray();
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

  static class ValueAtPosition {

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
