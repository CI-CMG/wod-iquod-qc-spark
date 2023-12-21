package edu.colorado.cires.wod.iquodqc.check.minmax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.util.FastMath;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Section;
import ucar.nc2.Variable;
import us.hebi.matlab.mat.format.Mat5File;
import us.hebi.matlab.mat.types.MatFile;
import us.hebi.matlab.mat.types.Matrix;

public class MinMax {
  
  private static final int GRID_SIZE = 10;
  
  public static int getGridIndex(double latitude, double longitude, Mat5File matLabFile) {
    longitude = longitude >= 180 ? longitude - 360 : longitude;

    int lonIndex = (int) (FastMath.floor((longitude + 180) / GRID_SIZE) + 1);
    int latIndex = (int) (FastMath.floor((latitude + 90) / GRID_SIZE) + 1);

    int maximumPossibleLatIndex = 180 / GRID_SIZE;
    
    latIndex = Math.min(latIndex, maximumPossibleLatIndex);
    int[] latLonIndex = new int[] {latIndex, lonIndex};
    int boxIndex = ravelMultiIndex(latLonIndex, new int[] {maximumPossibleLatIndex, 360 / GRID_SIZE});

    return findMinMaxGridBox(longitude, latitude, latLonIndex, boxIndex, matLabFile);
  }
  
  public static MinMaxTemperature getMinMax(
      double pressure,
      double[] depths,
      int gridIndex,
      Variable minimumTemperatureVariable,
      Variable maximumTemperatureVariable
  ) {
    int layerId = val2index(pressure, depths);
    
    return new MinMaxTemperature(
        readVariable(minimumTemperatureVariable, layerId, gridIndex),
        readVariable(maximumTemperatureVariable, layerId, gridIndex)
    );
  }
  
  public static boolean checkMinMax(double temperature, MinMaxTemperature minMaxTemperature) {
    return temperature > minMaxTemperature.min && temperature < minMaxTemperature.max;
  }

  private static int findMinMaxGridBox(double longitude, double latitude, int[] latLonIndex, int boxIndex, MatFile matLabFile) {

    Integer resultIndex = null;

    Matrix matrix = matLabFile.getStruct("list_ISEApts_in_boxes").getMatrix("index4H", latLonIndex[0] - 1, latLonIndex[1] -1);
    int[] listIseaBox = IntStream.range(0, matrix.getDimensions()[1])
        .map(i -> matrix.getInt(i) - 1)
        .toArray();

    Matrix lonMatrix = matLabFile.getMatrix("lon");
    Matrix latMatrix = matLabFile.getMatrix("lat");
    double[] clon = IntStream.range(0, matrix.getDimensions()[1])
        .mapToDouble(i -> lonMatrix.getDouble(listIseaBox[i]))
        .toArray();
    double[] clat = IntStream.range(0, matrix.getDimensions()[1])
        .mapToDouble(i -> latMatrix.getDouble(listIseaBox[i]))
        .toArray();

    double lat1 = FastMath.toRadians(latitude);
    double lon1 = FastMath.toRadians(longitude);

    double[] lat2 = Arrays.stream(clat)
        .map(FastMath::toRadians)
        .toArray();

    double[] lon2 = Arrays.stream(clon)
        .map(FastMath::toRadians)
        .toArray();

    List<Integer> res = IntStream.range(0, lon2.length).boxed()
        .map(i -> {
          double a = (FastMath.pow(FastMath.sin((lat2[i] - lat1)/2), 2)
              + FastMath.cos(lat1) * FastMath.cos(lat2[i]) * FastMath.pow(FastMath.sin((lon2[i] - lon1)/2), 2));
          return new Distance(i, FastMath.toDegrees(2 * FastMath.atan(FastMath.sqrt(a)/FastMath.sqrt(1 - a))));
        }).sorted(Comparator.comparing(Distance::getDistance))
        .map(Distance::getIndex)
        .collect(Collectors.toList()).subList(0, 3);

    int[] listBis = new int[] {listIseaBox[res.get(0)], listIseaBox[res.get(1)], listIseaBox[res.get(2)]};

    for (int index : listBis) {
      double[] vLon = IntStream.range(0, 7)
          .mapToDouble(i -> matLabFile.getStruct("vertices").getMatrix("lon").getDouble(i, index))
          .toArray();

      double[] vLat = IntStream.range(0,7)
          .mapToDouble(i -> matLabFile.getStruct("vertices").getMatrix("lat").getDouble(i, index))
          .toArray();

      if (Arrays.stream(vLon).max().orElseThrow(
          () -> new IllegalStateException("vLon cannot be empty")
      ) - Arrays.stream(vLon).min().getAsDouble() > 100) {
        vLon = Arrays.stream(vLon)
            .map(v -> v > 0 ? v - 360 : v)
            .toArray();
      }

      double[] finalVLon = vLon;
      double[] filteredVLon = IntStream.range(0, 7)
          .filter(i -> !Double.isNaN(vLat[i]) && !Double.isNaN(finalVLon[i]))
          .mapToDouble(i -> finalVLon[i])
          .toArray();

      double[] filteredVLat = IntStream.range(0, 7)
          .filter(i -> !Double.isNaN(vLat[i]) && !Double.isNaN(finalVLon[i]))
          .mapToDouble(i -> vLat[i])
          .toArray();

      double centralLat = matLabFile.getMatrix("lat").getDouble(0, index);

      List<Double> vc1 = new ArrayList<>();
      List<Double> vc2 = new ArrayList<>();
      if (FastMath.abs(centralLat) != 90) {
        vc1 = Arrays.stream(filteredVLon).boxed().collect(Collectors.toList());
        vc2 = Arrays.stream(filteredVLat).boxed().collect(Collectors.toList());
      } else {
        int[] reIndex = IntStream.range(0, filteredVLon.length - 1).boxed()
            .map(i -> new Distance(i, filteredVLon[i]))
            .sorted(Comparator.comparing(Distance::getDistance))
            .map(Distance::getIndex)
            .mapToInt(Integer::intValue).toArray();
        List<Double> vc2ReIndex = Arrays.stream(reIndex)
            .boxed()
            .map(i -> filteredVLat[i])
            .collect(Collectors.toList());
        List<Double> vc1ReIndex = Arrays.stream(reIndex)
            .boxed()
            .map(i -> filteredVLon[i])
            .collect(Collectors.toList());
        
        vc1.add(vc1ReIndex.get(vc1ReIndex.size() - 1) - 360);
        vc1.addAll(vc1ReIndex);
        vc1.add(vc1ReIndex.get(0) + 360);
        vc1.add(vc1ReIndex.get(0) + 360);
        vc1.add(vc1ReIndex.get(vc1ReIndex.size() - 1) - 360);
        vc1.add(vc1ReIndex.get(vc1ReIndex.size() - 1) - 360);
        
        vc2.add(vc2ReIndex.get(vc2ReIndex.size() - 1));
        vc2.addAll(vc2ReIndex);
        vc2.add(vc2ReIndex.get(0));
        vc2.add(centralLat + FastMath.signum(centralLat));
        vc2.add(centralLat + FastMath.signum(centralLat));
        vc2.add(vc2ReIndex.get(vc2ReIndex.size() - 1));
      }

      double vc1Max = vc1.stream().max(Double::compare).orElseThrow(
          () -> new IllegalStateException("vc1Max cannot be empty")
      );
      double vc1Min = vc1.stream().min(Double::compare).orElseThrow(
          () -> new IllegalStateException("vc1Min cannot be empty")
      );
      double vc2Max = vc2.stream().max(Double::compare).orElseThrow(
          () -> new IllegalStateException("vc2Max cannot be empty")
      );
      double vc2Min = vc2.stream().min(Double::compare).orElseThrow(
          () -> new IllegalStateException("vc2Min cannot be empty")
      );

      if (longitude > vc1Max) {
        longitude = longitude - 360;
      }
      if (longitude < vc1Min) {
        longitude = longitude + 360;
      }

      if (vc1Max >= longitude && vc1Min <= longitude && vc2Max >= latitude && vc2Min <= latitude) {
        resultIndex = index;
      }
    }

    assert resultIndex != null : String.format("Grid box index not found for coordinate pair: (%s, %s)", longitude, latitude);
    return resultIndex;
  }

  private static class Distance {
    private final int index;
    private final double distance;

    public Distance(int index, double distance) {
      this.index = index;
      this.distance = distance;
    }

    public double getDistance() {
      return distance;
    }

    public int getIndex() {
      return index;
    }
  }

  private static int ravelMultiIndex(int[] indices, int[] shape) {
    if (indices.length != shape.length) {
      throw new IllegalArgumentException("Indices and shape must have the same length");
    }

    int flatIndex = 0;
    int stride = 1;

    for (int i = 0; i < indices.length; i++) {
      if (indices[i] < 0 || indices[i] >= shape[i]) {
        throw new IndexOutOfBoundsException("Index out of bounds for dimension " + i);
      }

      flatIndex += indices[i] * stride;
      stride *= shape[i];
    }

    return flatIndex;
  }
  
  private static double readVariable(Variable variable, int layerId, int gridIndex) {
    try {
      return variable.read(
          Section.builder()
              .appendRange(gridIndex, gridIndex)
              .appendRange(layerId, layerId)
              .build()
      ).getDouble(0);
    } catch (IOException | InvalidRangeException e) {
      throw new RuntimeException(e);
    }
  }

  private static int val2index(double pressure, double[] depths) {
    for (int i = 0; i < depths.length - 1; i++) {
      double currentDepthValue = depths[i];
      double nextDepthValue = depths[i + 1];

      if (Double.isNaN(currentDepthValue) || Double.isNaN(nextDepthValue)) {
        continue;
      }

      if (pressure > currentDepthValue && pressure <= nextDepthValue) {
        return i;
      }
    }

    throw new IllegalStateException(String.format("Matching depth value not found for pressure: %s", pressure));
  }
  
  public static class MinMaxTemperature {
    private final double min;
    private final double max;

    public MinMaxTemperature(double min, double max) {
      this.min = min;
      this.max = max;
    }

    public double getMin() {
      return min;
    }

    public double getMax() {
      return max;
    }
  }

}
