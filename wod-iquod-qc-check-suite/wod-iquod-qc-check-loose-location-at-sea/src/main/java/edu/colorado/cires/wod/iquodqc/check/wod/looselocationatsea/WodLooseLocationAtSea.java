package edu.colorado.cires.wod.iquodqc.check.wod.looselocationatsea;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Section;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class WodLooseLocationAtSea {
  
  private static final String LAT_FIELD_NAME = "ETOPO05_Y";
  private static final String LON_FIELD_NAME = "ETOPO05_X";
  private static final String ELEVATIONS_FIELD_NAME = "ROSE";
  
  public static boolean checkLooseLocationAtSea(double lat, double lon, int bufferWidth, NetcdfFile netcdfFile) {
    if (!coordinatesValid(lat, lon)) {
      return false;
    }
    
    lon = updateLonToProperRange(lon);
    
    VariableMetadata latMetadata = computeVariableMetadataForExpected1DArray(
        getVariable(LAT_FIELD_NAME, netcdfFile),
        lat
    );
    VariableMetadata lonMetadata = computeVariableMetadataForExpected1DArray(
        getVariable(LON_FIELD_NAME, netcdfFile),
        lon
    );
    
    ArrayPoint2D start = computeStart2D(
        latMetadata,
        lonMetadata,
        bufferWidth,
        WodLooseLocationAtSea::computeStart1D
    );
    ArrayPoint2D end = computeStart2D(
        latMetadata,
        lonMetadata,
        bufferWidth,
        WodLooseLocationAtSea::computeEnd1D
    );

    Set<Double> elevationSubsectionValues = getSubsectionOfExpected2DArray(
        start,
        end,
        latMetadata,
        lonMetadata,
        getVariable(ELEVATIONS_FIELD_NAME, netcdfFile)
    );

    return subsectionValuesAtSea(elevationSubsectionValues);
  }
  
  private static double updateLonToProperRange(double lon) {
    if (lon < 0) {
      lon += 360;
    }
    
    return lon;
  }
  
  private static boolean coordinatesValid(double lat, double lon) {
    return !(lon < -180) && !(lon >= 360) && !(lat < -90) && !(lat > 90);
  }
  
  private static boolean subsectionValuesAtSea(Set<Double> values) {
    return values.stream()
        .anyMatch(v -> v < 0); // negative value is underwater. Original test passes if just one value is underwater
  }
  
  private static Set<Double> getSubsectionOfExpected2DArray(
      ArrayPoint2D start,
      ArrayPoint2D end,
      VariableMetadata latMetadata,
      VariableMetadata lonMetadata,
      Variable variable
  ) {
    if (variable.getShape().length == 2) {
      throw new IllegalStateException("Variable is not a 2D array");
    }
    try {
      Section section = Section.builder()
          .appendRange(start.lat.actual, end.lat.actual)
          .appendRange(start.lon.actual, end.lon.actual)
          .build();

      Set<Double> originalSubsection = Arrays.stream(((double[]) variable.read(section)
          .get1DJavaArray(DataType.DOUBLE))) // requesting a flat array should be sufficient because all values are being checked independently of one another
          .boxed()
          .collect(Collectors.toSet());
      
      if (!start.computedEqualsActual() || !end.computedEqualsActual()) { // cases where, because of the buffer width, additional values must be taken from the star or end of the matrix rows/columns
        Section.Builder builder = Section.builder();
        
        if (!start.lat.computedEqualsActual()) {
          builder = builder.appendRange(latMetadata.lastIndex + start.lat.computed, latMetadata.lastIndex);
        } else if (!end.lat.computedEqualsActual()) {
          builder = builder.appendRange(0, end.lat.computed - latMetadata.lastIndex);
        } else {
          builder = builder.appendRange(start.lat.actual, end.lat.actual);
        }
        
        if (!start.lon.computedEqualsActual()) {
          builder = builder.appendRange(lonMetadata.lastIndex + start.lon.computed, lonMetadata.lastIndex);
        } else if (!end.lon.computedEqualsActual()) {
          builder = builder.appendRange(0, end.lon.computed - lonMetadata.lastIndex);
        } else {
          builder = builder.appendRange(start.lon.actual, end.lon.actual);
        }
        
        section = builder.build();

        originalSubsection.addAll(
            Arrays.stream(((double[]) variable.read(section)
                    .get1DJavaArray(DataType.DOUBLE)))
                .boxed()
                .collect(Collectors.toList())
        );
      }
      return originalSubsection;
    } catch (IOException | InvalidRangeException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static Variable getVariable(String variableName, NetcdfFile netcdfFile) {
    return Objects.requireNonNull(
        netcdfFile.findVariable(variableName),
        String.format("%s variable not found", variableName)
    );
  }
  
  private static ArrayPoint2D computeStart2D(VariableMetadata latMetadata, VariableMetadata lonMetadata, int bufferWidth, ArrayPoint1DComputation computation) {
    return new ArrayPoint2D(
        computation.compute(latMetadata, bufferWidth),
        computation.compute(lonMetadata, bufferWidth)
    );
  }
  private static ArrayPoint1D computeStart1D(VariableMetadata metadata, int bufferWidth) {
    int computed = metadata.minIndex - bufferWidth - 1;
    return new ArrayPoint1D(
        computed,
        Math.max(computed, 0)
    );
  }
  
  private static ArrayPoint1D computeEnd1D(VariableMetadata metadata, int bufferWidth) {
    int computed = metadata.minIndex + bufferWidth + 1;
    return new ArrayPoint1D(
        computed,
        Math.min(computed, metadata.lastIndex)
    );
  }
  
  private static VariableMetadata computeVariableMetadataForExpected1DArray(Variable variable, double sourceCoordinateValue) {
    int[] variableShape = variable.getShape();
    if (variableShape.length == 1) {
      throw new IllegalStateException(String.format("%s variable is not a 1D array", variable));
    }
    int variableLength = variableShape[0];
    int minIndex = Integer.MIN_VALUE;
    for (int i = 0; i < variableLength; i++) {
      double diff = computeVariableDifference(i, sourceCoordinateValue, variable);
      
      if (minIndex == Integer.MIN_VALUE) {
        minIndex = i;
        continue;
      }
      
      double currentMinDiff = computeVariableDifference(minIndex, sourceCoordinateValue, variable);
      if (diff < currentMinDiff) {
        minIndex = i;
      }
    }
    
    return new VariableMetadata(minIndex, variableLength - 1);
  }
  
  private static double computeVariableDifference(int index, double sourceCoordinateValue, Variable variable) {
    try {
      return Math.abs(
          variable.read(
              Section.builder()
                  .appendRange(index, index)
                  .build()
          ).getDouble(0) - sourceCoordinateValue
      );
    } catch (IOException | InvalidRangeException e) {
      throw new RuntimeException(e);
    }
  }
  
  static class VariableMetadata {
    private final int minIndex;
    private final int lastIndex;

    VariableMetadata(int minIndex, int lastIndex) {
      this.minIndex = minIndex;
      this.lastIndex = lastIndex;
    }
  }
  
  static class ArrayPoint2D {
    private final ArrayPoint1D lat;
    private final ArrayPoint1D lon;

    ArrayPoint2D(ArrayPoint1D lat, ArrayPoint1D lon) {
      this.lat = lat;
      this.lon = lon;
    }
    
    public boolean computedEqualsActual() {
      return lat.computedEqualsActual() && lon.computedEqualsActual();
    }
  }
  
  static class ArrayPoint1D {
    private final int computed;
    private final int actual;

    ArrayPoint1D(int computed, int actual) {
      this.computed = computed;
      this.actual = actual;
    }
    
    public boolean computedEqualsActual() {
      return this.computed == this.actual;
    }
  }
  
  @FunctionalInterface
  interface ArrayPoint1DComputation {
    ArrayPoint1D compute(VariableMetadata metadata, int bufferWidth);
  }

}
