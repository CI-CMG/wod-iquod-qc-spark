package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.util.Precision;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class CoTeDeWoaNormbias {

  public static List<WoaNormbias> computeNormbias(double lat, double lon, double[] temperatures, double[] depths, long timestamp, WoaParameters woaParameters)
      throws IOException, InvalidRangeException {
    
    int doy = Instant.ofEpochMilli(timestamp).atZone(ZoneOffset.UTC).getDayOfYear();

    try (
        NetcdfFile s1 = NetcdfFiles.open(woaParameters.getS1Path().toString());
        NetcdfFile s2 = NetcdfFiles.open(woaParameters.getS2Path().toString());
        NetcdfFile s3 = NetcdfFiles.open(woaParameters.getS3Path().toString());
        NetcdfFile s4 = NetcdfFiles.open(woaParameters.getS4Path().toString())
    ) {
      NetcdfFile[] netCDFSeasons = new NetcdfFile[]{s1, s2, s3, s4};
      double[] netCDFLons = readVariableAsDoubleArray(s1, "lon");
      double[] netCDFLats = readVariableAsDoubleArray(s1, "lat");
      double[] netCDFDepths = readVariableAsDoubleArray(s1, "depth");
      double[] netCDFTimes = new double[]{
          readTime(s4) - 365.25,
          readTime(s1),
          readTime(s2),
          readTime(s3),
          readTime(s4),
          readTime(s1) + 365.25
      };
      
      int[] timeToSeasonMappings = new int[] {3, 0, 1, 2, 3, 0};
      
      int[] lonIndices = getEncompassingIndices(netCDFLons, lon);
      int[] latIndices = getEncompassingIndices(netCDFLats, lat);
      int[] timeMappingIndices = getEncompassingIndices(netCDFTimes, doy);
      int[] timeIndices = new int[]{timeToSeasonMappings[timeMappingIndices[0]], timeToSeasonMappings[timeMappingIndices[1]]};
      int[] depthIndices = getMinMaxIndices(netCDFDepths, depths);


      double[] latsSlice = Arrays.copyOfRange(netCDFLats, latIndices[0], latIndices[1] + 1);
      double[] lonsSlice = Arrays.copyOfRange(netCDFLons, lonIndices[0], lonIndices[1] + 1);
      double[] depthsSlice = Arrays.copyOfRange(netCDFDepths, depthIndices[0], depthIndices[1] + 1);
      double[] timesSlice = Arrays.copyOfRange(netCDFTimes, timeMappingIndices[0], timeMappingIndices[1] + 1);

      NetcdfFile[] seasons = new NetcdfFile[]{netCDFSeasons[timeIndices[0]], netCDFSeasons[timeIndices[1]]};
      
      double[] interpolatedMean = interpolateVariable(
          seasons,
          "t_mn",
          depthIndices,
          latIndices,
          lonIndices,
          doy,
          latsSlice,
          lonsSlice,
          timesSlice,
          depthsSlice,
          lat,
          lon,
          depths
      );
      double[] interpolatedStandardDeviation = interpolateVariable(
          seasons,
          "t_sd",
          depthIndices,
          latIndices,
          lonIndices,
          doy,
          latsSlice,
          lonsSlice,
          timesSlice,
          depthsSlice,
          lat,
          lon,
          depths
      );
      int[] numberOfObservations = Arrays.stream(
              interpolateVariable(
                  seasons,
                  "t_dd",
                  depthIndices,
                  latIndices,
                  lonIndices,
                  doy,
                  latsSlice,
                  lonsSlice,
                  timesSlice,
                  depthsSlice,
                  lat,
                  lon,
                  depths
              )
          )
          .mapToInt(d -> (int) Math.round(Math.abs(d)))
          .toArray();

      List<WoaNormbias> normBiases = new ArrayList<>(0);
      for (int i = 0; i < depths.length; i++) {
        normBiases.add(
            new WoaNormbias(
                (temperatures[i] - interpolatedMean[i]) / interpolatedStandardDeviation[i],
                numberOfObservations[i]
            )
        );
      }
      return normBiases;
    }
  }

  private static double[] interpolateVariable(
      NetcdfFile[] seasons,
      String variableName,
      int[] depthIndices,
      int[] latIndices,
      int[] lonIndices,
      int doy,
      double[] latsSlice,
      double[] lonsSlice,
      double[] timesSlice,
      double[] depthsSlice,
      double lat,
      double lon,
      double[] sourceDepths
  ) throws InvalidRangeException, IOException {
    double[] interpolatedValues = interpolateTime(
        getSlice(
            seasons[0],
            variableName,
            depthIndices,
            latIndices,
            lonIndices
        ),
        getSlice(
            seasons[1],
            variableName,
            depthIndices,
            latIndices,
            lonIndices
        ),
        timesSlice,
        doy
    );
    
    interpolatedValues = interpolateLatLon(
        interpolatedValues,
        latsSlice,
        lonsSlice,
        depthsSlice.length,
        lat,
        lon
    );
    
    return interpolateDepth(
        interpolatedValues,
        sourceDepths,
        depthsSlice
    );
  }

  protected static Variable findVariable(NetcdfFile file, String variableName) {
    return Objects.requireNonNull(file.findVariable(variableName));
  }
  
  protected static Attribute findAttribute(Variable variable, String attributeName) {
    return Objects.requireNonNull(variable.findAttribute(attributeName));
  }
  
  protected static double readAttributeAsDouble(Attribute attribute) {
    return Objects.requireNonNull(attribute.getNumericValue()).doubleValue();
  }
  
  protected static double getFillValueFromVariable(Variable variable) {
    return readAttributeAsDouble(findAttribute(variable, "_FillValue"));
  }

  private static double readTime(NetcdfFile file) throws IOException {
    Variable variable = findVariable(file, "time");
    return (365/12D) * (variable.read().getDouble(0) % 12);
  }
  
  protected static double[] readVariableAsDoubleArray(NetcdfFile file, String variableName) throws IOException {
    return (double[]) findVariable(file, variableName).read().get1DJavaArray(DataType.DOUBLE);
  }
  
  protected static int[] getEncompassingIndices(double[] values, double value) {
    int[] indices = new int[2];
    for (int i = 1; i < values.length; i++) {
      double currentLon = values[i];
      double lastLon = values[i - 1];

      if (currentLon > value && lastLon <= value) {
        indices[0] = i - 1;
        indices[1] = i;
      }
    }
    
    return indices;
  }
  
  protected static int[] getMinMaxIndices(double[] referenceValues, double[] values) {
    int[] indices = new int[2];
    
    double minDepth = Arrays.stream(values).min().orElseThrow(
        () -> new IllegalStateException("Failed to find minimum value for value set")
    );
    double maxDepth = Math.min(Arrays.stream(values).max().orElseThrow(
        () -> new IllegalStateException("Failed to find maximum value for value set")
    ), Arrays.stream(referenceValues).max().orElseThrow());
    
    for (int i = 1; i < referenceValues.length; i++) {
      double currentDepth = referenceValues[i];
      double lastDepth = referenceValues[i - 1];
      if (lastDepth <= minDepth && currentDepth > minDepth) {
        indices[0] = i - 1;
      }

      if (lastDepth < maxDepth && currentDepth >= maxDepth) {
        indices[1] = i;
      }
    }
    
    return indices;
  }
  
  private static double[] getSlice(NetcdfFile file, String variableName, int[] depthIndices, int[] latIndices, int[] lonIndices)
      throws InvalidRangeException, IOException {
    Variable variable = findVariable(file, variableName);
    double fill = getFillValueFromVariable(variable);
    
    double[] slice = (double[]) variable.read(List.of(
        Range.make(0, 0),
        Range.make(depthIndices[0], depthIndices[1]),
        Range.make(latIndices[0], latIndices[1]),
        Range.make(lonIndices[0], lonIndices[1])
    )).get1DJavaArray(DataType.DOUBLE);
    
    slice = Arrays.stream(slice)
        .map(v -> {
          if (Precision.equals(v, fill, 0.000001d)) {
            return Double.NaN;
          }
          return v;
        }).toArray();
    
    return slice;
  }
  
  private static double[] interpolateTime(double[] slice1, double[] slice2, double[] subTimes, int doy) {
    double[] interpolatedValues = new double[slice1.length];
    
    for (int i = 0; i < slice1.length; i++) {
      interpolatedValues[i] = new LinearInterpolator().interpolate(subTimes, new double[]{slice1[i], slice2[i]}).value(doy);
    }
    
    return interpolatedValues;
  }
  
  protected static double[] interpolateLatLon(
      double[] interpolatedValues,
      double[] latsSlice,
      double[] lonsSlice,
      int nDepths,
      double lat,
      double lon
  ) {
    double[] latLonInterpolatedValues = new double[nDepths];
    if (latsSlice.length != 1 && lonsSlice.length != 1) {
      double[][] latInterpolatedValues = new double[interpolatedValues.length / 4][];
      for (int i = 0; i < interpolatedValues.length; i += 4) {
        latInterpolatedValues[i / 4] = new double[]{
            new LinearInterpolator().interpolate(latsSlice, new double[]{interpolatedValues[i], interpolatedValues[i + 2]}).value(lat),
            new LinearInterpolator().interpolate(latsSlice, new double[]{interpolatedValues[i + 1], interpolatedValues[i + 3]}).value(lat)
        };
      }

      for (int i = 0; i < latInterpolatedValues.length; i++) {
        latLonInterpolatedValues[i] = new LinearInterpolator().interpolate(lonsSlice, latInterpolatedValues[i]).value(lon);
      }
    } else if (lonsSlice.length == 1 && latsSlice.length == 1) {
      latLonInterpolatedValues = interpolatedValues;
    } else if (lonsSlice.length != 1) {
      for (int i = 0; i < interpolatedValues.length; i+=2) {
        latLonInterpolatedValues[i / 2] =  new LinearInterpolator().interpolate(lonsSlice, new double[]{
            interpolatedValues[i], interpolatedValues[i + 1]
        }).value(lon);
      }
    } else {
      for (int i = 0; i < interpolatedValues.length; i+=2) {
        latLonInterpolatedValues[i / 2] =  new LinearInterpolator().interpolate(latsSlice, new double[]{
            interpolatedValues[i], interpolatedValues[i + 1]
        }).value(lat);
      }
    }
    
    return latLonInterpolatedValues;
  }
  
  protected static double[] interpolateDepth(double[] interpolatedValues, double[] sourceDepths, double[] depthsSlice) {
    if (depthsSlice.length == 1) {
      return interpolatedValues;
    }
    double[] depthInterpolatedValues = new double[sourceDepths.length];
    PolynomialSplineFunction function = new LinearInterpolator().interpolate(depthsSlice, interpolatedValues);
    for (int i = 0; i < sourceDepths.length; i++) {
      try {
        depthInterpolatedValues[i] = function.value(sourceDepths[i]);
      } catch (OutOfRangeException e) {
        depthInterpolatedValues[i] = Double.NaN;
      }
    }
    
    return depthInterpolatedValues;
  }
  
  public static class WoaNormbias {
    private final double value;
    private final int nSamples;

    public WoaNormbias(double value, int nSamples) {
      this.value = value;
      this.nSamples = nSamples;
    }

    public double getValue() {
      return value;
    }

    public int getNSamples() {
      return nSamples;
    }
  }

}
