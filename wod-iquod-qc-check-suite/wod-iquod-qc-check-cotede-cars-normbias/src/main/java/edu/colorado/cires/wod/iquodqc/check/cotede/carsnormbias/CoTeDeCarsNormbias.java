package edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias;

import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.CoTeDeWoaNormbias;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.math3.util.Precision;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class CoTeDeCarsNormbias extends CoTeDeWoaNormbias {
  
  public static List<Double> computeNormbias(double lat, double lon, double[] temperatures, double[] depths, CarsParameters carsParameters)
      throws IOException, InvalidRangeException {
    double transformedLongitude = (lon % 360 + 360) % 360;
    double transformedLatitude = ((lat + 90) % 180 + 180) % 180 - 90;
    
    try (NetcdfFile file = NetcdfFiles.open(carsParameters.getDataFilePath().toString())) {
      double[] netCDFLons = readVariableAsDoubleArray(file, "lon");
      double[] netCDFLats = readVariableAsDoubleArray(file, "lat");
      double[] netCDFDepths = readVariableAsDoubleArray(file, "depth");

      int[] lonIndices = getEncompassingIndices(netCDFLons, transformedLongitude);
      int[] latIndices = getEncompassingIndices(netCDFLats, transformedLatitude);
      int[] depthIndices = getMinMaxIndices(netCDFDepths, depths);

      double[] latsSlice = Arrays.copyOfRange(netCDFLats, latIndices[0], latIndices[1] + 1);
      double[] lonsSlice = Arrays.copyOfRange(netCDFLons, lonIndices[0], lonIndices[1] + 1);
      double[] depthsSlice = Arrays.copyOfRange(netCDFDepths, depthIndices[0], depthIndices[1] + 1);
      
      double[] interpolatedMean = interpolateVariable(
          file,
          "mean",
          depthIndices,
          latIndices,
          lonIndices,
          latsSlice,
          lonsSlice,
          depthsSlice,
          transformedLatitude,
          transformedLongitude,
          depths
      );

      double[] interpolatedStandardDeviation = interpolateVariable(
          file,
          "std_dev",
          depthIndices,
          latIndices,
          lonIndices,
          latsSlice,
          lonsSlice,
          depthsSlice,
          transformedLatitude,
          transformedLongitude,
          depths
      );
      
      List<Double> normBiases = new ArrayList<>(0);
      for (int i = 0; i < depths.length; i++) {
        normBiases.add(
            (temperatures[i] - interpolatedMean[i]) / interpolatedStandardDeviation[i]
        );
      }
      return normBiases;
    }
  }

  private static double[] interpolateVariable(
      NetcdfFile file,
      String variableName,
      int[] depthIndices,
      int[] latIndices,
      int[] lonIndices,
      double[] latsSlice,
      double[] lonsSlice,
      double[] depthsSlice,
      double lat,
      double lon,
      double[] sourceDepths
  ) throws InvalidRangeException, IOException {
    double[] interpolatedValues = interpolateLatLon(
        getKnots(file, variableName, depthIndices, latIndices, lonIndices),
        latsSlice,
        lonsSlice,
        depthsSlice.length,
        lat,
        lon
    );

    return interpolateDepth(interpolatedValues, sourceDepths, depthsSlice);
  }

  private static double[] getKnots(NetcdfFile file, String variableName, int[] depthIndices, int[] latIndices, int[] lonIndices)
      throws InvalidRangeException, IOException {
    Variable variable = findVariable(file, variableName);
    double fill = getFillValueFromVariable(variable);

    int[] netCdfShape = variable.getShape();

    Array sliceArray = variable.read(List.of(
        Range.make(depthIndices[0], depthIndices[1]),
        Range.make(latIndices[0], latIndices[1]),
        Range.make(lonIndices[0], lonIndices[1])
    ));
    double scaleFactor = getScaleFactorFromVariable(variable);
    double addOffset = getAddOffsetFromVariable(variable);

    int[] shape = sliceArray.getShape();
    double[] knots = new double[shape[0]* shape[1]* shape[2]];

    Index index = sliceArray.getIndex();
    int i = 0;

    for (int depthIndex = 0; depthIndex < shape[0]; depthIndex++) {
      for (int latIndex = 0; latIndex < shape[1]; latIndex++) {
        for (int lonIndex = 0; lonIndex < shape[2]; lonIndex++) {
          double value = sliceArray.getDouble(index.set( depthIndex, latIndex, lonIndex));
          if (Precision.equals(value, fill, 0.000001d)) {
            value = Double.NaN;
          } else {
            value = (value * scaleFactor) + addOffset;
          }
          knots[i++] = value;
        }
      }
    }

    return knots;
  }
  private static double[] getSlice(NetcdfFile file, String variableName, int[] depthIndices, int[] latIndices, int[] lonIndices)
      throws InvalidRangeException, IOException {
    Variable variable = findVariable(file, variableName);
    double fill = getFillValueFromVariable(variable);

    double[] slice = (double[]) variable.read(List.of(
        Range.make(depthIndices[0], depthIndices[1]),
        Range.make(latIndices[0], latIndices[1]),
        Range.make(lonIndices[0], lonIndices[1])
    )).get1DJavaArray(DataType.DOUBLE);
    
    double scaleFactor = getScaleFactorFromVariable(variable);
    double addOffset = getAddOffsetFromVariable(variable);

    slice = Arrays.stream(slice)
        .map(v -> {
          
          if (Precision.equals(v, fill, 0.000001d)) {
            return Double.NaN;
          }
          return (v * scaleFactor) + addOffset;
        }).toArray();

    return slice;
  }
  
  private static double getScaleFactorFromVariable(Variable variable) {
    return readAttributeAsDouble(findAttribute(variable, "scale_factor"));
  }

  private static double getAddOffsetFromVariable(Variable variable) {
    return readAttributeAsDouble(findAttribute(variable, "add_offset"));
  }
}
