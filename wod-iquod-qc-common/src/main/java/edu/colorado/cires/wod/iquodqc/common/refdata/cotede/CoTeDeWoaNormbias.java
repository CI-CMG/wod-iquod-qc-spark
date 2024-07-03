package edu.colorado.cires.wod.iquodqc.common.refdata.cotede;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.MonthDay;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.DoubleStream;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.interpolation.TricubicInterpolatingFunction;
import org.apache.commons.math3.analysis.interpolation.TricubicInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.util.Precision;
import org.jetbrains.annotations.NotNull;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class CoTeDeWoaNormbias {

  private static class SeasonFile implements Comparable<SeasonFile> {
    private final NetcdfFile netCdfFile;
    private final MonthDay monthDay;
    private LocalDate localDate;

    private SeasonFile(NetcdfFile netCdfFile, MonthDay monthDay) {
      this.netCdfFile = netCdfFile;
      this.monthDay = monthDay;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SeasonFile that = (SeasonFile) o;
      return Objects.equals(monthDay, that.monthDay);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(monthDay);
    }

    @Override
    public int compareTo(@NotNull SeasonFile o) {
      return monthDay.compareTo(o.monthDay);
    }

    public NetcdfFile getNetCdfFile() {
      return netCdfFile;
    }

    public MonthDay getMonthDay() {
      return monthDay;
    }

    public LocalDate getLocalDate() {
      return localDate;
    }

    public void setLocalDate(LocalDate localDate) {
      this.localDate = localDate;
    }

    public SeasonFile duplicate() {
      SeasonFile seasonFile = new SeasonFile(netCdfFile, monthDay);
      seasonFile.setLocalDate(localDate);
      return seasonFile;
    }
  }

  private static List<SeasonFile> getSeasons(LocalDate localDate, NetcdfFile... files) throws IOException {
    List<SeasonFile> seasonFiles = new ArrayList<>(files.length);
    for (NetcdfFile file: files) {
      double firstTime = readTime(file); // 373.5
      LocalDate _1955 = LocalDate.of(1955, 1, 1);
      LocalDate seasonStart = _1955.plusMonths((long) firstTime);
      int month = seasonStart.getMonthValue();
      YearMonth yearMonthObject = YearMonth.of(seasonStart.getYear(), month);
      int daysInMonth = yearMonthObject.lengthOfMonth();
      double percentMonth = firstTime - (long) firstTime;
      int startDay = (int)((double) daysInMonth * percentMonth) + 1;
      SeasonFile seasonFile = new SeasonFile(file, MonthDay.of(month, startDay));
      seasonFile.setLocalDate(LocalDate.of(localDate.getYear(), month, startDay));
      seasonFiles.add(seasonFile);
    }
    Collections.sort(seasonFiles);

    SeasonFile after = seasonFiles.get(0).duplicate();
    after.setLocalDate(after.getLocalDate().plusYears(1));

    SeasonFile before = seasonFiles.get(seasonFiles.size() - 1).duplicate();
    before.setLocalDate(before.getLocalDate().minusYears(1));

    List<SeasonFile> result = new ArrayList<>(seasonFiles.size() + 1);
    result.add(before);
    result.addAll(seasonFiles);
    result.add(after);
    return result;
  }

  public static List<WoaNormbias> computeNormbias(double lat, double lon, double[] temperatures, double[] depths, long timestamp, WoaParameters woaParameters)
      throws IOException, InvalidRangeException {

    ZonedDateTime dateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneOffset.UTC);
    LocalDate localDate = LocalDate.of(dateTime.getYear(), dateTime.getMonth(), dateTime.getDayOfMonth());
    try (
        NetcdfFile s1 = NetcdfFiles.open(woaParameters.getS1Path().toString());
        NetcdfFile s2 = NetcdfFiles.open(woaParameters.getS2Path().toString());
        NetcdfFile s3 = NetcdfFiles.open(woaParameters.getS3Path().toString());
        NetcdfFile s4 = NetcdfFiles.open(woaParameters.getS4Path().toString())
    ) {
      List<SeasonFile> seasons = getSeasons(localDate, s1, s2, s3, s4);
      SeasonFile startSeason = null;
      SeasonFile endSeason = null;
      for (int index = 0; index < seasons.size() - 1; index++) {
        SeasonFile seasonFile = seasons.get(index);
        SeasonFile afterSeasonFile = seasons.get(index + 1);
        if (localDate.equals(seasonFile.getLocalDate()) || localDate.isAfter(seasonFile.getLocalDate())) {
          startSeason = seasonFile;
          endSeason = afterSeasonFile;
          break;
        }
      }

      int daysInSeason = (int) ChronoUnit.DAYS.between(startSeason.getLocalDate(), endSeason.getLocalDate());
      int dayOfSeason = (int) ChronoUnit.DAYS.between(startSeason.getLocalDate(), localDate);
      double percentSeason = Math.max(0D, Math.min(1D, (double) (dayOfSeason - 1) / (double) daysInSeason));


//      NetcdfFile[] netCDFSeasons = new NetcdfFile[]{s1, s2, s3, s4};
      double[] netCDFLons = readVariableAsDoubleArray(s1, "lon");
      double[] netCDFLats = readVariableAsDoubleArray(s1, "lat");
      double[] netCDFDepths = readVariableAsDoubleArray(s1, "depth");





//      double firstTime = readTime(s1); // 373.5, 376.5, 379.5, 382.5



//      double[] netCDFTimes = new double[]{
//          readTime(s4) - 365.25,
//          readTime(s1),
//          readTime(s2),
//          readTime(s3),
//          readTime(s4),
//          readTime(s1) + 365.25
//      };
//
//      int[] timeToSeasonMappings = new int[] {3, 0, 1, 2, 3, 0};
      
      int[] lonIndices = getEncompassingIndices(netCDFLons, lon);
      int[] latIndices = getEncompassingIndices(netCDFLats, lat);
//      int[] timeMappingIndices = getEncompassingIndices(netCDFTimes, doy);
//      int[] timeIndices = new int[]{timeToSeasonMappings[timeMappingIndices[0]], timeToSeasonMappings[timeMappingIndices[1]]};
      int[] depthIndices = getMinMaxIndices(netCDFDepths, depths);


      double[] latsSlice = Arrays.copyOfRange(netCDFLats, latIndices[0], latIndices[1] + 1);
      double[] lonsSlice = Arrays.copyOfRange(netCDFLons, lonIndices[0], lonIndices[1] + 1);
      double[] depthsSlice = Arrays.copyOfRange(netCDFDepths, depthIndices[0], depthIndices[1] + 1);
//      double[] timesSlice = Arrays.copyOfRange(netCDFTimes, timeMappingIndices[0], timeMappingIndices[1] + 1);

//      NetcdfFile[] seasons = new NetcdfFile[]{netCDFSeasons[timeIndices[0]], netCDFSeasons[timeIndices[1]]};
      
      double[] interpolatedMean = interpolateVariable(
          startSeason,
          endSeason,
          percentSeason,
          "t_mn",
          depthIndices,
          latIndices,
          lonIndices,
          latsSlice,
          lonsSlice,
          depthsSlice,
          lat,
          lon,
          depths
      );
      double[] interpolatedStandardDeviation = interpolateVariable(
          startSeason,
          endSeason,
          percentSeason,
          "t_sd",
          depthIndices,
          latIndices,
          lonIndices,
          latsSlice,
          lonsSlice,
          depthsSlice,
          lat,
          lon,
          depths
      );
      int[] numberOfObservations = Arrays.stream(
              interpolateVariable(
                  startSeason,
                  endSeason,
                  percentSeason,
                  "t_dd",
                  depthIndices,
                  latIndices,
                  lonIndices,
                  latsSlice,
                  lonsSlice,
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
      SeasonFile startSeason,
      SeasonFile endSeason,
      double percentOfSeason,
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

    double[][][] startKnots = getKnots(
        startSeason.getNetCdfFile(),
        variableName,
        depthIndices,
        latIndices,
        lonIndices);

    double[][][] endKnots = getKnots(
        endSeason.getNetCdfFile(),
        variableName,
        depthIndices,
        latIndices,
        lonIndices);

    TricubicInterpolatingFunction startInterpolator = new TricubicInterpolator().interpolate(depthsSlice, latsSlice, lonsSlice, startKnots);
    TricubicInterpolatingFunction endInterpolator = new TricubicInterpolator().interpolate(depthsSlice, latsSlice, lonsSlice, endKnots);

    double[] startValues = new double[sourceDepths.length];
    for (int i = 0; i < sourceDepths.length; i++) {
      startValues[i] = startInterpolator.value(sourceDepths[i], lat, lon);
    }

    double[] endValues = new double[sourceDepths.length];
    for (int i = 0; i < sourceDepths.length; i++) {
      endValues[i] = endInterpolator.value(sourceDepths[i], lat, lon);
    }

    double[] result = new double[sourceDepths.length];

    for (int i = 0; i < sourceDepths.length; i++) {
      result[i] = new LinearInterpolator().interpolate(new double[] {0D, 1D}, new double[]{startValues[i], endValues[i]}).value(percentOfSeason);
    }

    return result;

//    double[] interpolatedValues = interpolateTime(
//        getSlice(
//            startSeason.getNetCdfFile(),
//            variableName,
//            depthIndices,
//            latIndices,
//            lonIndices
//        ),
//        getSlice(
//            endSeason.getNetCdfFile(),
//            variableName,
//            depthIndices,
//            latIndices,
//            lonIndices
//        ),
//        percentOfSeason
//    );
//
//    interpolatedValues = interpolateLatLon(
//        interpolatedValues,
//        latsSlice,
//        lonsSlice,
//        depthsSlice.length,
//        lat,
//        lon
//    );
//
//    return interpolateDepth(
//        interpolatedValues,
//        sourceDepths,
//        depthsSlice
//    );
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
    return variable.read().getDouble(0);
  }
  
  protected static double[] readVariableAsDoubleArray(NetcdfFile file, String variableName) throws IOException {
    return (double[]) findVariable(file, variableName).read().get1DJavaArray(DataType.DOUBLE);
  }

  //TODO does this support antimeridian?
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

  private static double[][][] getKnots(NetcdfFile file, String variableName, int[] depthIndices, int[] latIndices, int[] lonIndices)
      throws InvalidRangeException, IOException {
    Variable variable = findVariable(file, variableName);
    double fill = getFillValueFromVariable(variable);

    int[] netCdfShape = variable.getShape();
    int maxDepthIndex = netCdfShape[1] - 1;
    int maxLatIndex = netCdfShape[2] - 1;
    int maxLonIndex = netCdfShape[3] - 1;

    Array sliceArray = variable.read(List.of(
        Range.make(0, 0),
        Range.make(depthIndices[0], depthIndices[1]),
        Range.make(latIndices[0], latIndices[1]),
        Range.make(lonIndices[0], lonIndices[1])
    ));

    int[] shape = sliceArray.getShape();
    double[][][] knots = new double[shape[1]][shape[2]][shape[3]];

    Index index = sliceArray.getIndex();

    for (int depthIndex = 0; depthIndex < shape[1]; depthIndex++) {
      for (int latIndex = 0; latIndex < shape[2]; latIndex++) {
        for (int lonIndex = 0; lonIndex < shape[3]; lonIndex++) {
          double value = sliceArray.getDouble(index.set(0, depthIndex, latIndex, lonIndex));
          if (Precision.equals(value, fill, 0.000001d)) {
            value = interpolateNan(variable, fill, depthIndex + depthIndices[0], latIndex + latIndices[0], lonIndex + lonIndices[0], maxDepthIndex, maxLatIndex, maxLonIndex);
          }
          knots[depthIndex][latIndex][lonIndex] = value;
        }
      }
    }

    return knots;
  }

  private static double interpolateNan(Variable variable, double fill, int depthIndex, int latIndex, int lonIndex, int maxDepthIndex, int maxLatIndex, int maxLonIndex ) throws InvalidRangeException, IOException {
    int negDepthIndex = depthIndex - 1;
    double negDepthValue = Double.NaN;
    while (negDepthIndex >= 0 && Double.isNaN(negDepthValue)) {
      negDepthValue = variable.read(List.of(
          Range.make(0, 0),
          Range.make(negDepthIndex, negDepthIndex),
          Range.make(latIndex, latIndex),
          Range.make(lonIndex, lonIndex)
      )).getDouble(0);
      if (Precision.equals(negDepthValue, fill, 0.000001d)) {
        negDepthIndex--;
        negDepthValue = Double.NaN;
      }
    }

    int posDepthIndex = depthIndex + 1;
    double posDepthValue = Double.NaN;
    if(!Double.isNaN(negDepthValue)) {
      while (posDepthIndex <= maxDepthIndex && Double.isNaN(posDepthValue)) {
        posDepthValue = variable.read(List.of(
            Range.make(0, 0),
            Range.make(posDepthIndex, posDepthIndex),
            Range.make(latIndex, latIndex),
            Range.make(lonIndex, lonIndex)
        )).getDouble(0);
        if (Precision.equals(posDepthValue, fill, 0.000001d)) {
          posDepthIndex++;
          posDepthValue = Double.NaN;
        }
      }
    }

    //TODO support antimeridian
    int negLonIndex = lonIndex - 1;
    double negLonValue = Double.NaN;
    while (negLonIndex >= 0 && Double.isNaN(negLonValue)) {
      negLonValue = variable.read(List.of(
          Range.make(0, 0),
          Range.make(depthIndex, depthIndex),
          Range.make(latIndex, latIndex),
          Range.make(negLonIndex, negLonIndex)
      )).getDouble(0);
      if (Precision.equals(negLonValue, fill, 0.000001d)) {
        negLonIndex--;
        negLonValue = Double.NaN;
      }
    }

    //TODO support antimeridian
    int posLonIndex = lonIndex + 1;
    double posLonValue = Double.NaN;
    if(!Double.isNaN(negLonValue)) {
      while (posLonIndex <= maxLonIndex && Double.isNaN(posLonValue)) {
        posLonValue = variable.read(List.of(
            Range.make(0, 0),
            Range.make(depthIndex, depthIndex),
            Range.make(latIndex, latIndex),
            Range.make(posLonIndex, posLonIndex)
        )).getDouble(0);
        if (Precision.equals(posLonValue, fill, 0.000001d)) {
          posLonIndex++;
          posLonValue = Double.NaN;
        }
      }
    }


    int negLatIndex = latIndex - 1;
    double negLatValue = Double.NaN;
    while (negLatIndex >= 0 && Double.isNaN(negLatValue)) {
      negLatValue = variable.read(List.of(
          Range.make(0, 0),
          Range.make(depthIndex, depthIndex),
          Range.make(negLatIndex, negLatIndex),
          Range.make(lonIndex, lonIndex)
      )).getDouble(0);
      if (Precision.equals(negLatValue, fill, 0.000001d)) {
        negLatIndex--;
        negLatValue = Double.NaN;
      }
    }

    int posLatIndex = latIndex + 1;
    double posLatValue = Double.NaN;
    if(!Double.isNaN(negLatValue)) {
      while (posLatIndex <= maxLatIndex && Double.isNaN(posLatValue)) {
        posLatValue = variable.read(List.of(
            Range.make(0, 0),
            Range.make(depthIndex, depthIndex),
            Range.make(posLatIndex, posLatIndex),
            Range.make(lonIndex, lonIndex)
        )).getDouble(0);
        if (Precision.equals(posLatValue, fill, 0.000001d)) {
          posLatIndex++;
          posLatValue = Double.NaN;
        }
      }
    }

    List<Double> toAvg = new ArrayList<>(3);
    if (!Double.isNaN(negDepthValue) && !Double.isNaN(posDepthValue)) {
      toAvg.add(new LinearInterpolator().interpolate(new double[] {negDepthIndex, posDepthIndex}, new double[]{negDepthValue, posDepthValue}).value(depthIndex));
    }

    if (!Double.isNaN(negLonValue) && !Double.isNaN(posLonValue)) {
      toAvg.add(new LinearInterpolator().interpolate(new double[] {negLonIndex, posLonIndex}, new double[]{negLonValue, posLonValue}).value(lonIndex));
    }

    if (!Double.isNaN(negLatValue) && !Double.isNaN(posLatValue)) {
      toAvg.add(new LinearInterpolator().interpolate(new double[] {negLatIndex, posLatIndex}, new double[]{negLatValue, posLatValue}).value(latIndex));
    }

    if(toAvg.isEmpty()) {
      return Double.NaN;
    }

    return toAvg.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);

  }
  
  private static double[] getSlice(NetcdfFile file, String variableName, int[] depthIndices, int[] latIndices, int[] lonIndices)
      throws InvalidRangeException, IOException {
    Variable variable = findVariable(file, variableName);
    double fill = getFillValueFromVariable(variable);

    Array sliceArray = variable.read(List.of(
        Range.make(0, 0),
        Range.make(depthIndices[0], depthIndices[1]),
        Range.make(latIndices[0], latIndices[1]),
        Range.make(lonIndices[0], lonIndices[1])
    ));

    double[] slice = (double[]) sliceArray.get1DJavaArray(DataType.DOUBLE);

    slice = Arrays.stream(slice)
        .map(v -> {
          if (Precision.equals(v, fill, 0.000001d)) {
            return Double.NaN;
          }
          return v;
        }).toArray();

    return slice;
  }
  
  private static double[] interpolateTime(double[] startSeasonValues, double[] endSeasonValues, double percentOfSeason) {
    double[] interpolatedValues = new double[startSeasonValues.length];
    
    for (int i = 0; i < startSeasonValues.length; i++) {
      interpolatedValues[i] = new LinearInterpolator().interpolate(new double[] {0D, 1D}, new double[]{startSeasonValues[i], endSeasonValues[i]}).value(percentOfSeason);
    }
    
    return interpolatedValues;
  }

  //TODO handle antimeridian
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
