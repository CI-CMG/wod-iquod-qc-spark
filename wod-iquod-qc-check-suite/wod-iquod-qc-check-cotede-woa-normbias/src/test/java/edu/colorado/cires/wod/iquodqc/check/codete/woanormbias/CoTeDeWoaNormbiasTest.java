package edu.colorado.cires.wod.iquodqc.check.codete.woanormbias;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.CoTeDeWoaNormbias;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.CoTeDeWoaNormbias.WoaNormbias;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParametersReader;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.util.Precision;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

public class CoTeDeWoaNormbiasTest {
  
  private static final double[] EXPECTED_WOA_NORMBIAS = {0.01958676, 0.04645438, 0.03666594, 0.05105439, 0.11075098, 0.18418772, 0.13581132, 0.04307891, -0.01186448, -0.00556727, -5.22259791, 0.01277138, -0.23932256, -0.06257082, Double.NaN};
  private static final double[] TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final double[] DEPTHS = {2, 6, 10, 21, 44, 79, 100, 150, 200,
      400, 410, 650, 1000, 2000, 5000};
  
  private static final long TIMESTAMP = LocalDateTime.of(2016, 6, 4, 0, 0)
      .atZone(ZoneId.of("UTC"))
      .toInstant()
      .toEpochMilli();
  
  private static WoaParameters woaParameters;
  
  @BeforeAll static void beforeAll() {
    Properties properties = new Properties();
    properties.put("woa_s1.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t13_5d.nc");
    properties.put("woa_s2.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t14_5d.nc");
    properties.put("woa_s3.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t15_5d.nc");
    properties.put("woa_s4.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t16_5d.nc");
    properties.put("data.dir", "../../test-data");

    woaParameters = WoaParametersReader.loadParameters(properties);
  }

  @Test void testComputeNormbias() throws InvalidRangeException, IOException {
    assertArrayEquals(
        EXPECTED_WOA_NORMBIAS,
        CoTeDeWoaNormbias.computeNormbias(
            15,
            -38,
            TEMPERATURES,
            DEPTHS,
            TIMESTAMP,
            woaParameters
        ).stream().map(WoaNormbias::getValue).mapToDouble(v -> v).toArray(),
        /* TODO: See if we can reduce this delta given the current interpolation method. 
            The sampling distance for lat/lons in this reference data is 5 degrees, which changes interpolated values
            much more than cars normbias (sampling distance = 0.5 degrees) if not using linear interpolation (we are 
            currently using a tricubic interpolation).*/
        1e+1
    );
  }
  
  @Test void scratchTest() throws InvalidRangeException {
    double lat = 15;
    double lon = -38;
    int doy = Instant.ofEpochMilli(TIMESTAMP).atZone(ZoneOffset.UTC).getDayOfYear();
    double[] sourceDepths = DEPTHS;
    double[] temperatures = TEMPERATURES;

    try (
        NetcdfFile s1 = NetcdfFiles.open("../../test-data/woa_s1.netcdf.uri.dat");
        NetcdfFile s2 = NetcdfFiles.open("../../test-data/woa_s2.netcdf.uri.dat");
        NetcdfFile s3 = NetcdfFiles.open("../../test-data/woa_s3.netcdf.uri.dat");
        NetcdfFile s4 = NetcdfFiles.open("../../test-data/woa_s4.netcdf.uri.dat")
    ) {
      int[] lonIndices = new int[2];
      double[] lons = (double[]) s1.findVariable("lon").read().get1DJavaArray(DataType.DOUBLE);
      for (int i = 1; i < lons.length; i++) {
        double currentLon = lons[i];
        double lastLon = lons[i - 1];

        if (currentLon > lon && lastLon < lon) {
          lonIndices[0] = i - 1;
          lonIndices[1] = i;
        }
      }

      int[] latIndices = new int[2];
      double[] lats = (double[]) s1.findVariable("lat").read().get1DJavaArray(DataType.DOUBLE);
      for (int i = 1; i < lats.length; i++) {
        double currentLat = lats[i];
        double lastLat = lats[i - 1];

        if (currentLat > lat && lastLat < lat) {
          latIndices[0] = i - 1;
          latIndices[1] = i;
        }
      }

      double[] times = new double[6];
      times[0] = (365/12D) * (s4.findVariable("time").read().getDouble(0) % 12) - 365.25;
      times[1] = (365/12D) * (s1.findVariable("time").read().getDouble(0) % 12);
      times[2] = (365/12D) * (s2.findVariable("time").read().getDouble(0) % 12);
      times[3] = (365/12D) * (s3.findVariable("time").read().getDouble(0) % 12);
      times[4] = (365/12D) * (s4.findVariable("time").read().getDouble(0) % 12);
      times[5] = (365/12D) * (s1.findVariable("time").read().getDouble(0) % 12) + 365.25;

      int[] timeMappingIndices = new int[2];
      int[] timeMappings = new int[] {3, 0, 1, 2, 3, 0};
      NetcdfFile[] ts = new NetcdfFile[]{s1, s2, s3, s4};
      for (int i = 1; i < times.length; i++) {
        double currentTime = times[i];
        double lastTime = times[i - 1];

        if (currentTime > doy && lastTime < doy) {
          timeMappingIndices[0] = i - 1;
          timeMappingIndices[1] = i;
        }
      }
      int[] timeIndices = new int[]{timeMappings[timeMappingIndices[0]], timeMappings[timeMappingIndices[1]]};

      int[] depthIndices = new int[2];
      double[] depths = (double[]) s1.findVariable("depth").read().get1DJavaArray(DataType.DOUBLE);
      double minDepth = Arrays.stream(sourceDepths).min().orElseThrow(
          () -> new IllegalStateException("")
      );
      double maxDepth = Math.min(Arrays.stream(sourceDepths).max().orElseThrow(
          () -> new IllegalStateException("")
      ), Arrays.stream(depths).max().orElseThrow());
      for (int i = 1; i < depths.length; i++) {
        double currentDepth = depths[i];
        double lastDepth = depths[i - 1];
        if (lastDepth <= minDepth && currentDepth > minDepth) {
          depthIndices[0] = i - 1;
        }

        if (lastDepth < maxDepth && currentDepth >= maxDepth) {
          depthIndices[1] = i;
        }
      }


      double[] subLats = Arrays.copyOfRange(lats, latIndices[0], latIndices[1] + 1);
      double[] subLons = Arrays.copyOfRange(lons, lonIndices[0], lonIndices[1] + 1);
      double[] subDepths = Arrays.copyOfRange(depths, depthIndices[0], depthIndices[1] + 1);
      double[] subTimes = Arrays.copyOfRange(times, timeMappingIndices[0], timeMappingIndices[1] + 1);

      double[] interpolatedMean = interpolateVariable(ts[timeIndices[0]], ts[timeIndices[1]], "t_mn", depthIndices, latIndices, lonIndices, doy, subLats, subLons, subTimes, subDepths, lat, lon, sourceDepths);
      double[] interpolatedStandardDeviation = interpolateVariable(ts[timeIndices[0]], ts[timeIndices[1]], "t_sd", depthIndices, latIndices, lonIndices, doy, subLats, subLons, subTimes, subDepths, lat, lon, sourceDepths);
      int[] numberOfObservations = Arrays.stream(
              interpolateVariable(ts[timeIndices[0]], ts[timeIndices[1]], "t_dd", depthIndices, latIndices, lonIndices, doy, subLats, subLons, subTimes,
                  subDepths, lat, lon, sourceDepths))
          .mapToInt(d -> (int) Math.round(d))
          .toArray();

      double[] normBiases = IntStream.range(0, sourceDepths.length)
          .mapToDouble(i -> (temperatures[i] - interpolatedMean[i]) / interpolatedStandardDeviation[i])
          .toArray();

      List<Integer> flags = IntStream.range(0, temperatures.length)
          .filter(i -> numberOfObservations[i] >= 3)
          .filter(i -> Math.abs(normBiases[i]) > 10)
          .boxed().collect(Collectors.toList());
      
      String test = "";
    } catch (IOException | InvalidRangeException e) {
      throw new RuntimeException(e);
    }
  }

  double[] interpolateVariable(
      NetcdfFile s1,
      NetcdfFile s2,
      String variableName,
      int[] depthIndices,
      int[] latIndices,
      int[] lonIndices,
      int doy,
      double[] subLats,
      double[] subLons,
      double[] subTimes,
      double[] subDepths,
      double lat,
      double lon,
      double[] sourceDepths
  ) throws InvalidRangeException, IOException {

    Variable variable1 = s1.findVariable(variableName);
    double fill1 = variable1.findAttribute("_FillValue").getNumericValue().doubleValue();
    double[] s2mean = (double[]) variable1.read(List.of(
        Range.make(0, 0),
        Range.make(depthIndices[0], depthIndices[1]),
        Range.make(latIndices[0], latIndices[1]),
        Range.make(lonIndices[0], lonIndices[1])
    )).get1DJavaArray(DataType.DOUBLE);
    s2mean = Arrays.stream(s2mean)
        .map(v -> {
          if (Precision.equals(v, fill1, 0.000001d)) {
            return Double.NaN;
          }
          return v;
        }).toArray();


    Variable variable2 = s2.findVariable(variableName);
    double fill2 = variable2.findAttribute("_FillValue").getNumericValue().doubleValue();
    double[] s3mean = (double[]) variable2.read(List.of(
        Range.make(0, 0),
        Range.make(depthIndices[0], depthIndices[1]),
        Range.make(latIndices[0], latIndices[1]),
        Range.make(lonIndices[0], lonIndices[1])
    )).get1DJavaArray(DataType.DOUBLE);
    s3mean = Arrays.stream(s3mean)
        .map(v -> {
          if (Precision.equals(v, fill2, 0.000001d)) {
            return Double.NaN;
          }
          return v;
        }).toArray();

    double[] timeInterpolatedValues = new double[s2mean.length];
    for (int i = 0; i < s2mean.length; i++) {
      timeInterpolatedValues[i] = new LinearInterpolator().interpolate(subTimes, new double[]{s2mean[i], s3mean[i]}).value(doy);
    }

    double[] lonInterpolatedValues;
    if (subLats.length != 1 && subLons.length != 1) {
      double[][] latInterpolatedValues = new double[timeInterpolatedValues.length / 4][];
      for (int i = 0; i < timeInterpolatedValues.length; i += 4) {
        latInterpolatedValues[i / 4] = new double[]{
            new LinearInterpolator().interpolate(subLats, new double[]{timeInterpolatedValues[i], timeInterpolatedValues[i + 2]}).value(lat),
            new LinearInterpolator().interpolate(subLats, new double[]{timeInterpolatedValues[i + 1], timeInterpolatedValues[i + 3]}).value(lat)
        };
      }

      lonInterpolatedValues = new double[subDepths.length];
      for (int i = 0; i < latInterpolatedValues.length; i++) {
        lonInterpolatedValues[i] = new LinearInterpolator().interpolate(subLons, latInterpolatedValues[i]).value(lon);
      }
    } else if (subLons.length == 1 && subLats.length == 1) {
      lonInterpolatedValues = timeInterpolatedValues;
    } else if (subLons.length != 1) {
      lonInterpolatedValues = new double[subDepths.length];

      for (int i = 0; i < timeInterpolatedValues.length; i+=2) {
        lonInterpolatedValues[i / 2] =  new LinearInterpolator().interpolate(subLons, new double[]{
            timeInterpolatedValues[i], timeInterpolatedValues[i + 1]
        }).value(lon);
      }
    } else {
      lonInterpolatedValues = new double[subDepths.length];

      for (int i = 0; i < timeInterpolatedValues.length; i+=2) {
        lonInterpolatedValues[i / 2] =  new LinearInterpolator().interpolate(subLats, new double[]{
            timeInterpolatedValues[i], timeInterpolatedValues[i + 1]
        }).value(lat);
      }
    }

    double[] depthInterpolatedValues = new double[sourceDepths.length];
    PolynomialSplineFunction function = new LinearInterpolator().interpolate(subDepths, lonInterpolatedValues);
    for (int i = 0; i < sourceDepths.length; i++) {
      try {
        depthInterpolatedValues[i] = function.value(sourceDepths[i]);
      } catch (OutOfRangeException e) {
        depthInterpolatedValues[i] = Double.NaN;
      }
    }

    return depthInterpolatedValues;
  }
}
