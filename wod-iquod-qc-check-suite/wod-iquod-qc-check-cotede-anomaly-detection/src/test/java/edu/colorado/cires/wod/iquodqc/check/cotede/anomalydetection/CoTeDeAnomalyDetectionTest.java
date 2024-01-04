package edu.colorado.cires.wod.iquodqc.check.cotede.anomalydetection;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsGetter;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParameters;
import edu.colorado.cires.wod.iquodqc.check.cotede.carsnormbias.refdata.CarsParametersReader;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaGetter;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParameters;
import edu.colorado.cires.wod.iquodqc.common.refdata.cotede.WoaParametersReader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CoTeDeAnomalyDetectionTest {
  
  private static final double[] TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95, 15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final double[] DEPTHS = {   2,    6,   10,   21,   44,   79,  100,  150,  200,
      400,  410,  650, 1000, 2000, 5000};
  
  private static final double[] EXPECTED_COMBINED_PROBABILITIES = {0.0, 0.0, -0.23157947207232124, -2.6488964615385746, -9.473933624327975, -1.6923874035379025, -12.330604621573432, 0.0, -11.060933449655789, 0.0, -11.944071864368553, -16.039987990243688, 0.0, 0.0, 0.0};
  private static final double[] EXPECTED_GRADIENT_PROB = {Double.NaN, 0.0, -0.23157947207232124, -2.6488964615385746, -4.916622075070896, -1.1530672299030673, -6.205209584292135, 0.0, -5.6379292141032, 0.0, 0.0, -6.97884790313254, 0.0, Double.NaN, Double.NaN};

  private static final double[] EXPECTED_SPIKE_PROB = {Double.NaN, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -4.502240302430026, -4.502240302430026, 0.0, Double.NaN, Double.NaN};

  private static final double[] EXPECTED_TUKEY_PROB = {Double.NaN, Double.NaN, Double.NaN, Double.NaN, -4.557311549257078, -0.5393201736348352, -6.125395037281296, 0.0, -5.423004235552588, 0.0, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN};

  private static final double[] EXPECTED_RATE_OF_CHANGE_PROB = {Double.NaN, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -4.55889978468112, 0.0, 0.0, Double.NaN};

  private static final double[] EXPECTED_WOA_NORM_BIAS_PROB = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -2.6185526839834155, 0.0, 0.0, 0.0, Double.NaN};

  private static final double[] EXPECTED_CARS_NORM_BIAS_PROB = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -4.791718000267658, 0.0, 0.0, 0.0, Double.NaN};

  private static final double[] EXPECTED_CONSTANT_CLUSTER_SIZE_PROB = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
  
  private static final long TIMESTAMP = LocalDateTime.of(2016, 6, 4, 0, 0).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
  private static final double LATITUDE = 15;
  private static final double LONGITUDE = -38;
  private static WoaGetter woaGetter;
  private static CarsGetter carsGetter;
  
  @BeforeAll static void beforeAll() {
    Properties properties = new Properties();
    properties.put("woa_s1.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t13_5d.nc");
    properties.put("woa_s2.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t14_5d.nc");
    properties.put("woa_s3.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t15_5d.nc");
    properties.put("woa_s4.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t16_5d.nc");
    properties.put(CarsParametersReader.CARS_NC_PROP, "https://www.marine.csiro.au/atlas/export/temperature_cars2009a.nc.gz");
    properties.put("data.dir", "../../test-data");

    WoaParameters woaParameters = WoaParametersReader.loadParameters(properties);
    CarsParameters carsParameters = CarsParametersReader.loadParameters(properties);
    woaGetter = new WoaGetter(woaParameters);
    carsGetter = new CarsGetter(carsParameters);
  }
  
  @Test void testComputeAnomaly() {
    assertArrayEquals(
        EXPECTED_COMBINED_PROBABILITIES,
        CoTeDeAnomalyDetection.computeAnomallyProbabilities(TEMPERATURES, DEPTHS, TIMESTAMP, LATITUDE, LONGITUDE, woaGetter, carsGetter),
        1e-1
    );
  }
  
  @Test void  testComputeGradientProb() {
    assertArrayEquals(
        EXPECTED_GRADIENT_PROB,
        CoTeDeAnomalyDetection.processGradient(TEMPERATURES),
        1e-14
    );
  }
  
  @Test void testComputeSpikeProb() {
    assertArrayEquals(
        EXPECTED_SPIKE_PROB,
        CoTeDeAnomalyDetection.processSpike(TEMPERATURES),
        1e-14
    ); 
  }
  
  @Test void  testComputeTukeyNormProb() {
    assertArrayEquals(
        EXPECTED_TUKEY_PROB,
        CoTeDeAnomalyDetection.processTukeyNorm(TEMPERATURES),
        1e-14
    );
  }
  
  @Test void testComputeRateOfChangeProb() {
    assertArrayEquals(
      EXPECTED_RATE_OF_CHANGE_PROB,
      CoTeDeAnomalyDetection.processRateOfChange(TEMPERATURES),
        1e-14
    );
  }
  
  @Test void testComputeWoaNormBiasProb() {
    assertArrayEquals(
        EXPECTED_WOA_NORM_BIAS_PROB,
        CoTeDeAnomalyDetection.processWoaNormBias(
            TEMPERATURES, DEPTHS, TIMESTAMP, LATITUDE, LONGITUDE, woaGetter
        )
    );
  }
  
  @Test void testComputeCarsNormBiasProb() {
    assertArrayEquals(
        EXPECTED_CARS_NORM_BIAS_PROB,
        CoTeDeAnomalyDetection.processCarsNormBias(TEMPERATURES, DEPTHS, LATITUDE, LONGITUDE, carsGetter),
        1e-1
    );
  }
  
  @Test void testComputeConstantClusterSizeProb() {
    assertArrayEquals(
        EXPECTED_CONSTANT_CLUSTER_SIZE_PROB,
        CoTeDeAnomalyDetection.processConstantClusterSize(TEMPERATURES)
    );
  }
  
  @Test void testExpectedFlags() {
    assertEquals(
        Collections.singletonList(11),
        CoTeDeAnomalyDetection.checkFlags(
          TEMPERATURES, DEPTHS, TIMESTAMP, LATITUDE, LONGITUDE, woaGetter, carsGetter, -16.0
        )
    );
  }
}
