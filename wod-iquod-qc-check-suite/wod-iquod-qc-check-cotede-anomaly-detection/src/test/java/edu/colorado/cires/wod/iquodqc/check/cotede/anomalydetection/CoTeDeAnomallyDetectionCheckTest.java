package edu.colorado.cires.wod.iquodqc.check.cotede.anomalydetection;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CoTeDeAnomallyDetectionCheckTest {

  private static final double[] TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95, 15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final double[] FAILING_TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 30, 23.46, 21.85, 17.95, 15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final double[] DEPTHS = {   2,    6,   10,   21,   44,   79,  100,  150,  200,
      400,  410,  650, 1000, 2000, 5000};
  private static final double LATITUDE = 15;
  private static final double LONGITUDE = -38;
  private static final long TIMESTAMP = LocalDateTime.of(2016, 6, 4, 0, 0).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private static final CoTeDeAnomalyDetectionCheck check = (CoTeDeAnomalyDetectionCheck) ServiceLoader.load(CastCheck.class).iterator().next();

  private static SparkSession spark;
  private static CastCheckContext context;

  @BeforeAll
  public static void beforeAll() {
    spark = SparkSession
        .builder()
        .appName("test")
        .master("local[*]")
        .getOrCreate();
    Properties properties = new Properties();
    properties.put("woa_s1.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t13_5d.nc");
    properties.put("woa_s2.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t14_5d.nc");
    properties.put("woa_s3.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t15_5d.nc");
    properties.put("woa_s4.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t16_5d.nc");
    properties.put("data.dir", "../../test-data");
    context = new CastCheckContext() {
      @Override
      public SparkSession getSparkSession() {
        return spark;
      }

      @Override
      public Dataset<Cast> readCastDataset() {
        return spark.read().parquet(TEST_PARQUET).as(Encoders.bean(Cast.class));
      }

      @Override
      public Dataset<CastCheckResult> readCastCheckResultDataset(String checkName) {
        throw new UnsupportedOperationException("not implemented for test");
      }

      @Override
      public Properties getProperties() {
        return properties;
      }
    };
    check.initialize(() -> properties);
  }

  @AfterAll
  public static void afterAll() {
    spark.sparkContext().stop(0);
  }

  @BeforeEach
  public void before() throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
    Files.createDirectories(TEMP_DIR);
  }

  @AfterEach
  public void after() {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
  }

  @Test
  public void testStandardDatasetPass() {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLongitude(LONGITUDE)
        .withLatitude(LATITUDE)
        .withTimestamp(TIMESTAMP)
        .withCastNumber(123)
        .withMonth(6)
        .withAttributes(Collections.singletonList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(
            IntStream.range(0, TEMPERATURES.length).boxed()
                .map(i -> Depth.builder().withDepth(DEPTHS[i])
                    .withData(Collections.singletonList(ProfileData.builder()
                        .withOriginatorsFlag(0).withQcFlag(0)
                        .withVariableCode(TEMPERATURE).withValue(TEMPERATURES[i])
                        .build()))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(true)
        .withFailedDepths(Collections.emptyList())
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testStandardDatasetFail() {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLongitude(LONGITUDE)
        .withLatitude(LATITUDE)
        .withTimestamp(TIMESTAMP)
        .withCastNumber(123)
        .withMonth(6)
        .withAttributes(Collections.singletonList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(
            IntStream.range(0, FAILING_TEMPERATURES.length).boxed()
                .map(i -> Depth.builder().withDepth(DEPTHS[i])
                    .withData(Collections.singletonList(ProfileData.builder()
                        .withOriginatorsFlag(0).withQcFlag(0)
                        .withVariableCode(TEMPERATURE).withValue(FAILING_TEMPERATURES[i])
                        .build()))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(Collections.singletonList(4))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

}
