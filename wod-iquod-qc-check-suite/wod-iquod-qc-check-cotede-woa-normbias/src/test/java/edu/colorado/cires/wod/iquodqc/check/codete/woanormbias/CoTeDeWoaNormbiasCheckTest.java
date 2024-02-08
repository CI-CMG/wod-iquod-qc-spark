package edu.colorado.cires.wod.iquodqc.check.codete.woanormbias;

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
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CoTeDeWoaNormbiasCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private static final CoTeDeWoaNormbiasCheck check = (CoTeDeWoaNormbiasCheck) ServiceLoader.load(CastCheck.class).iterator().next();

  private static SparkSession spark;
  private static CastCheckContext context;

  @BeforeAll
  public static void beforeAll() {
    spark = SparkSession
        .builder()
        .appName("test")
        .master("local[*]")
        .getOrCreate();
    Properties properties = getProperties();
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

  @NotNull
  private static Properties getProperties() {
    Properties properties = new Properties();
    properties.put("woa_s1.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t13_5d.nc");
    properties.put("woa_s2.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t14_5d.nc");
    properties.put("woa_s3.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t15_5d.nc");
    properties.put("woa_s4.netcdf.uri", "https://data.nodc.noaa.gov/woa/WOA18/DATA/temperature/netcdf/decav/5deg/woa18_decav_t16_5d.nc");
    properties.put("data.dir", "../../test-data");
    return properties;
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
  public void testInvalidPosition() {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLongitude(38)
        .withLatitude(15)
        .withTimestamp(LocalDate.of(2016, 6, 4).atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli())
        .withCastNumber(123)
        .withMonth(6)
        .withAttributes(Collections.singletonList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Collections.singletonList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(125.32)
                    .build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(true)
        .withFailedDepths(new ArrayList<>(0))
        .withSignal(Collections.singletonList(
            Double.NaN
        ))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testStandardDataset() {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLongitude(-38)
        .withLatitude(15)
        .withTimestamp(LocalDate.of(2016, 6, 4).atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli())
        .withCastNumber(123)
        .withMonth(6)
        .withAttributes(Collections.singletonList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(2D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(25.32)
                    .build()))
                .build(),
            Depth.builder().withDepth(6D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(25.34)
                    .build()))
                .build(),
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(25.34)
                    .build()))
                .build(),
            Depth.builder().withDepth(21D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(25.31)
                    .build()))
                .build(),
            Depth.builder().withDepth(44D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(24.99)
                    .build()))
                .build(),
            Depth.builder().withDepth(79D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(23.46)
                    .build()))
                .build(),
            Depth.builder().withDepth(100D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(21.85)
                    .build()))
                .build(),
            Depth.builder().withDepth(150D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(17.95)
                    .build()))
                .build(),
            Depth.builder().withDepth(200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(15.39)
                    .build()))
                .build(),
            Depth.builder().withDepth(400D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(11.08)
                    .build()))
                .build(),
            Depth.builder().withDepth(410D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(6.93)
                    .build()))
                .build(),
            Depth.builder().withDepth(650D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(7.93)
                    .build()))
                .build(),
            Depth.builder().withDepth(1000D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(5.71)
                    .build()))
                .build(),
            Depth.builder().withDepth(2000D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(3.58)
                    .build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(true)
        .withSignal(List.of(
            0.0170755008399874,
            0.04272566664546515,
            0.032660694620077604,
            0.04697188603222362,
            0.11378466871759259,
            0.21973130718802203,
            0.18378288009772245,
            0.06368034917899819,
            -0.012417439811128205,
            -0.04929516902953407,
            -5.331450876195618,
            -0.031357548742522716,
            -0.22869667883215036,
            -0.08657975849870081
        ))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }
}