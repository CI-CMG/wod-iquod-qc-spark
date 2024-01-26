package edu.colorado.cires.wod.iquodqc.check.iquod.bottom;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.*;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.Metadata;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import edu.colorado.cires.wod.parquet.model.Variable;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class IquodBottomCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private static final IquodBottomCheck check = (IquodBottomCheck) ServiceLoader.load(CastCheck.class).iterator().next();

  private static SparkSession spark;
  private static CastCheckContext context;

  @BeforeAll
  public static void beforeAll() throws Exception {
    spark = SparkSession
        .builder()
        .appName("test")
        .master("local[*]")
        .getOrCreate();
    Properties properties = new Properties();
    properties.put("etopo5.netcdf.uri",
        "https://pae-paha.pacioos.hawaii.edu/thredds/ncss/etopo5?var=ROSE&disableLLSubset=on&disableProjSubset=on&horizStride=1&addLatLon=true");
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
  public static void afterAll() throws Exception {
    spark.sparkContext().stop(0);
  }

  @BeforeEach
  public void before() throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
    Files.createDirectories(TEMP_DIR);
  }

  @AfterEach
  public void after() throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
  }

  @Test
  public void testStandardDataset() throws Exception {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLongitude(-38)
        .withLatitude(15)
        .withTimestamp(LocalDate.of(2016, 6, 4).atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli())
        .withCastNumber(123)
        .withMonth(6)
        .withAttributes(Arrays.asList(
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
                .build(),
            Depth.builder().withDepth(5200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(2.5)
                    .build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(14))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testReal() {

    Cast cast = Cast.builder()
        .withDataset("CTD")
        .withCastNumber(13333902)
        .withCruiseNumber(0)
        .withTimestamp(35017200000L)
        .withYear(1971)
        .withMonth(2)
        .withDay(10)
        .withTime(7D)
        .withLongitude(-78.05)
        .withLatitude(17.1)
        .withProfileType(0)
        .withGeohash("d70")
        .withVariables(Arrays.asList(
            Variable.builder().withCode(1).withMetadata(Arrays.asList(Metadata.builder().withCode(5).withValue(4D).build())).build()
        ))
        .withAttributes(Arrays.asList(
            Attribute.builder().withCode(1).withValue(64432D).build(),
            Attribute.builder().withCode(4).withValue(1449D).build(),
            Attribute.builder().withCode(8).withValue(2D).build(),
            Attribute.builder().withCode(29).withValue(4D).build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder()
                .withDepth(0.0)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(26.389).build()
                )).build(),
            Depth.builder()
                .withDepth(91.4)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(26.389).build()
                )).build(),
            Depth.builder()
                .withDepth(109.7)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(25.556).build()
                )).build(),
            Depth.builder()
                .withDepth(121.9)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(24.444).build()
                )).build(),
            Depth.builder()
                .withDepth(146.3)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(23.333).build()
                )).build(),
            Depth.builder()
                .withDepth(158.5)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(22.222).build()
                )).build(),
            Depth.builder()
                .withDepth(182.9)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(21.111).build()
                )).build(),
            Depth.builder()
                .withDepth(195.1)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(20.0).build()
                )).build(),
            Depth.builder()
                .withDepth(219.5)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(18.889).build()
                )).build(),
            Depth.builder()
                .withDepth(231.6)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(17.778).build()
                )).build(),
            Depth.builder()
                .withDepth(283.5)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(16.667).build()
                )).build(),
            Depth.builder()
                .withDepth(335.3)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(15.0).build()
                )).build(),
            Depth.builder()
                .withDepth(396.2)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(12.778).build()
                )).build(),
            Depth.builder()
                .withDepth(457.2)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withOriginatorsFlag(0).withQcFlag(0).withVariableCode(1).withValue(10.833).build()
                )).build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder().withCastNumber(13333902).withPassed(false).withFailedDepths(Arrays.asList(9, 10, 11, 12, 13)).build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);

  }
}