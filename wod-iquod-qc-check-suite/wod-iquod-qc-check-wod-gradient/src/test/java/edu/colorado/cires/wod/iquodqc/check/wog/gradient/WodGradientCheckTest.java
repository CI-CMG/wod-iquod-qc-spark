package edu.colorado.cires.wod.iquodqc.check.wog.gradient;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants.XBT;
import static org.junit.jupiter.api.Assertions.*;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

class WodGradientCheckTest {
  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final WodGradientCheck check = (WodGradientCheck) ServiceLoader.load(CastCheck.class).iterator().next();

  private static SparkSession spark;
  private static CastCheckContext context;

  @BeforeAll
  public static void beforeAll() throws Exception {
    spark = SparkSession
        .builder()
        .appName("test")
        .master("local[*]")
        .getOrCreate();
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
    };
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
  public void testWodGradientCheckCheckPassInversion() throws Exception{
    // should just barely pass; gradient exactly at threshold
    // flagged temperature inversion at threshold
    Cast cast = Cast.builder()
        .withLatitude(0)
        .withLongitude(0)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Collections.singletonList(Attribute.builder()
            .withCode(PROBE_TYPE)
            .withValue(XBT)
            .build()))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(100D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(100)
                    .build()))
                .build(),
            Depth.builder().withDepth(200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(130)
                    .build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(true)
        .withFailedDepths(new ArrayList<>())
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testWodGradientCheckCheckFailInversion() throws Exception{
    // should just barely fail; gradient slightly over threshold
    // failed to flag slight excess temperature inversion
    Cast cast = Cast.builder()
        .withLatitude(0)
        .withLongitude(0)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Collections.singletonList(Attribute.builder()
            .withCode(PROBE_TYPE)
            .withValue(XBT)
            .build()))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(100D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(100)
                    .build()))
                .build(),
            Depth.builder().withDepth(200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(130.00001)
                    .build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(0,1))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testWodGradientCheckCheckFailedGradient() throws Exception{
    // should just barely fail; inversion slightly over threshold
    // failed to flag slight excess temperature gradient
    Cast cast = Cast.builder()
        .withLatitude(0)
        .withLongitude(0)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Collections.singletonList(Attribute.builder()
            .withCode(PROBE_TYPE)
            .withValue(XBT)
            .build()))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(100D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(100)
                    .build()))
                .build(),
            Depth.builder().withDepth(200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(29.9999)
                    .build()))
                .build(),
            Depth.builder().withDepth(300D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(0,1))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testWodGradientCheckCheckPassedGradient() throws Exception{
    // should just barely pass; gradient exactly at threshold
    // flagged temperature gradient at threshold
    Cast cast = Cast.builder()
        .withLatitude(0)
        .withLongitude(0)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Collections.singletonList(Attribute.builder()
            .withCode(PROBE_TYPE)
            .withValue(XBT)
            .build()))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(100D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(100)
                    .build()))
                .build(),
            Depth.builder().withDepth(150D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .build()))
                .build(),
            Depth.builder().withDepth(200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(30)
                    .build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(true)
        .withFailedDepths(new ArrayList<>())
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }
}
