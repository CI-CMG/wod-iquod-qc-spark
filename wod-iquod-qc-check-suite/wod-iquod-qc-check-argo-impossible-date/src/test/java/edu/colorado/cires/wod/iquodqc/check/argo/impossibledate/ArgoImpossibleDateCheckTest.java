package edu.colorado.cires.wod.iquodqc.check.argo.impossibledate;


import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants.XBT;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ArgoImpossibleDateCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final ArgoImpossibleDateCheck check = (ArgoImpossibleDateCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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
  public void testArgoImpossibleDateTestYear() throws Exception {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withYear((short) 1699)
        .withMonth((short) 1)
        .withDay((short) 1)
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(1D)
                    .build()))
                .build()
        ))
        .build();

//    List<Boolean> expected = Collections.singletonList(true);

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(0))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Disabled //This test is impossible with the global test filter
  @Test
  public void testArgoImpossibleDateTestMonth() throws Exception {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withYear((short) 2001)
        .withMonth((short) 0)
        .withDay((short) 1)
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(1D)
                    .build()))
                .build()
        ))
        .build();

//    List<Boolean> expected = Collections.singletonList(true);

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(0))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testArgoImpossibleDateTestDayBasic() throws Exception {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withYear((short) 2001)
        .withMonth((short) 2)
        .withDay((short) 29)
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(1D)
                    .build()))
                .build()
        ))
        .build();

//    List<Boolean> expected = Collections.singletonList(true);

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(0))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testArgoImpossibleDateTestDayLeapYear() throws Exception {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withYear((short) 2004)
        .withMonth((short) 2)
        .withDay((short) 29)
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(1D)
                    .build()))
                .build(),
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(-200D)
                    .build()))
                .build(),
            Depth.builder().withDepth(20D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(3D).build()))
                .build()
        ))
        .build();

//    List<Boolean> expected = Collections.singletonList(false);

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(true)
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testArgoImpossibleDateTestHour() throws Exception {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withYear((short) 2004)
        .withMonth((short) 2)
        .withDay((short) 29)
        .withTime(24D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(1D)
                    .build()))
                .build()
        ))
        .build();

//    List<Boolean> expected = Collections.singletonList(true);

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(0))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testArgoImpossibleDateTestHourMissing() throws Exception {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withYear((short) 2004)
        .withMonth((short) 1)
        .withDay((short) 29)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(1D)
                    .build()))
                .build(),
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(-200D)
                    .build()))
                .build(),
            Depth.builder().withDepth(20D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(3D).build()))
                .build()
        ))
        .build();

//    List<Boolean> expected = Collections.singletonList(false);

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(true)
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }
}