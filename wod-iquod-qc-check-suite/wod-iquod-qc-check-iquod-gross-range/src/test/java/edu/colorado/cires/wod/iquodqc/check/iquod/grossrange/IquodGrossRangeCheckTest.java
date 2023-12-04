package edu.colorado.cires.wod.iquodqc.check.iquod.grossrange;

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

class IquodGrossRangeCheckTest {
  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final IquodGrossRangeCheck check = (IquodGrossRangeCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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
  public void testIquodGrossRangeCheckFailedColdTemp() throws Exception{
    // failed to flag temperature slightly colder than -4 C
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
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
                    .withVariableCode(TEMPERATURE).withValue(-4.00000001)
                    .build()))
                .build()
        ))
        .build();

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
  public void testIquodGrossRangeCheckPassedColdTemp() throws Exception{
    // -4 OK
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
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
                    .withVariableCode(TEMPERATURE).withValue(-4)
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
  public void testIquodGrossRangeCheckFailedHotTemp() throws Exception{
    // failed to flag temperature slightly warmer than 100 C
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
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
                    .withVariableCode(TEMPERATURE).withValue(100.00000001)
                    .build()))
                .build()
        ))
        .build();

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
  public void testIquodGrossRangeCheckPassedHotTemp() throws Exception{
    // 100 OK
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
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
                    .withVariableCode(TEMPERATURE).withValue(100)
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
  public void testIquodGrossRangeCheckFailTemps() throws Exception{
    // test of a sequence of temperatures contained in a profile
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
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
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(0.0)
                    .build()))
                .build(),
            Depth.builder().withDepth(1D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(20.0)
                    .build()))
                .build(),
            Depth.builder().withDepth(2D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(-5.0)
                    .build()))
                .build(),
            Depth.builder().withDepth(3D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(16.0)
                    .build()))
                .build(),
            Depth.builder().withDepth(4D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .build()))
                .build(),
            Depth.builder().withDepth(5D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariableCode(TEMPERATURE).withValue(101.0)
                    .build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(2,5))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }
}