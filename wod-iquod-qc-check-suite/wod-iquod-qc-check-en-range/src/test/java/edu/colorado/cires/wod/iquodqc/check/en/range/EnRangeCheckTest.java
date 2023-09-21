package edu.colorado.cires.wod.iquodqc.check.en.range;

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

class EnRangeCheckTest {
  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final EnRangeCheck check = (EnRangeCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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
  public void testEnRangeCheckFailedColdTemp() throws Exception{
    // failed to flag temperature slightly colder than -4 C
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
                    .withVariable(TEMPERATURE).withValue(-4.00000001)
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
  public void testEnRangeCheckPassedColdTemp() throws Exception{
    // -4 OK
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
                    .withVariable(TEMPERATURE).withValue(-4)
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
  public void testEnRangeCheckFailedHotTemp() throws Exception{
    // failed to flag temperature slightly warmer than 40 C
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
                    .withVariable(TEMPERATURE).withValue(40.00000001)
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
  public void testEnRangeCheckPassedHotTemp() throws Exception{
    // 40 OK
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
                    .withVariable(TEMPERATURE).withValue(40)
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
  public void testForBuddy() throws Exception {
    Cast cast = Cast.builder()
        .withCastNumber(1)
        .withLatitude(-39.889)
        .withLongitude(17.650000)
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(12D)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(9).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(15).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(21).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(27).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(33).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(39).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(45).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(51).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(57).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6600).build())).build(),
            Depth.builder().withDepth(63).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.4300).build())).build(),
            Depth.builder().withDepth(68).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.9100).build())).build(),
            Depth.builder().withDepth(74).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.6600).build())).build(),
            Depth.builder().withDepth(80).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.5300).build())).build(),
            Depth.builder().withDepth(86).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.3000).build())).build(),
            Depth.builder().withDepth(92).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.2200).build())).build(),
            Depth.builder().withDepth(98).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.1300).build())).build(),
            Depth.builder().withDepth(104).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.0400).build())).build(),
            Depth.builder().withDepth(110).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.9600).build())).build(),
            Depth.builder().withDepth(116).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.8200).build())).build(),
            Depth.builder().withDepth(122).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.7400).build())).build(),
            Depth.builder().withDepth(140).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.4300).build())).build(),
            Depth.builder().withDepth(170).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.0900).build())).build(),
            Depth.builder().withDepth(199).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.6900).build())).build(),
            Depth.builder().withDepth(229).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.2300).build())).build(),
            Depth.builder().withDepth(259).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.8300).build())).build(),
            Depth.builder().withDepth(289).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.4200).build())).build(),
            Depth.builder().withDepth(318).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.9900).build())).build(),
            Depth.builder().withDepth(348).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.4600).build())).build(),
            Depth.builder().withDepth(378).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.9400).build())).build(),
            Depth.builder().withDepth(407).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.6400).build())).build(),
            Depth.builder().withDepth(437).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.1800).build())).build(),
            Depth.builder().withDepth(467).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.7500).build())).build(),
            Depth.builder().withDepth(511).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.2200).build())).build(),
            Depth.builder().withDepth(571).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(12.7000).build())).build(),
            Depth.builder().withDepth(630).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(12.0100).build())).build(),
            Depth.builder().withDepth(690).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(11.3000).build())).build(),
            Depth.builder().withDepth(749).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.6400).build())).build(),
            Depth.builder().withDepth(808).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.0000).build())).build(),
            Depth.builder().withDepth(867).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(9.36000).build())).build(),
            Depth.builder().withDepth(927).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(8.66000).build())).build(),
            Depth.builder().withDepth(986).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(8.37000).build())).build(),
            Depth.builder().withDepth(1045).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(7.58000).build())).build(),
            Depth.builder().withDepth(1104).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(6.86000).build())).build(),
            Depth.builder().withDepth(1164).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.46000).build())).build(),
            Depth.builder().withDepth(1223).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.03000).build())).build(),
            Depth.builder().withDepth(1282).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.79000).build())).build(),
            Depth.builder().withDepth(1341).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.42000).build())).build(),
            Depth.builder().withDepth(1400).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.10000).build())).build(),
            Depth.builder().withDepth(1460).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.66000).build())).build(),
            Depth.builder().withDepth(1519).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.53000).build())).build(),
            Depth.builder().withDepth(1578).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.42000).build())).build(),
            Depth.builder().withDepth(1637).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.17000).build())).build(),
            Depth.builder().withDepth(1696).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.05000).build())).build(),
            Depth.builder().withDepth(1755).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.02000).build())).build(),
            Depth.builder().withDepth(1814).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(2.93000).build())).build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(1)
        .withPassed(true)
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }
}