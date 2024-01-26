package edu.colorado.cires.wod.iquodqc.check.cotede.tukey53H;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PRESSURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.SALINITY;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants.XBT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.Metadata;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import edu.colorado.cires.wod.parquet.model.Variable;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CoToDeTukey53HCheckSparkTest {

  private static final double[] FAILING_VALUES = {Double.NaN, 25.34, 25.34, 25.31, 24.99, 230.46, 21.85, 17.95,
      15.39, 11.08, 6.93, 7.93, 5.71, 3.58, Double.NaN};
  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final CoTeDeTukey53HCheck check = (CoTeDeTukey53HCheck) ServiceLoader.load(CastCheck.class).iterator().next();

  private static SparkSession spark;
  private static CastCheckContext context;

  @BeforeAll
  static void beforeAll() {
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
  static void afterAll() {
    spark.sparkContext().stop(0);
  }

  @BeforeEach
  void before() throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
    Files.createDirectories(TEMP_DIR);

  }

  @AfterEach
  void after() {
    FileUtils.deleteQuietly(TEMP_DIR.toFile());
  }

  @Test
  void testDigitRolloverFromCastTemperatureFailure() {
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
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build(),
            Attribute.builder()
                .withCode(PROBE_TYPE)
                .withValue(XBT)
                .build()
        ))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(
            Arrays.stream(FAILING_VALUES)
                .mapToObj(t -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(t)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(1D)
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(1D)
                            .build()
                    ))
                    .build())
                .collect(Collectors.toList())
        )
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(List.of(0, 5, 14))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  void testDigitRolloverFromCastPass() {
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
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build(),
            Attribute.builder()
                .withCode(PROBE_TYPE)
                .withValue(XBT)
                .build()
        ))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(
            Arrays.stream(FAILING_VALUES)
                .mapToObj(v -> Depth.builder().withDepth(100D)
                    .withData(List.of(
                        ProfileData.builder()
                            .withVariableCode(TEMPERATURE).withValue(ThreadLocalRandom.current().nextDouble(0, 2.5))
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(PRESSURE).withValue(ThreadLocalRandom.current().nextDouble(0, 2.5))
                            .build(),
                        ProfileData.builder()
                            .withVariableCode(SALINITY).withValue(ThreadLocalRandom.current().nextDouble(0, 2.5))
                            .build()
                    ))
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
  public void testReal() {
    Cast cast = Cast.builder()
        .withDataset("CTD")
        .withCastNumber(3320570)
        .withCruiseNumber(8518)
        .withTimestamp(45425160000L)
        .withYear(1971)
        .withMonth(6)
        .withDay(10)
        .withTime(18.1)
        .withLongitude(-145.1333)
        .withLatitude(49.9833)
        .withProfileType(0)
        .withGeohash("bbb")
        .withVariables(Arrays.asList(
            Variable.builder().withCode(1).withMetadata(Arrays.asList(Metadata.builder().withValue(5).withValue(4D).build())).build(),
            Variable.builder().withCode(2).withMetadata(Arrays.asList(Metadata.builder().withValue(5).withValue(4D).build())).build()
        ))
        .withAttributes(Arrays.asList(
            Attribute.builder().withCode(1).withValue(9600170D).build(),
            Attribute.builder().withCode(3).withValue(979D).build(),
            Attribute.builder().withCode(7).withValue(17D).build(),
            Attribute.builder().withCode(29).withValue(4D).build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(6.49).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.73).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(6.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(2).withValue(32.74).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(8.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(6.49).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.75).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(52.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(6.55).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.76).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(53.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(6.48).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.76).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(56.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(6.38).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.76).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(60.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(5.93).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.77).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(66.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(5.55).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.78).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(68.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(5.47).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.78).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(72.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(5.19).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.79).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(78.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(5.14).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.79).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(80.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.77).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.8).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(82.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.72).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.8).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(87.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.52).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.83).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(102.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.43).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.83).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(104.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.4).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.84).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(119.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.35).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.85).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(120.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.29).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.85).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(126.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.2).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.85).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(130.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.2).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.87).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(131.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.13).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.88).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(134.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.14).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.9).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(136.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.13).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(32.92).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(140.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.13).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.0).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(150.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.13).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.56).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(156.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.12).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.6).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(157.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.11).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.67).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(159.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.1).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.71).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(164.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.09).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.73).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(170.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.04).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.82).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(174.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.02).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.84).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(180.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(4.0).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.86).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(200.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(3.96).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.89).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(220.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(3.91).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.93).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(240.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(3.88).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(33.97).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(260.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(3.84).withQcFlag(0).withOriginatorsFlag(0).build(),
                ProfileData.builder().withVariableCode(2).withValue(34.01).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(280.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(3.82).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build(),
            Depth.builder().withDepth(300.0).withDepthErrorFlag(0).withOriginatorsFlag(0).withData(Arrays.asList(
                ProfileData.builder().withVariableCode(1).withValue(3.8).withQcFlag(0).withOriginatorsFlag(0).build()
            )).build()
        ))
        .build();
    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(3320570)
        .withPassed(true)
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }
}
