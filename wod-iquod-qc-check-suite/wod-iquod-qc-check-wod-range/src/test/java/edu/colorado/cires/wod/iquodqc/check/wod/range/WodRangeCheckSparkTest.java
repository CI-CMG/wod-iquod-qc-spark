package edu.colorado.cires.wod.iquodqc.check.wod.range;

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

public class WodRangeCheckSparkTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final WodRangeCheck check = (WodRangeCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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
        .withLatitude(-89.5)
        .withLongitude(0.5)
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
        .withDepths(
            List.of(
                Depth.builder().withDepth(0.)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(0.)
                                .build()
                        )
                    ).build(),
                Depth.builder().withDepth(2400)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(10.)
                                .build()
                        )
                    ).build()
            )
        )
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(List.of(1))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test void testDigitRolloverFromCastPass() {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLatitude(-89.5)
        .withLongitude(0.5)
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
        .withDepths(
            List.of(
                Depth.builder().withDepth(0.)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(15.)
                                .build()
                        )
                    ).build(),
                Depth.builder().withDepth(50.)
                    .withData(List.of(
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE).withValue(-2.4)
                                .build()
                        )
                    ).build()
            )
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

}
