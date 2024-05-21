package edu.colorado.cires.wod.iquodqc.check.lfprgroup;

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
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NonNls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class LowFalsePositiveRateGroupCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private static final LowFalsePositiveRateGroupCheck check = (LowFalsePositiveRateGroupCheck) ServiceLoader.load(CastCheck.class).iterator().next();
  private static SparkSession spark;
  private static CastCheckContext context;
  private static final double LATITUDE = 15.0;
  private static final double LONGITUDE = -38.0;
  private static final long TIMESTAMP = LocalDateTime.of(2016, 6, 4, 0, 0).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
  private static final double[] DEPTHS = {2.0, 6.0, 10.0, 21.0, 44.0, 79.0, 100.0, 150.0, 200.0,
      400.0, 410.0, 650.0, 1000.0, 2000.0, 5000.0};
  private static final double[] TEMPERATURES = {25.32, 25.34, 25.34, 25.31, 24.99, 23.46, 21.85, 17.95, 15.39, 11.08, 6.93, 7.93, 5.71, 3.58,
      Double.NaN};

  @BeforeAll
  public static void beforeAll() {
    spark = SparkSession
        .builder()
        .appName("test")
        .master("local[*]")
        .getOrCreate();
    Properties properties = new Properties();
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
        return spark.read().parquet(TEMP_DIR.resolve(checkName + ".parquet").toString()).as(Encoders.bean(CastCheckResult.class));
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

  @ParameterizedTest
  @ValueSource(strings = {
      "Argo_impossible_date_test",
      "Argo_impossible_location_test",
      "wod_loose_location_at_sea_check",
      "ICDC_aqc_01_level_order",
      "IQuOD_gross_range_check",
      "ICDC_aqc_02_crude_range",
      "EN_background_check",
      "EN_std_lev_bkg_and_buddy_check",
      "EN_increasing_depth_check",
      "ICDC_aqc_05_stuck_value",
      "EN_spike_and_step_check",
      "CSIRO_long_gradient",
      "EN_stability_check",
      ""
  })
  public void test(@NonNls String failingTest) {
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
                .withValue(1.0)
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

    List<Integer> failedDepths = List.of(
        1, 3, 5, 7, 9, 11, 13
    );

    for (String testDependency : check.dependsOn()) {
      Dataset<CastCheckResult> testDependencyResult;
      if (testDependency.equals(failingTest)) {
        testDependencyResult = spark.createDataset(
            Collections.singletonList(
                CastCheckResult.builder()
                    .withCastNumber(123)
                    .withPassed(false)
                    .withFailedDepths(failedDepths)
                    .build()
            ),
            Encoders.bean(CastCheckResult.class)
        );
      } else {
        testDependencyResult = spark.createDataset(
            Collections.singletonList(
                CastCheckResult.builder()
                    .withCastNumber(123)
                    .withPassed(true)
                    .build()
            ),
            Encoders.bean(CastCheckResult.class)
        );
      }

      testDependencyResult.write().parquet(TEMP_DIR.resolve(String.format("%s.parquet", testDependency)).toString());
    }

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(failingTest.isBlank())
        .withFailedDepths(failingTest.isBlank() ? Collections.emptyList() : failedDepths)
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

}
