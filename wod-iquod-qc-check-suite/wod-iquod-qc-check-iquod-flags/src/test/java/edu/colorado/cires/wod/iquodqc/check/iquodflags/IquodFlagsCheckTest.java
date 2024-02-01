package edu.colorado.cires.wod.iquodqc.check.iquodflags;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.iquodqc.common.ProbeTypeConstants;
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
import org.jetbrains.annotations.NonNls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class IquodFlagsCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private static final IquodFlagsCheck check = (IquodFlagsCheck) ServiceLoader.load(CastCheck.class).iterator().next();
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
      "HTPR_check",
      "LTPR_check",
      "COMP_check",
      ""
  })
  public void testBasicProbeType(@NonNls String failingTest) {
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

    int fillValue;

    if (CheckNames.HIGH_TRUE_POSITIVE_RATE_GROUP.getName().equals(failingTest)) {
      fillValue = 2;
    } else if (CheckNames.COMPROMISE_GROUP.getName().equals(failingTest)) {
      fillValue = 3;
    } else if (CheckNames.LOW_TRUE_POSITIVE_RATE_GROUP.getName().equals(failingTest)) {
      fillValue = 4;
    } else {
      fillValue = 1;
    }

    List<Integer> expectedIquodFlags = IntStream.range(0, cast.getDepths().size()).boxed()
        .map(i -> failedDepths.contains(i) ? fillValue : 1)
        .collect(Collectors.toList());

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(failingTest.isBlank())
        .withFailedDepths(failingTest.isBlank() ? Collections.emptyList() : failedDepths)
        .withIquodFlags(expectedIquodFlags)
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "HTPR_check",
      "LTPR_check",
      "COMP_check",
      ""
  })
  public void testXBTProbeType(@NonNls String failingTest) {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLongitude(LONGITUDE)
        .withLatitude(LATITUDE)
        .withTimestamp(TIMESTAMP)
        .withCastNumber(123)
        .withMonth(6)
        .withAttributes(List.of(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1.0)
                .build(),
            Attribute.builder()
                .withCode(PROBE_TYPE)
                .withValue(ProbeTypeConstants.XBT)
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
        5, 13
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

    int fillValue;

    if (CheckNames.HIGH_TRUE_POSITIVE_RATE_GROUP.getName().equals(failingTest)) {
      fillValue = 2;
    } else if (CheckNames.COMPROMISE_GROUP.getName().equals(failingTest)) {
      fillValue = 3;
    } else if (CheckNames.LOW_TRUE_POSITIVE_RATE_GROUP.getName().equals(failingTest)) {
      fillValue = 4;
    } else {
      fillValue = 1;
    }

    List<Integer> expectedIquodFlags = IntStream.range(0, cast.getDepths().size()).boxed()
        .map(i -> i >= 5 ? fillValue : 1)
        .collect(Collectors.toList());

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(failingTest.isBlank())
        .withFailedDepths(failingTest.isBlank() ? Collections.emptyList() : failedDepths)
        .withIquodFlags(expectedIquodFlags)
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testBasicProbeTypeMixedFailures() {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLongitude(LONGITUDE)
        .withLatitude(LATITUDE)
        .withTimestamp(TIMESTAMP)
        .withCastNumber(123)
        .withMonth(6)
        .withAttributes(List.of(
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

    Dataset<CastCheckResult> testDependencyResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(123)
                .withPassed(false)
                .withFailedDepths(List.of(
                    4, 10, 11
                )).build()
        ),
        Encoders.bean(CastCheckResult.class)
    );

    testDependencyResult.write().parquet(TEMP_DIR.resolve(String.format("%s.parquet", CheckNames.HIGH_TRUE_POSITIVE_RATE_GROUP.getName())).toString());

    testDependencyResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(123)
                .withPassed(false)
                .withFailedDepths(List.of(
                    2, 5, 11
                )).build()
        ),
        Encoders.bean(CastCheckResult.class)
    );

    testDependencyResult.write().parquet(TEMP_DIR.resolve(String.format("%s.parquet", CheckNames.COMPROMISE_GROUP.getName())).toString());

    testDependencyResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(123)
                .withPassed(false)
                .withFailedDepths(List.of(
                    1, 5, 11, 12
                )).build()
        ),
        Encoders.bean(CastCheckResult.class)
    );

    testDependencyResult.write().parquet(TEMP_DIR.resolve(String.format("%s.parquet", CheckNames.LOW_TRUE_POSITIVE_RATE_GROUP.getName())).toString());

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(List.of(
            1, 2, 4, 5, 10, 11, 12
        )).withIquodFlags(List.of(
            1, 4, 3, 1, 2, 4, 1, 1, 1, 1, 2, 4, 4, 1, 1
        )).build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testXBTProbeTypeMixedFailures() {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLongitude(LONGITUDE)
        .withLatitude(LATITUDE)
        .withTimestamp(TIMESTAMP)
        .withCastNumber(123)
        .withMonth(6)
        .withAttributes(List.of(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1.0)
                .build(),
            Attribute.builder()
                .withCode(PROBE_TYPE)
                .withValue(ProbeTypeConstants.XBT)
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

    Dataset<CastCheckResult> testDependencyResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(123)
                .withPassed(false)
                .withFailedDepths(List.of(
                    4, 7, 11, 12
                )).build()
        ),
        Encoders.bean(CastCheckResult.class)
    );

    testDependencyResult.write().parquet(TEMP_DIR.resolve(String.format("%s.parquet", CheckNames.HIGH_TRUE_POSITIVE_RATE_GROUP.getName())).toString());

    testDependencyResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(123)
                .withPassed(false)
                .withFailedDepths(List.of(
                    7, 11, 12
                )).build()
        ),
        Encoders.bean(CastCheckResult.class)
    );

    testDependencyResult.write().parquet(TEMP_DIR.resolve(String.format("%s.parquet", CheckNames.COMPROMISE_GROUP.getName())).toString());

    testDependencyResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(123)
                .withPassed(false)
                .withFailedDepths(List.of(
                    11, 12
                )).build()
        ),
        Encoders.bean(CastCheckResult.class)
    );

    testDependencyResult.write().parquet(TEMP_DIR.resolve(String.format("%s.parquet", CheckNames.LOW_TRUE_POSITIVE_RATE_GROUP.getName())).toString());

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(List.of(
            4, 7, 11, 12
        )).withIquodFlags(List.of(
            1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4
        )).build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

}
