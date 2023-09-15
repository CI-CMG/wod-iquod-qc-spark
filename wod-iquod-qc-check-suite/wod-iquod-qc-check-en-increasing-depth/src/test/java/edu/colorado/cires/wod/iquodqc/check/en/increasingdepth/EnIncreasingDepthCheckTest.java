package edu.colorado.cires.wod.iquodqc.check.en.increasingdepth;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

public class EnIncreasingDepthCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private static final EnIncreasingDepthCheck check = (EnIncreasingDepthCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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
    properties.put("EN_bgcheck_info.netcdf.uri", "https://www.metoffice.gov.uk/hadobs/en4/data/EN_bgcheck_info.nc");
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
    check.initialize(new CastCheckInitializationContext() {
      @Override
      public Properties getProperties() {
        return properties;
      }
    });
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
  public void testEnIncreasingDepthOutsideValidRangeNegative() throws Exception {

    Cast cast = Cast.builder()
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(-1D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(300D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(400D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(500D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(600D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(700D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(800D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(900D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(1000D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(8888)
                .withPassed(true)
                .build()),
        Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_check.parquet").toString());

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(0))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testEnIncreasingDepthOutsideValidRangeBigun() throws Exception {

    Cast cast = Cast.builder()
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(100D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(300D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(400D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(500D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(600D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(700D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(800D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(900D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(11001D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(8888)
                .withPassed(true)
                .build()),
        Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_check.parquet").toString());

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(9))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testEnIncreasingDepthFlagging() throws Exception {

    Cast cast = Cast.builder()
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(100D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(300D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(500D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(500D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(600D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(700D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(800D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(900D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(1000D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(8888)
                .withPassed(true)
                .build()),
        Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_check.parquet").toString());

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(3, 4))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testEnIncreasingDepthFlagging2() throws Exception {

    Cast cast = Cast.builder()
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(100D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(300D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(510D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(500D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(600D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(700D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(800D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(900D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(1000D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(8888)
                .withPassed(true)
                .build()),
        Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_check.parquet").toString());

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(3, 4))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testEnIncreasingDepthFlagging3() throws Exception {

    Cast cast = Cast.builder()
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(100D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(200D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(300D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(610D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(500D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(600D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(700D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(800D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(900D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(1000D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(8888)
                .withPassed(true)
                .build()),
        Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_check.parquet").toString());

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(3))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testEnIncreasingDepthAllZero() throws Exception {

    Cast cast = Cast.builder()
        .withCastNumber(8888)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build(),
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0D).build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(
        Collections.singletonList(
            CastCheckResult.builder()
                .withCastNumber(8888)
                .withPassed(true)
                .build()),
        Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_check.parquet").toString());

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

}