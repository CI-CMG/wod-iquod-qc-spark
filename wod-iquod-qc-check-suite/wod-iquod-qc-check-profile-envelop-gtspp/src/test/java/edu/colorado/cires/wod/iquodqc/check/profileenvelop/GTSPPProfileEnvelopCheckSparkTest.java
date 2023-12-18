package edu.colorado.cires.wod.iquodqc.check.profileenvelop;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PRESSURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
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

public class GTSPPProfileEnvelopCheckSparkTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final GTSPPProfileEnvelopCheck check = (GTSPPProfileEnvelopCheck) ServiceLoader.load(CastCheck.class).iterator().next();

  private static SparkSession spark;
  private static CastCheckContext context;

  @BeforeAll
  public static void beforeAll() {
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
  
  @Test void testPass() {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLatitude(-4.1)
        .withLongitude(-38.15)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(0D)
        .withCastNumber(123)
        .withDepths(
            List.of(
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(35)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(10)
                                .build()
                        )
                    )
                    .build(),
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(250)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(28)
                                .build()
                        )
                    )
                    .build(),
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(5600)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(2)
                                .build()
                        )
                    )
                    .build()
            )
        ).build();

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
  
  @Test void testFail() {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withLatitude(-4.1)
        .withLongitude(-38.15)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(0D)
        .withCastNumber(123)
        .withDepths(
            List.of(
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(35)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(38)
                                .build()
                        )
                    )
                    .build(),
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(250)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(28)
                                .build()
                        )
                    )
                    .build(),
                Depth.builder()
                    .withData(
                        List.of(
                            ProfileData.builder()
                                .withVariableCode(PRESSURE)
                                .withValue(5600)
                                .build(),
                            ProfileData.builder()
                                .withVariableCode(TEMPERATURE)
                                .withValue(10)
                                .build()
                        )
                    )
                    .build()
            )
        ).build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(List.of(0, 2))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

}
