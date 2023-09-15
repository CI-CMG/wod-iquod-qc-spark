package edu.colorado.cires.wod.iquodqc.check.en.backgroundcheck;


import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
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

public class BackgroundCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private static final BackgroundCheck check = (BackgroundCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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
  public void testEnBackgroundCheckTemperature() throws Exception {
    /*
      p = util.testingProfile.fakeProfile([1.8, 1.8, 1.8, 7.1], [0.0, 2.5, 5.0, 7.5], latitude=55.6, longitude=12.9, date=[1900, 1, 15, 0], probe_type=7, uid=8888)
        qctests.EN_background_check.prepare_data_store(self.data_store)
        qc = qctests.EN_background_check.test(p, self.parameters, self.data_store)
        expected = [False, False, False, True]
        assert numpy.array_equal(qc, expected), 'mismatch between qc results and expected values'
     */
    Cast cast = Cast.builder()
        .withLatitude(55.6)
        .withLongitude(12.9)
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Collections.emptyList())
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(8888)
        .withAttributes(Collections.singletonList(Attribute.builder().withCode(PROBE_TYPE).withValue(7D).build()))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0.0)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(1.8)
                    .build()))
                .build(),
            Depth.builder().withDepth(2.5)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(1.8).build()))
                .build(),
            Depth.builder().withDepth(5.0)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(1.8).build()))
                .build(),
            Depth.builder().withDepth(7.5)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withOriginatorsFlag(0).withQcFlag(0)
                    .withVariable(TEMPERATURE).withValue(7.1).build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(
        Collections.singletonList(CastCheckResult.builder().withCastNumber(8888).withPassed(false).withFailedDepths(Collections.singletonList(3)).build()),
        Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_suspect.parquet").toString());

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


}