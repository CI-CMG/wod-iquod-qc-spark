package edu.colorado.cires.wod.iquodqc.check.aoml.climatology;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.ascii.reader.BufferedCharReader;
import edu.colorado.cires.wod.ascii.reader.CastFileReader;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class AomlClimatologyCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private static final AomlClimatologyCheck check = (AomlClimatologyCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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
    properties.put("woa13_00_025.netcdf.uri", "ftp://anonymous:anonymous@ftp.aoml.noaa.gov/phod/pub/bringas/XBT/AQC/AOML_AQC_2018/data_center/woa13_00_025.nc");
    properties.put("data.dir", "../../test-data");
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

  @Disabled
  @Test
  public void testRealFailure() throws Exception {

    Cast cast;
    try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get("src/test/resources/APBO2020-19827232.ascii"))) {
      CastFileReader reader = new CastFileReader(new BufferedCharReader(bufferedReader), "APB");
      cast = AsciiToSparkModelTransformer.fromAsciiModel(reader.next());
      List<Attribute> attributes = new ArrayList<>(0);
      attributes.addAll(cast.getAttributes());
      attributes.add(
          Attribute.builder()
              .withCode(ORIGINATORS_FLAGS)
              .withValue(1)
              .build()
      );
      cast = Cast.builder(cast)
          .withAttributes(attributes)
          .build();
    }

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(19827232)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(13, 14, 15))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testStandardDataset() {
    Dataset<Cast> dataset = spark.read().parquet("file:///Users/paytoncain/Desktop/spark-wod/wod-test-parquet/wod-parquet/yearly/CTD/OBS/CTDO1971.parquet").as(Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    String test = "";
  }


}