package edu.colorado.cires.wod.iquodqc.check.icdcaqc07.spikecheck;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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

class IcdcAqc07SpikeCheckTest {
  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final IcdcAqc07SpikeCheck check = (IcdcAqc07SpikeCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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

  private final Path dataPath = Paths.get("src/test/resources/test-files/data.txt");

  private List<ICDCdata> getTestData() throws Exception {
    List<ICDCdata> icdCdata = new ArrayList<>();
    BufferedReader reader;
    try {
      reader = Files.newBufferedReader(dataPath);
      String line = reader.readLine();
      while (line != null) {
        if (line.substring(0, 2).equals("HH")) {
          ICDCdata d = new ICDCdata(line);
          for (int i = 0; i < d.getRows(); i++) {
            line = reader.readLine();
            d.addDepth(i, line);
          }
          icdCdata.add(d);
        }
        line = reader.readLine();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return icdCdata;
  }

  private Cast buildCast(ICDCdata data) {
    return Cast.builder()
        .withLatitude(data.getLatitude())
        .withLongitude(data.getLongitude())
        .withYear(data.getYear())
        .withMonth(data.getMonth())
        .withDay(data.getDay())
        .withTime(0D)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Collections.emptyList())
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(data.getCastnumber())
        .withCountry(data.getCountry())
        .withAttributes(Collections.singletonList(Attribute.builder()
            .withCode(PROBE_TYPE)
            .withValue(data.getProbeType())
            .build()))
        .withDepths(data.getDepths())
        .build();
  }

  @Test
  public void test7SpikeCheck() throws Exception {
    List<ICDCdata> data = getTestData();
    for (int i = 0; i < data.size(); i++) {
      System.out.printf("Running ICDC data group: %d\n", i);
      Cast cast = buildCast(data.get(i));
      Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
      dataset.write().mode("overwrite").parquet(TEST_PARQUET);

      CastCheckResult expected = CastCheckResult.builder()
          .withCastNumber(data.get(i).getCastnumber())
          .withPassed(data.get(i).isPass())
          .withFailedDepths(data.get(i).getFailures())
          .build();

      List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
      assertEquals(1, results.size());
      CastCheckResult result = results.get(0);
      assertEquals(expected, result);
    }

  }
}