package edu.colorado.cires.wod.iquodqc.check.icdcaqc01.levelorder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IcdcAqc01DepthDataCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final IcdcAqc01LevelOrderCheck check = (IcdcAqc01LevelOrderCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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
  private List<Aqc02Data> getTestData() throws Exception {
    List<Aqc02Data> examples = new ArrayList<>();
    BufferedReader reader;
    Aqc02Data example = null;
    try {
      reader = Files.newBufferedReader(dataPath);
      String line = reader.readLine();
      while (line != null) {
        if (line.substring(0, 7).equals("example")) {
          if (example != null) {
            examples.add(example);
          }
          example = new Aqc02Data(line);
        } else if (line.substring(0,1).equals("[")){
          example.addrow(line);
        }
        line = reader.readLine();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    examples.add(example);
    return examples;
  }
  @Test
  public void testIcdcAqc04MaxObsDepthCheck() throws Exception {
    List<Aqc02Data> examples = getTestData();
    for (Aqc02Data example: examples){
      Cast cast = example.buildCast();
      Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
      dataset.write().mode("overwrite").parquet(TEST_PARQUET);
      boolean pass = example.getQc().size() == 0;
      CastCheckResult expected = CastCheckResult.builder()
          .withCastNumber(8888)
          .withPassed(pass)
          .withFailedDepths(example.getQc())
          .build();

      List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
      assertEquals(1, results.size());
      CastCheckResult result = results.get(0);
      assertEquals(expected, result);
    }
  }
}