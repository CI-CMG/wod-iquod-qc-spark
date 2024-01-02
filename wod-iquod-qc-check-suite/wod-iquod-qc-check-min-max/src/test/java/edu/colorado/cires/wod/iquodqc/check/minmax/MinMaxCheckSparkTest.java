package edu.colorado.cires.wod.iquodqc.check.minmax;

import static edu.colorado.cires.wod.iquodqc.check.minmax.refdata.MinMaxParametersReader.WOD_INFO_DGG4H6_PROP;
import static edu.colorado.cires.wod.iquodqc.check.minmax.refdata.MinMaxParametersReader.WOD_TEMP_MIN_MAX_PROP;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PRESSURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.SALINITY;
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
import java.util.Arrays;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MinMaxCheckSparkTest {
  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private static final MinMaxCheck check = (MinMaxCheck) ServiceLoader.load(CastCheck.class).iterator().next();

  private static SparkSession spark;
  private static CastCheckContext context;

  @BeforeAll
  public static void beforeAll() {
    spark = SparkSession
        .builder()
        .appName("test")
        .master("local[*]")
        .getOrCreate();
    Properties properties = new Properties();
    properties.put(WOD_TEMP_MIN_MAX_PROP, "https://auto-qc-data.s3.us-west-2.amazonaws.com/TEMP_MIN_MAX.nc");
    properties.put(WOD_INFO_DGG4H6_PROP, "https://auto-qc-data.s3.us-west-2.amazonaws.com/info_DGG4H6.mat");
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
        throw new UnsupportedOperationException("not implemented for test");
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

  private static final double FLAG_LONGITUDE = 61.856;
  private static final double FLAG_LATITUDE = 9.379000000000001;
  private static final double[] FLAG_TEMP = {29.345, 29.329, 29.293, 29.228, 29.133, 29.023, 28.881, 28.469, 28.255, 27.758, 27.258, 27.038, 26.721, 26.256, 25.924, 25.504, 24.523, 23.632, 21.634, 21.003, 20.387, 19.852, 18.964, 18.036, 17.444, 16.928, 16.634, 16.214, 15.784, 15.319, 14.946, 14.696, 14.522, 14.429, 14.144, 13.928, 13.758, 13.569, 13.476, 13.278, 12.546, 12.332, 12.21, 12.08, 11.884, 11.788, 11.783, 11.625, 11.592, 11.536, 11.455, 11.388, 11.194, 10.881, 10.49, 9.963, 9.621, 9.216, 9.036, 8.851, 8.592, 8.075, 7.741, 7.557, 7.293, 7.087, 6.831, 6.463, 6.151, 5.6, 5.212, 4.662, 4.215};
  private static final double[] FLAG_PRES = {4.2, 10.0, 15.4, 20.1, 25.0, 29.8, 35.0, 40.4, 45.3, 50.6, 60.4, 69.1, 79.4, 89.4, 98.7, 110.3, 120.2, 130.8, 139.6, 150.2, 160.2, 170.4, 180.1, 189.7, 200.0, 210.4, 218.6, 230.5, 240.5, 250.5, 260.1, 270.2, 279.1, 287.0, 300.2, 310.2, 320.2, 330.0, 339.9, 349.9, 375.1, 400.4, 424.7, 450.0, 474.6, 499.2, 524.1, 549.9, 574.6, 600.3, 625.3, 650.5, 700.4, 749.7, 800.3, 850.4, 900.4, 950.1, 999.9, 1049.3, 1099.5, 1150.6, 1200.4, 1249.4, 1299.6, 1350.4, 1400.2, 1450.2, 1500.1, 1599.9, 1699.5, 1800.1, 1900.2};
  private static final double[] FLAG_PSAL = {36.199, 36.177, 36.143, 36.115, 36.087, 36.075, 36.047, 36.087, 36.098, 36.029, 35.994, 35.937, 35.871, 35.916, 35.942, 35.773, 35.856, 35.77, 35.815, 35.814, 35.848, 35.791, 35.714, 35.835, 35.867, 35.931, 35.949, 35.987, 35.978, 35.983, 36.05, 36.078, 36.135, 36.134, 36.142, 36.16, 36.116, 36.122, 36.06, 36.025, 36.177, 36.215, 36.231, 36.201, 36.22, 36.266, 36.197, 36.23, 36.236, 36.23, 36.227, 36.254, 36.175, 36.179, 36.128, 36.164, 36.148, 36.198, 36.182, 36.168, 36.146, 36.14, 36.172, 36.164, 36.168, 36.175, 36.128, 36.144, 36.183, 36.15, 36.105, 36.123, 36.098};
  private static final List<Integer> FLAG_RESULT = List.of(59, 61, 65, 66, 67, 68, 69, 70, 71, 72);
  
  @Test void testFlag() {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withMonth(1)
        .withLongitude(FLAG_LONGITUDE)
        .withLatitude(FLAG_LATITUDE)
        .withPrincipalInvestigators(Collections.emptyList())
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withBiologicalAttributes(Collections.emptyList())
        .withTaxonomicDatasets(Collections.emptyList())
        .withCastNumber(123)
        .withDepths(IntStream.range(0, FLAG_PSAL.length).boxed().map(
            i -> Depth.builder()
                .withDepth(i * 2D)
                .withData(List.of(
                    ProfileData.builder()
                        .withVariableCode(TEMPERATURE)
                        .withValue(FLAG_TEMP[i])
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(PRESSURE)
                        .withValue(FLAG_PRES[i])
                        .build(),
                    ProfileData.builder()
                        .withVariableCode(SALINITY)
                        .withValue(FLAG_PSAL[i])
                        .build()
                ))
                .build()
        ).collect(Collectors.toList()))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(123)
        .withPassed(false)
        .withFailedDepths(FLAG_RESULT)
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }
}
