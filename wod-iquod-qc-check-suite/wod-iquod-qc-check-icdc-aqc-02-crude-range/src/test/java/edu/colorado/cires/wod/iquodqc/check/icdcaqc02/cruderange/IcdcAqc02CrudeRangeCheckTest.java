package edu.colorado.cires.wod.iquodqc.check.icdcaqc02.cruderange;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.*;

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

class IcdcAqc02CrudeRangeCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final IcdcAqc02CrudeRangeCheck check = (IcdcAqc02CrudeRangeCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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

  @Test
  public void testIcdcAqc02CrudeRangeCheckExample1() {
    /*
  # Data provided by Viktor Gouretski, ICDC, University of Hamburg.
example1 = np.array([
[     .0,    28.600,  0],
[   10.0,    28.600,  0],
[   25.0,    28.520,  0],
[   50.0,    27.410,  0],
[   73.0,    25.120,  0],
[   98.0,    24.390,  0],
[  122.0,    23.320,  0],
[  147.0,    22.790,  0],
[  197.0,    22.040,  0],
[  247.0,    21.880,  0],
[  297.0,    21.860,  0],
[  396.0,    21.780,  0],
[  496.0,    21.760,  0],
[  595.0,    21.740,  0],
[  795.0,    21.810,  0],
[  994.0,    21.840,  0],
[ 1193.0,    21.860,  0],
[ 1492.0,    21.930,  0],
[ 1888.0,    21.960,  0],
[ 1937.0,    22.330,  0],
[ 1987.0,    24.560,  2],
[ 2008.0,    38.150,  1],
[ 2027.0,    48.800,  1]])
     */
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withCastNumber(1)
        .withMonth(1)
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(28.600).build())).build(),
            Depth.builder().withDepth(10D)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(28.600).build())).build(),
            Depth.builder().withDepth(25.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(28.520).build())).build(),
            Depth.builder().withDepth(50.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(27.410).build())).build(),
            Depth.builder().withDepth(73.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.120).build())).build(),
            Depth.builder().withDepth(98.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(24.390).build())).build(),
            Depth.builder().withDepth(122.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(23.320).build())).build(),
            Depth.builder().withDepth(147.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(22.790).build())).build(),
            Depth.builder().withDepth(197.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(22.040).build())).build(),
            Depth.builder().withDepth(247.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.880).build())).build(),
            Depth.builder().withDepth(297.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.860).build())).build(),
            Depth.builder().withDepth(396.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.780).build())).build(),
            Depth.builder().withDepth(496.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.760).build())).build(),
            Depth.builder().withDepth(595.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.740).build())).build(),
            Depth.builder().withDepth(795.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.810).build())).build(),
            Depth.builder().withDepth(994.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.840).build())).build(),
            Depth.builder().withDepth(1193.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.860).build())).build(),
            Depth.builder().withDepth(1492.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.930).build())).build(),
            Depth.builder().withDepth(1888.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.960).build())).build(),
            Depth.builder().withDepth(1937.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(22.330).build())).build(),
            Depth.builder().withDepth(1987.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(24.560).build())).build(),
            Depth.builder().withDepth(2008.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(38.150).build())).build(),
            Depth.builder().withDepth(2027.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(48.800).build())).build()
        )).build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(1)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(20, 21, 22))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testIcdcAqc02CrudeRangeCheckExample2() {
    /*
  # Data provided by Viktor Gouretski, ICDC, University of Hamburg.
example2 = np.array([
[     .0,    30.310,  0],
[    5.0,    30.310,  0],
[   18.0,    29.260,  0],
[   30.0,    29.170,  0],
[   34.0,    27.500,  0],
[   42.0,    26.300,  0],
[   43.0,    25.740,  0],
[   48.0,    24.810,  0],
[   55.0,    24.070,  0],
[   83.0,    22.590,  0],
[  108.0,    21.850,  0],
[  113.0,    21.300,  0],
[  163.0,    19.920,  0],
[  255.0,    18.830,  0],
[  327.0,    18.730,  0],
[  334.0,    23.330,  0],
[  341.0,    27.780,  2],
[  349.0,    32.050,  2],
[  356.0,    36.140,  1]])
     */
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withCastNumber(8888)
        .withMonth(1)
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0D)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(30.310).build())).build(),
            Depth.builder().withDepth(5.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(30.310).build())).build(),
            Depth.builder().withDepth(18.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(29.260).build())).build(),
            Depth.builder().withDepth(30.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(29.170).build())).build(),
            Depth.builder().withDepth(34.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(27.500).build())).build(),
            Depth.builder().withDepth(42.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.300).build())).build(),
            Depth.builder().withDepth(43.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.740).build())).build(),
            Depth.builder().withDepth(48.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(24.810).build())).build(),
            Depth.builder().withDepth(55.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(24.070).build())).build(),
            Depth.builder().withDepth(83.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(22.590).build())).build(),
            Depth.builder().withDepth(108.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.850).build())).build(),
            Depth.builder().withDepth(113.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.300).build())).build(),
            Depth.builder().withDepth(163.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.920).build())).build(),
            Depth.builder().withDepth(255.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.830).build())).build(),
            Depth.builder().withDepth(327.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.730).build())).build(),
            Depth.builder().withDepth(334.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(23.330).build())).build(),
            Depth.builder().withDepth(341.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(27.780).build())).build(),
            Depth.builder().withDepth(349.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(32.050).build())).build(),
            Depth.builder().withDepth(356.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(36.140).build())).build()
        )).build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(16, 17, 18))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testIcdcAqc02CrudeRangeCheckExample3() {
    /*
  # Data provided by Viktor Gouretski, ICDC, University of Hamburg.
example3 = np.array([
[    6.9,    15.700,  0],
[    8.9,    15.700,  0],
[   18.8,    15.700,  0],
[   28.8,    15.670,  0],
[   38.7,    15.440,  0],
[   48.6,    13.620,  0],
[   58.5,    11.920,  0],
[   68.4,    11.390,  0],
[   78.3,    11.270,  0],
[   98.1,    10.900,  0],
[  117.9,    10.730,  0],
[  137.8,    10.510,  0],
[  157.6,    10.340,  0],
[  177.4,     9.470,  0],
[  197.2,     9.140,  0],
[  222.0,     8.380,  0],
[  246.7,     8.090,  0],
[  271.5,     7.870,  0],
[  296.2,     7.290,  0],
[  321.0,     6.910,  0],
[  345.7,     6.830,  0],
[  370.5,     6.720,  0],
[  395.2,     6.210,  0],
[  419.9,     6.070,  0],
[  444.7,     5.690,  0],
[  469.4,     5.790,  0],
[  494.1,     5.570,  0],
[  518.8,     5.650,  0],
[  543.6,     5.680,  0],
[  568.3,     5.270,  0],
[  593.0,     5.170,  0],
[  617.7,     5.010,  0],
[  642.4,     5.030,  0],
[  667.1,     5.120,  0],
[  691.8,     5.180,  0],
[  741.2,     4.920,  0],
[  787.7,    36.560,  1],
[  790.6,     4.370,  0],
[  840.0,     4.200,  0],
[  888.4,    33.530,  1],
[  889.3,     4.110,  0],
[  938.7,     4.040,  0],
[  987.0,    32.370,  2],
[  988.0,     3.970,  0],
[ 1037.4,     3.910,  0],
[ 1084.7,    15.690,  2],
[ 1086.7,     3.840,  0],
[ 1136.0,     3.780,  0],
[ 1431.6,     3.420,  0],
[ 1480.8,     3.370,  0],
[ 1530.0,     3.340,  0],
[ 1579.2,     3.320,  0],
[ 1628.4,     3.300,  0],
[ 1676.6,     3.260,  0],
[ 1725.8,     3.240,  0],
[ 1775.9,     3.230,  0],
[ 1825.0,     3.230,  0],
[ 1874.2,     3.210,  0],
[ 1923.3,     3.170,  0],
[ 5021.8,     7.290,  2]])
     */
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withCastNumber(8888)
        .withMonth(1)
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(6.9)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.700).build())).build(),
            Depth.builder().withDepth(8.9)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.700).build())).build(),
            Depth.builder().withDepth(18.8)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.700).build())).build(),
            Depth.builder().withDepth(28.8)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.670).build())).build(),
            Depth.builder().withDepth(38.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.440).build())).build(),
            Depth.builder().withDepth(48.6)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.620).build())).build(),
            Depth.builder().withDepth(58.5)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(11.920).build())).build(),
            Depth.builder().withDepth(68.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(11.390).build())).build(),
            Depth.builder().withDepth(78.3)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(11.270).build())).build(),
            Depth.builder().withDepth(98.1)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(10.900).build())).build(),
            Depth.builder().withDepth(117.9)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(10.730).build())).build(),
            Depth.builder().withDepth(137.8)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(10.510).build())).build(),
            Depth.builder().withDepth(157.6)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(10.340).build())).build(),
            Depth.builder().withDepth(177.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(9.470).build())).build(),
            Depth.builder().withDepth(197.2)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(9.140).build())).build(),
            Depth.builder().withDepth(222.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(8.380).build())).build(),
            Depth.builder().withDepth(246.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(8.090).build())).build(),
            Depth.builder().withDepth(271.5)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(7.870).build())).build(),
            Depth.builder().withDepth(296.2)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(7.290).build())).build(),
            Depth.builder().withDepth(321.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(6.910).build())).build(),
            Depth.builder().withDepth(345.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(6.830).build())).build(),
            Depth.builder().withDepth(370.5)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(6.720).build())).build(),
            Depth.builder().withDepth(395.2)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(6.210).build())).build(),
            Depth.builder().withDepth(419.9)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(6.070).build())).build(),
            Depth.builder().withDepth(444.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.690).build())).build(),
            Depth.builder().withDepth(469.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.790).build())).build(),
            Depth.builder().withDepth(494.1)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.570).build())).build(),
            Depth.builder().withDepth(518.8)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.650).build())).build(),
            Depth.builder().withDepth(543.6)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.680).build())).build(),
            Depth.builder().withDepth(568.3)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.270).build())).build(),
            Depth.builder().withDepth(593.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.170).build())).build(),
            Depth.builder().withDepth(617.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.010).build())).build(),
            Depth.builder().withDepth(642.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.030).build())).build(),
            Depth.builder().withDepth(667.1)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.120).build())).build(),
            Depth.builder().withDepth(691.8)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.180).build())).build(),
            Depth.builder().withDepth(741.2)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.920).build())).build(),
            Depth.builder().withDepth(787.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(36.560).build())).build(),
            Depth.builder().withDepth(790.6)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.370).build())).build(),
            Depth.builder().withDepth(840.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.200).build())).build(),
            Depth.builder().withDepth(888.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(33.530).build())).build(),
            Depth.builder().withDepth(889.3)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.110).build())).build(),
            Depth.builder().withDepth(938.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.040).build())).build(),
            Depth.builder().withDepth(987.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(32.370).build())).build(),
            Depth.builder().withDepth(988.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.970).build())).build(),
            Depth.builder().withDepth(1037.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.910).build())).build(),
            Depth.builder().withDepth(1084.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.690).build())).build(),
            Depth.builder().withDepth(1086.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.840).build())).build(),
            Depth.builder().withDepth(1136.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.780).build())).build(),
            Depth.builder().withDepth(1431.6)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.420).build())).build(),
            Depth.builder().withDepth(1480.8)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.370).build())).build(),
            Depth.builder().withDepth(1530.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.340).build())).build(),
            Depth.builder().withDepth(1579.2)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.320).build())).build(),
            Depth.builder().withDepth(1628.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.300).build())).build(),
            Depth.builder().withDepth(1676.6)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.260).build())).build(),
            Depth.builder().withDepth(1725.8)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.240).build())).build(),
            Depth.builder().withDepth(1775.9)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.230).build())).build(),
            Depth.builder().withDepth(1825.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.230).build())).build(),
            Depth.builder().withDepth(1874.2)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.210).build())).build(),
            Depth.builder().withDepth(1923.3)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.170).build())).build(),
            Depth.builder().withDepth(5021.8)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(7.290).build())).build()
        )).build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(36, 39, 42, 45, 59))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testIcdcAqc02CrudeRangeCheckExample4() {
    /*
  # Data provided by Viktor Gouretski, ICDC, University of Hamburg.
example4 = np.array([
[    5.2,    32.370,  0],
[   31.9,    32.370,  0],
[   66.1,    32.480,  2],
[   73.8,    32.504,  2],
[   96.6,    33.846,  1],
[  123.4,    35.412,  1],
[  131.0,     1.393,  0],
[  161.4,     1.442,  0],
[  195.7,     1.497,  0],
[  260.4,     1.497,  0],
[  298.5,     1.497,  0],
[  344.1,     1.497,  0],
[  363.1,     1.497,  0],
[  370.7,     1.497,  0],
[  386.0,     1.497,  0],
[  412.6,     1.497,  0],
[  465.8,     1.497,  0],
[  492.4,     1.497,  0],
[  610.2,     1.393,  0]])
     */
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withCastNumber(8888)
        .withMonth(1)
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5.2)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(32.370).build())).build(),
            Depth.builder().withDepth(31.9)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(32.370).build())).build(),
            Depth.builder().withDepth(66.1)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(32.480).build())).build(),
            Depth.builder().withDepth(73.8)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(32.504).build())).build(),
            Depth.builder().withDepth(96.6)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(33.846).build())).build(),
            Depth.builder().withDepth(23.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(35.412).build())).build(),
            Depth.builder().withDepth(131.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.393).build())).build(),
            Depth.builder().withDepth(161.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.442).build())).build(),
            Depth.builder().withDepth(195.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.497).build())).build(),
            Depth.builder().withDepth(260.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.497).build())).build(),
            Depth.builder().withDepth(298.5)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.497).build())).build(),
            Depth.builder().withDepth(344.1)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.497).build())).build(),
            Depth.builder().withDepth(363.1)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.497).build())).build(),
            Depth.builder().withDepth(370.7)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.497).build())).build(),
            Depth.builder().withDepth(386.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.497).build())).build(),
            Depth.builder().withDepth(412.6)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.497).build())).build(),
            Depth.builder().withDepth(465.8)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.497).build())).build(),
            Depth.builder().withDepth(492.4)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.497).build())).build(),
            Depth.builder().withDepth(610.2)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(1.393).build())).build()
        )).build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(5, 2, 3, 4))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testIcdcAqc02CrudeRangeCheckExample5() {
    /*
  # Data provided by Viktor Gouretski, ICDC, University of Hamburg.
example5 = np.array([
[     .0,    26.000,  0],
[    5.0,    26.000,  0],
[   10.0,    26.000,  0],
[   15.0,    26.000,  0],
[   20.0,    26.000,  0],
[   25.0,    26.000,  0],
[   30.0,    26.000,  0],
[   35.0,    26.000,  0],
[   40.0,    26.000,  0],
[   45.0,    26.000,  0],
[   50.0,    26.000,  0],
[   55.0,    26.000,  0],
[   60.0,    26.000,  0],
[   65.0,    25.800,  0],
[   70.0,    25.400,  0],
[   75.0,    25.100,  0],
[   80.0,    25.000,  0],
[   85.0,    24.500,  0],
[   90.0,    25.100,  0],
[   95.0,    31.800,  0],
[  100.0,    20.600,  0],
[  105.0,     6.200,  0],
[  110.0,    36.100,  1],
[  115.0,    31.500,  2],
[  120.0,      .300,  0],
[  125.0,      .300,  0],
[  130.0,     6.800,  0],
[  135.0,    95.200,  1],
[  140.0,    14.700,  0]])
    */
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withCastNumber(8888)
        .withMonth(1)
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(5.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(10.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(15.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(20.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(25.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(30.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(35.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(40.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(45.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(50.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(55.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(60.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.000).build())).build(),
            Depth.builder().withDepth(65.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.800).build())).build(),
            Depth.builder().withDepth(70.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.400).build())).build(),
            Depth.builder().withDepth(75.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.100).build())).build(),
            Depth.builder().withDepth(80.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.000).build())).build(),
            Depth.builder().withDepth(85.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(24.500).build())).build(),
            Depth.builder().withDepth(90.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.100).build())).build(),
            Depth.builder().withDepth(95.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(31.800).build())).build(),
            Depth.builder().withDepth(100.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.600).build())).build(),
            Depth.builder().withDepth(105.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(6.200).build())).build(),
            Depth.builder().withDepth(110.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(36.100).build())).build(),
            Depth.builder().withDepth(115.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(31.500).build())).build(),
            Depth.builder().withDepth(120.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(.300).build())).build(),
            Depth.builder().withDepth(125.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(.300).build())).build(),
            Depth.builder().withDepth(130.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(6.800).build())).build(),
            Depth.builder().withDepth(135.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(95.200).build())).build(),
            Depth.builder().withDepth(140.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(14.700).build())).build()
        )).build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(22, 23, 27))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testIcdcAqc02CrudeRangeCheckExample6() {
    /*
  # Data provided by Viktor Gouretski, ICDC, University of Hamburg.
example6 = np.array([
[     .0,    24.410,  0],
[    1.0,    24.320,  0],
[   14.0,    25.560,  0],
[   35.0,    25.680,  0],
[   42.0,    26.020,  0],
[   49.0,    24.800,  0],
[   69.0,    24.640,  0],
[   81.0,    25.610,  0],
[   83.0,    26.530,  0],
[   91.0,    26.960,  0],
[   97.0,    27.310,  0],
[  121.0,    26.860,  0],
[  140.0,    28.360,  0],
[  142.0,    29.180,  0],
[  163.0,    29.850,  0],
[  168.0,    30.630,  0],
[  171.0,    32.220,  2],
[  177.0,    33.130,  1],
[  184.0,    30.750,  0],
[  195.0,    29.380,  0],
[  210.0,    28.960,  2],
[  215.0,    28.760,  2],
[  241.0,    28.210,  2],
[  326.0,    28.390,  2]])
    */
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withCastNumber(8888)
        .withMonth(1)
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Arrays.asList(

            Depth.builder().withDepth(.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(24.410).build())).build(),
            Depth.builder().withDepth(1.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(24.320).build())).build(),
            Depth.builder().withDepth(14.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.560).build())).build(),
            Depth.builder().withDepth(35.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.680).build())).build(),
            Depth.builder().withDepth(42.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.020).build())).build(),
            Depth.builder().withDepth(49.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(24.800).build())).build(),
            Depth.builder().withDepth(69.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(24.640).build())).build(),
            Depth.builder().withDepth(81.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.610).build())).build(),
            Depth.builder().withDepth(83.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.530).build())).build(),
            Depth.builder().withDepth(91.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.960).build())).build(),
            Depth.builder().withDepth(97.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(27.310).build())).build(),
            Depth.builder().withDepth(121.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(26.860).build())).build(),
            Depth.builder().withDepth(140.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(28.360).build())).build(),
            Depth.builder().withDepth(142.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(29.180).build())).build(),
            Depth.builder().withDepth(163.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(29.850).build())).build(),
            Depth.builder().withDepth(168.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(30.630).build())).build(),
            Depth.builder().withDepth(171.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(32.220).build())).build(),
            Depth.builder().withDepth(177.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(33.130).build())).build(),
            Depth.builder().withDepth(184.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(30.750).build())).build(),
            Depth.builder().withDepth(195.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(29.380).build())).build(),
            Depth.builder().withDepth(210.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(28.960).build())).build(),
            Depth.builder().withDepth(215.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(28.760).build())).build(),
            Depth.builder().withDepth(241.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(28.210).build())).build(),
            Depth.builder().withDepth(326.0)
                .withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(28.390).build())).build()
        )).build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(16, 17, 20, 21, 22, 23))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }
}