package edu.colorado.cires.wod.iquodqc.check.en.stability;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PRESSURE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.SALINITY;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.Metadata;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import edu.colorado.cires.wod.parquet.model.Variable;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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

class EnStabilityCheckTest {

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private final EnStabilityCheck check = (EnStabilityCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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
  public void testEnStabilityCheckPadded() {
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
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(40D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(8000D).build()
                    ))
                .build(),
            Depth.builder().withDepth(10D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(35D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(2000).build()
                    ))
                .build(),
            Depth.builder().withDepth(20D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.4).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(20D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(1000D).build()
                    ))
                .build(),
            Depth.builder().withDepth(30D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(40D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(8000D).build()
                    ))
                .build(),
            Depth.builder().withDepth(40D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(40D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(8000D).build()
                    ))
                .build(),
            Depth.builder().withDepth(50D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(40D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(8000D).build()
                    ))
                .build(),
            Depth.builder().withDepth(60D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(40D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(8000D).build()
                    ))
                .build(),
            Depth.builder().withDepth(70D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(40D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(8000D).build()
                    ))
                .build(),
            Depth.builder().withDepth(80D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(40D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(8000D).build()
                    ))
                .build()
        )).build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(1, 2))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testEnStabilityCheckUnpadded() {
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
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(40D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(8000D).build()
                ))
                .build(),
            Depth.builder().withDepth(10D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(25.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(35D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(2000D).build()
                ))
                .build(),
            Depth.builder().withDepth(20D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.4).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(20D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(1000D).build()
                ))
                .build(),
            Depth.builder().withDepth(30D)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.5).build(),
                    ProfileData.builder().withVariableCode(SALINITY).withValue(40D).build(),
                    ProfileData.builder().withVariableCode(PRESSURE).withValue(8000D).build()
                ))
                .build()
        )).build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(false)
        .withFailedDepths(Arrays.asList(0, 1, 2, 3))
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }


  @Test
  public void testForBuddy() throws Exception {
    Cast cast = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withCastNumber(1)
        .withLatitude(-39.889)
        .withLongitude(17.650000)
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(12D)
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(9).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(15).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(21).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(27).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(33).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(39).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(45).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(51).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(57).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6600).build())).build(),
            Depth.builder().withDepth(63).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.4300).build())).build(),
            Depth.builder().withDepth(68).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.9100).build())).build(),
            Depth.builder().withDepth(74).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.6600).build())).build(),
            Depth.builder().withDepth(80).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.5300).build())).build(),
            Depth.builder().withDepth(86).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.3000).build())).build(),
            Depth.builder().withDepth(92).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.2200).build())).build(),
            Depth.builder().withDepth(98).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.1300).build())).build(),
            Depth.builder().withDepth(104).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.0400).build())).build(),
            Depth.builder().withDepth(110).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.9600).build())).build(),
            Depth.builder().withDepth(116).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.8200).build())).build(),
            Depth.builder().withDepth(122).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.7400).build())).build(),
            Depth.builder().withDepth(140).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.4300).build())).build(),
            Depth.builder().withDepth(170).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.0900).build())).build(),
            Depth.builder().withDepth(199).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(17.6900).build())).build(),
            Depth.builder().withDepth(229).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(17.2300).build())).build(),
            Depth.builder().withDepth(259).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.8300).build())).build(),
            Depth.builder().withDepth(289).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.4200).build())).build(),
            Depth.builder().withDepth(318).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.9900).build())).build(),
            Depth.builder().withDepth(348).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.4600).build())).build(),
            Depth.builder().withDepth(378).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(14.9400).build())).build(),
            Depth.builder().withDepth(407).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(14.6400).build())).build(),
            Depth.builder().withDepth(437).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(14.1800).build())).build(),
            Depth.builder().withDepth(467).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.7500).build())).build(),
            Depth.builder().withDepth(511).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.2200).build())).build(),
            Depth.builder().withDepth(571).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(12.7000).build())).build(),
            Depth.builder().withDepth(630).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(12.0100).build())).build(),
            Depth.builder().withDepth(690).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(11.3000).build())).build(),
            Depth.builder().withDepth(749).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(10.6400).build())).build(),
            Depth.builder().withDepth(808).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(10.0000).build())).build(),
            Depth.builder().withDepth(867).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(9.36000).build())).build(),
            Depth.builder().withDepth(927).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(8.66000).build())).build(),
            Depth.builder().withDepth(986).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(8.37000).build())).build(),
            Depth.builder().withDepth(1045).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(7.58000).build())).build(),
            Depth.builder().withDepth(1104).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(6.86000).build())).build(),
            Depth.builder().withDepth(1164).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.46000).build())).build(),
            Depth.builder().withDepth(1223).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.03000).build())).build(),
            Depth.builder().withDepth(1282).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.79000).build())).build(),
            Depth.builder().withDepth(1341).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.42000).build())).build(),
            Depth.builder().withDepth(1400).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.10000).build())).build(),
            Depth.builder().withDepth(1460).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.66000).build())).build(),
            Depth.builder().withDepth(1519).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.53000).build())).build(),
            Depth.builder().withDepth(1578).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.42000).build())).build(),
            Depth.builder().withDepth(1637).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.17000).build())).build(),
            Depth.builder().withDepth(1696).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.05000).build())).build(),
            Depth.builder().withDepth(1755).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.02000).build())).build(),
            Depth.builder().withDepth(1814).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(2.93000).build())).build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(1)
        .withPassed(true)
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    CastCheckResult result = results.get(0);
    assertEquals(expected, result);
  }

  @Test
  public void testForBuddy2() throws Exception {
    Cast realProfile2 = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withCastNumber(2)
        .withLatitude(-30.229)
        .withLongitude(2.658)
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 10)
        .withTime(0D)
        .withCruiseNumber(2)
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.4200).build())).build(),
            Depth.builder().withDepth(10).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.1300).build())).build(),
            Depth.builder().withDepth(15).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.4800).build())).build(),
            Depth.builder().withDepth(20).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.8400).build())).build(),
            Depth.builder().withDepth(25).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.2000).build())).build(),
            Depth.builder().withDepth(30).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.9400).build())).build(),
            Depth.builder().withDepth(35).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.7600).build())).build(),
            Depth.builder().withDepth(40).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.5000).build())).build(),
            Depth.builder().withDepth(45).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.0700).build())).build(),
            Depth.builder().withDepth(50).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(17.5900).build())).build(),
            Depth.builder().withDepth(55).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(17.3400).build())).build(),
            Depth.builder().withDepth(60).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(17.0000).build())).build(),
            Depth.builder().withDepth(65).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.8000).build())).build(),
            Depth.builder().withDepth(70).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.6200).build())).build(),
            Depth.builder().withDepth(74).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.5700).build())).build(),
            Depth.builder().withDepth(79).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.4900).build())).build(),
            Depth.builder().withDepth(84).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.4500).build())).build(),
            Depth.builder().withDepth(89).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.4100).build())).build(),
            Depth.builder().withDepth(94).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.3900).build())).build(),
            Depth.builder().withDepth(99).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.3500).build())).build(),
            Depth.builder().withDepth(104).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.3300).build())).build(),
            Depth.builder().withDepth(109).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.3300).build())).build(),
            Depth.builder().withDepth(114).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.3300).build())).build(),
            Depth.builder().withDepth(119).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.3200).build())).build(),
            Depth.builder().withDepth(124).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.3000).build())).build(),
            Depth.builder().withDepth(129).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.2800).build())).build(),
            Depth.builder().withDepth(134).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.2700).build())).build(),
            Depth.builder().withDepth(139).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.2400).build())).build(),
            Depth.builder().withDepth(144).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.2300).build())).build(),
            Depth.builder().withDepth(149).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.2100).build())).build(),
            Depth.builder().withDepth(154).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.2000).build())).build(),
            Depth.builder().withDepth(159).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.1700).build())).build(),
            Depth.builder().withDepth(164).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.1400).build())).build(),
            Depth.builder().withDepth(169).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.1100).build())).build(),
            Depth.builder().withDepth(174).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.0800).build())).build(),
            Depth.builder().withDepth(179).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.0500).build())).build(),
            Depth.builder().withDepth(184).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.0200).build())).build(),
            Depth.builder().withDepth(189).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.9900).build())).build(),
            Depth.builder().withDepth(194).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.9700).build())).build(),
            Depth.builder().withDepth(199).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.9400).build())).build(),
            Depth.builder().withDepth(218).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.7500).build())).build(),
            Depth.builder().withDepth(238).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.6000).build())).build(),
            Depth.builder().withDepth(258).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.3700).build())).build(),
            Depth.builder().withDepth(278).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(14.9300).build())).build(),
            Depth.builder().withDepth(298).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(14.7200).build())).build(),
            Depth.builder().withDepth(318).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(14.4800).build())).build(),
            Depth.builder().withDepth(337).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(14.1600).build())).build(),
            Depth.builder().withDepth(357).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.8000).build())).build(),
            Depth.builder().withDepth(377).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.6600).build())).build(),
            Depth.builder().withDepth(397).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.3100).build())).build(),
            Depth.builder().withDepth(446).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(12.4700).build())).build(),
            Depth.builder().withDepth(496).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(11.7400).build())).build(),
            Depth.builder().withDepth(546).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(10.9700).build())).build(),
            Depth.builder().withDepth(595).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(10.4300).build())).build(),
            Depth.builder().withDepth(645).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(9.69000).build())).build(),
            Depth.builder().withDepth(694).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(8.42000).build())).build(),
            Depth.builder().withDepth(744).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(7.20000).build())).build(),
            Depth.builder().withDepth(793).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(6.22000).build())).build(),
            Depth.builder().withDepth(842).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.48000).build())).build(),
            Depth.builder().withDepth(892).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.02000).build())).build(),
            Depth.builder().withDepth(941).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.59000).build())).build(),
            Depth.builder().withDepth(991).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.18000).build())).build(),
            Depth.builder().withDepth(1040).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.05000).build())).build(),
            Depth.builder().withDepth(1068).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.01000).build())).build()
        ))
        .build();


    Cast realProfile3 = Cast.builder()
        .withDataset("TEST")
        .withGeohash("TEST")
        .withCastNumber(3)
        .withLatitude(-28.36)
        .withLongitude(-0.752)
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 10)
        .withTime(0.1895833)
        .withCruiseNumber(3)
        .withAttributes(Arrays.asList(
            Attribute.builder()
                .withCode(ORIGINATORS_FLAGS)
                .withValue(1)
                .build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(22.9400).build())).build(),
            Depth.builder().withDepth(9).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.8800).build())).build(),
            Depth.builder().withDepth(15).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(21.1200).build())).build(),
            Depth.builder().withDepth(21).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.6100).build())).build(),
            Depth.builder().withDepth(27).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(20.3600).build())).build(),
            Depth.builder().withDepth(33).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.5500).build())).build(),
            Depth.builder().withDepth(39).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(19.1200).build())).build(),
            Depth.builder().withDepth(45).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(18.5600).build())).build(),
            Depth.builder().withDepth(51).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(17.9400).build())).build(),
            Depth.builder().withDepth(57).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(17.6900).build())).build(),
            Depth.builder().withDepth(63).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(17.4800).build())).build(),
            Depth.builder().withDepth(69).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(17.2600).build())).build(),
            Depth.builder().withDepth(74).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(17.1000).build())).build(),
            Depth.builder().withDepth(80).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.9000).build())).build(),
            Depth.builder().withDepth(86).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.7400).build())).build(),
            Depth.builder().withDepth(92).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.5800).build())).build(),
            Depth.builder().withDepth(98).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.4200).build())).build(),
            Depth.builder().withDepth(104).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.2900).build())).build(),
            Depth.builder().withDepth(110).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.1900).build())).build(),
            Depth.builder().withDepth(116).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(16.0700).build())).build(),
            Depth.builder().withDepth(122).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.9200).build())).build(),
            Depth.builder().withDepth(140).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(15.3700).build())).build(),
            Depth.builder().withDepth(170).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(14.5000).build())).build(),
            Depth.builder().withDepth(200).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.8400).build())).build(),
            Depth.builder().withDepth(229).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(13.2600).build())).build(),
            Depth.builder().withDepth(259).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(12.8200).build())).build(),
            Depth.builder().withDepth(289).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(12.3100).build())).build(),
            Depth.builder().withDepth(319).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(11.9200).build())).build(),
            Depth.builder().withDepth(348).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(11.4300).build())).build(),
            Depth.builder().withDepth(378).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(10.9800).build())).build(),
            Depth.builder().withDepth(408).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(10.4100).build())).build(),
            Depth.builder().withDepth(438).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(9.77000).build())).build(),
            Depth.builder().withDepth(482).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(8.75000).build())).build(),
            Depth.builder().withDepth(542).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(7.66000).build())).build(),
            Depth.builder().withDepth(601).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(6.77000).build())).build(),
            Depth.builder().withDepth(660).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.91000).build())).build(),
            Depth.builder().withDepth(720).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(5.14000).build())).build(),
            Depth.builder().withDepth(779).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.63000).build())).build(),
            Depth.builder().withDepth(839).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.25000).build())).build(),
            Depth.builder().withDepth(898).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(4.00000).build())).build(),
            Depth.builder().withDepth(957).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.75000).build())).build(),
            Depth.builder().withDepth(1017).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.61000).build())).build(),
            Depth.builder().withDepth(1076).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.46000).build())).build(),
            Depth.builder().withDepth(1135).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.34000).build())).build(),
            Depth.builder().withDepth(1194).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.24000).build())).build(),
            Depth.builder().withDepth(1254).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.21000).build())).build(),
            Depth.builder().withDepth(1313).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.21000).build())).build(),
            Depth.builder().withDepth(1372).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.21000).build())).build(),
            Depth.builder().withDepth(1431).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.18000).build())).build(),
            Depth.builder().withDepth(1491).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.12000).build())).build(),
            Depth.builder().withDepth(1550).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.08000).build())).build(),
            Depth.builder().withDepth(1609).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.06000).build())).build(),
            Depth.builder().withDepth(1668).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.04000).build())).build(),
            Depth.builder().withDepth(1727).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(3.02000).build())).build(),
            Depth.builder().withDepth(1786).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(2.97000).build())).build(),
            Depth.builder().withDepth(1845).withData(Collections.singletonList(ProfileData.builder().withVariableCode(TEMPERATURE).withValue(2.88000).build())).build()
        ))
        .build();
    Dataset<Cast> dataset = spark.createDataset(Arrays.asList(realProfile2, realProfile3), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);



    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    results.sort(Comparator.comparingInt(CastCheckResult::getCastNumber));
    assertEquals(2, results.size());
    assertEquals(CastCheckResult.builder().withCastNumber(2).withPassed(true).build(), results.get(0));
    assertEquals(CastCheckResult.builder().withCastNumber(3).withPassed(true).build(), results.get(1));

  }


  @Test
  public void realTest() throws Exception {
    Cast cast = Cast.builder()
        .withDataset("CTD")
        .withCastNumber(7662759)
        .withCruiseNumber(1315)
        .withTimestamp(57055500000L)
        .withYear(1971)
        .withMonth(10)
        .withDay(23)
        .withTime(8.75)
        .withLongitude(9.6083)
        .withLatitude(43.253)
        .withProfileType(0)
        .withGeohash("spw")
        .withVariables(Arrays.asList(
            Variable.builder().withCode(1).withMetadata(Arrays.asList(Metadata.builder().withCode(1).withValue(4D).build())).build(),
            Variable.builder().withCode(2).withMetadata(Arrays.asList(Metadata.builder().withCode(5).withValue(4D).build())).build(),
            Variable.builder().withCode(25).withMetadata(Arrays.asList()).build()
        ))
        .withAttributes(Arrays.asList(
            Attribute.builder().withCode(1).withValue(9900172D).build(),
            Attribute.builder().withCode(4).withValue(599D).build(),
            Attribute.builder().withCode(7).withValue(42940D).build(),
            Attribute.builder().withCode(8).withValue(1D).build(),
            Attribute.builder().withCode(29).withValue(4D).build(),
            Attribute.builder().withCode(91).withValue(5D).build()
        ))
        .withDepths(Arrays.asList(
            Depth.builder()
                .withDepth(0.0)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(20.3).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(2).withValue(38.2).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(0.0).withQcFlag(0).withOriginatorsFlag(0).build()

                )).build(),
            Depth.builder()
                .withDepth(5.0)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(20.0).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(5.0).withQcFlag(0).withOriginatorsFlag(0).build()
                )).build(),
            Depth.builder()
                .withDepth(9.9)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(20.0).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(2).withValue(38.1).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(10.0).withQcFlag(0).withOriginatorsFlag(0).build()
                )).build(),
            Depth.builder()
                .withDepth(19.8)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(19.5).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(2).withValue(38.2).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(20.0).withQcFlag(0).withOriginatorsFlag(0).build()
                )).build(),
            Depth.builder()
                .withDepth(29.8)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(20.0).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(2).withValue(38.2).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(30.0).withQcFlag(0).withOriginatorsFlag(0).build()
                )).build(),
            Depth.builder()
                .withDepth(49.6)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(18.5).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(2).withValue(38.3).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(50.0).withQcFlag(0).withOriginatorsFlag(0).build()
                )).build(),
            Depth.builder()
                .withDepth(59.5)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(16.4).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(60.0).withQcFlag(0).withOriginatorsFlag(0).build()
                )).build(),
            Depth.builder()
                .withDepth(64.5)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(15.7).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(65.0).withQcFlag(0).withOriginatorsFlag(0).build()
                )).build(),
            Depth.builder()
                .withDepth(69.4)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(15.0).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(70.0).withQcFlag(0).withOriginatorsFlag(0).build()
                )).build(),
            Depth.builder()
                .withDepth(74.4)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(14.8).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(75.0).withQcFlag(0).withOriginatorsFlag(0).build()
                )).build(),
            Depth.builder()
                .withDepth(79.3)
                .withDepthErrorFlag(0)
                .withOriginatorsFlag(0)
                .withData(Arrays.asList(
                    ProfileData.builder().withVariableCode(1).withValue(14.5).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(2).withValue(38.0).withQcFlag(0).withOriginatorsFlag(0).build(),
                    ProfileData.builder().withVariableCode(25).withValue(80.0).withQcFlag(0).withOriginatorsFlag(0).build()
                )).build()
        ))
        .build();



    Dataset<Cast> dataset = spark.createDataset(Arrays.asList(cast), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);


    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    assertEquals(1, results.size());
    assertEquals(CastCheckResult.builder().withCastNumber(7662759).withPassed(false).withFailedDepths(Arrays.asList(3)).build(), results.get(0));

  }

}