package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;


import static edu.colorado.cires.wod.iquodqc.common.CastConstants.PROBE_TYPE;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EnBkgBuddyCheckTest {

  private static class EnBkgBuddyCheckTestWrapper extends EnBkgBuddyCheck {
    private final EnBkgBuddyCheck check;

    private EnBkgBuddyCheckTestWrapper(EnBkgBuddyCheck check) {
      this.check = check;
    }

    @Override
    protected Dataset<Row> createQuery(CastCheckContext context) {
      return check.createQuery(context);
    }

  }

  private static final Path TEMP_DIR = Paths.get("target/testspace").toAbsolutePath().normalize();
  private static final String TEST_PARQUET = TEMP_DIR.resolve("test.parquet").toString();

  private static final EnBkgBuddyCheck check = (EnBkgBuddyCheck) ServiceLoader.load(CastCheck.class).iterator().next();

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
  public void testQuery() throws Exception {
    List<Cast> casts = Arrays.asList(
        Cast.builder()
            .withLatitude(55.6)
            .withLongitude(12.9)
            .withGeohash("u3c")
            .withYear((short) 1900)
            .withMonth((short) 1)
            .withDay((short) 15)
            .withTime(0D)
            .withCastNumber(1)
            .withCruiseNumber(100)
            .withDepths(Arrays.asList(
                Depth.builder().withDepth(0.0)
                    .withData(Collections.singletonList(ProfileData.builder()
                        .withOriginatorsFlag(0).withQcFlag(0)
                        .withVariable(TEMPERATURE).withValue(1.8)
                        .build()))
                    .build()
            ))
            .build(),
        Cast.builder()
            .withLatitude(55.6)
            .withLongitude(15)
            .withGeohash("u3f")
            .withYear((short) 1900)
            .withMonth((short) 1)
            .withDay((short) 15)
            .withTime(0D)
            .withCastNumber(2)
            .withCruiseNumber(100)
            .withDepths(Arrays.asList(
                Depth.builder().withDepth(0.0)
                    .withData(Collections.singletonList(ProfileData.builder()
                        .withOriginatorsFlag(0).withQcFlag(0)
                        .withVariable(TEMPERATURE).withValue(1.8)
                        .build()))
                    .build()
            ))
            .build(),
        Cast.builder()
            .withLatitude(55.6)
            .withLongitude(15.3)
            .withGeohash("u3f")
            .withYear((short) 1900)
            .withMonth((short) 1)
            .withDay((short) 15)
            .withTime(0D)
            .withCastNumber(3)
            .withCruiseNumber(200)
            .withDepths(Arrays.asList(
                Depth.builder().withDepth(0.0)
                    .withData(Collections.singletonList(ProfileData.builder()
                        .withOriginatorsFlag(0).withQcFlag(0)
                        .withVariable(TEMPERATURE).withValue(1.8)
                        .build()))
                    .build()
            ))
            .build(),
        Cast.builder()
            .withLatitude(55.6)
            .withLongitude(15.1)
            .withGeohash("u3f")
            .withYear((short) 1900)
            .withMonth((short) 2)
            .withDay((short) 15)
            .withTime(0D)
            .withCastNumber(4)
            .withCruiseNumber(200)
            .withDepths(Arrays.asList(
                Depth.builder().withDepth(0.0)
                    .withData(Collections.singletonList(ProfileData.builder()
                        .withOriginatorsFlag(0).withQcFlag(0)
                        .withVariable(TEMPERATURE).withValue(1.8)
                        .build()))
                    .build()
            ))
            .build(),
        Cast.builder()
            .withLatitude(56.3)
            .withLongitude(15.1)
            .withGeohash("u64")
            .withYear((short) 1900)
            .withMonth((short) 2)
            .withDay((short) 15)
            .withTime(0D)
            .withCastNumber(5)
            .withCruiseNumber(200)
            .withDepths(Arrays.asList(
                Depth.builder().withDepth(0.0)
                    .withData(Collections.singletonList(ProfileData.builder()
                        .withOriginatorsFlag(0).withQcFlag(0)
                        .withVariable(TEMPERATURE).withValue(1.8)
                        .build()))
                    .build()
            ))
            .build()
        );

    Dataset<Cast> dataset = spark.createDataset(casts, Encoders.bean(Cast.class));
    dataset.write().partitionBy("geohash", "year").parquet(TEST_PARQUET);

    for (String other : Arrays.asList(CheckNames.EN_BACKGROUND_CHECK.getName(),
        CheckNames.EN_CONSTANT_VALUE_CHECK.getName(),
        CheckNames.EN_INCREASING_DEPTH_CHECK.getName(),
        CheckNames.EN_RANGE_CHECK.getName(),
        CheckNames.EN_SPIKE_AND_STEP_CHECK.getName(),
        CheckNames.EN_STABILITY_CHECK.getName())) {
      Dataset<CastCheckResult> otherResult = spark.createDataset(
          Arrays.asList(
              CastCheckResult.builder().withCastNumber(1).withPassed(true).build(),
              CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
              CastCheckResult.builder().withCastNumber(3).withPassed(true).build(),
              CastCheckResult.builder().withCastNumber(4).withPassed(true).build(),
              CastCheckResult.builder().withCastNumber(5).withPassed(true).build()
          ),
          Encoders.bean(CastCheckResult.class));
      otherResult.write().parquet(TEMP_DIR.resolve(other + ".parquet").toString());
    }

//    new EnBkgBuddyCheckTestWrapper(check).createQuery(context).explain();
//    new EnBkgBuddyCheckTestWrapper(check).createQuery(context).show();

    List<Row> rows = new EnBkgBuddyCheckTestWrapper(check).createQuery(context).collectAsList();
    rows.sort(Comparator.comparingInt(o -> {
      Row cast = o.getStruct(o.fieldIndex("cast"));
      return cast.getInt(cast.fieldIndex("castNumber"));
    }));
    Cast c1 = Cast.builder(rows.get(0).getStruct(rows.get(0).fieldIndex("cast"))).build();
    Cast c2 = Cast.builder(rows.get(1).getStruct(rows.get(1).fieldIndex("cast"))).build();
    Cast c3 = Cast.builder(rows.get(2).getStruct(rows.get(2).fieldIndex("cast"))).build();
    Cast c4 = Cast.builder(rows.get(3).getStruct(rows.get(3).fieldIndex("cast"))).build();
    Cast c5 = Cast.builder(rows.get(4).getStruct(rows.get(4).fieldIndex("cast"))).build();

    assertEquals(1, c1.getCastNumber());
    assertEquals(2, c2.getCastNumber());
    assertEquals(3, c3.getCastNumber());
    assertEquals(4, c4.getCastNumber());
    assertEquals(5, c5.getCastNumber());


    CastCheckResult c1Bg = CastCheckResult.builder(rows.get(0).getStruct(rows.get(0).fieldIndex(CheckNames.EN_BACKGROUND_CHECK.getName()))).build();
    CastCheckResult c1Cv = CastCheckResult.builder(rows.get(0).getStruct(rows.get(0).fieldIndex(CheckNames.EN_CONSTANT_VALUE_CHECK.getName()))).build();
    CastCheckResult c1Id = CastCheckResult.builder(rows.get(0).getStruct(rows.get(0).fieldIndex(CheckNames.EN_INCREASING_DEPTH_CHECK.getName()))).build();
    CastCheckResult c1Rc = CastCheckResult.builder(rows.get(0).getStruct(rows.get(0).fieldIndex(CheckNames.EN_RANGE_CHECK.getName()))).build();
    CastCheckResult c1Ssc = CastCheckResult.builder(rows.get(0).getStruct(rows.get(0).fieldIndex(CheckNames.EN_SPIKE_AND_STEP_CHECK.getName()))).build();
    CastCheckResult c1Sc = CastCheckResult.builder(rows.get(0).getStruct(rows.get(0).fieldIndex(CheckNames.EN_STABILITY_CHECK.getName()))).build();

    assertEquals(1, c1Bg.getCastNumber());
    assertEquals(1, c1Cv.getCastNumber());
    assertEquals(1, c1Id.getCastNumber());
    assertEquals(1, c1Rc.getCastNumber());
    assertEquals(1, c1Ssc.getCastNumber());
    assertEquals(1, c1Sc.getCastNumber());

    Row c1Buddy = rows.get(0).getStruct(rows.get(0).fieldIndex("buddy"));
    Row c1Result = c1Buddy.getStruct(c1Buddy.fieldIndex("result"));
    double c1Distance = c1Buddy.getDouble(c1Buddy.fieldIndex("distance"));
    Cast c1b = Cast.builder(c1Result.getStruct(c1Result.fieldIndex("cast"))).build();

    Row c2Buddy = rows.get(1).getStruct(rows.get(1).fieldIndex("buddy"));
    Row c2Result = c2Buddy.getStruct(c2Buddy.fieldIndex("result"));
    double c2Distance = c2Buddy.getDouble(c2Buddy.fieldIndex("distance"));
    Cast c2b = Cast.builder(c2Result.getStruct(c2Result.fieldIndex("cast"))).build();

    Row c3Buddy = rows.get(2).getStruct(rows.get(2).fieldIndex("buddy"));
    Row c3Result = c3Buddy.getStruct(c3Buddy.fieldIndex("result"));
    double c3Distance = c3Buddy.getDouble(c3Buddy.fieldIndex("distance"));
    Cast c3b = Cast.builder(c3Result.getStruct(c3Result.fieldIndex("cast"))).build();

    Row c4Buddy= rows.get(3).getStruct(rows.get(3).fieldIndex("buddy"));
    Row c5Buddy = rows.get(4).getStruct(rows.get(4).fieldIndex("buddy"));

    assertEquals(3, c1b.getCastNumber());
    assertEquals(3, c2b.getCastNumber());
    assertEquals(2, c3b.getCastNumber());
    assertNull(c4Buddy);
    assertNull(c5Buddy);

    assertTrue(c1Distance >= 0D);
    assertTrue(c2Distance >= 0D);
    assertTrue(c3Distance >= 0D);

    CastCheckResult c3Bg = CastCheckResult.builder(c2Result.getStruct(c2Result.fieldIndex(CheckNames.EN_BACKGROUND_CHECK.getName()))).build();
    CastCheckResult c3Cv = CastCheckResult.builder(c2Result.getStruct(c2Result.fieldIndex(CheckNames.EN_CONSTANT_VALUE_CHECK.getName()))).build();
    CastCheckResult c3Id = CastCheckResult.builder(c2Result.getStruct(c2Result.fieldIndex(CheckNames.EN_INCREASING_DEPTH_CHECK.getName()))).build();
    CastCheckResult c3Rc = CastCheckResult.builder(c2Result.getStruct(c2Result.fieldIndex(CheckNames.EN_RANGE_CHECK.getName()))).build();
    CastCheckResult c3Ssc = CastCheckResult.builder(c2Result.getStruct(c2Result.fieldIndex(CheckNames.EN_SPIKE_AND_STEP_CHECK.getName()))).build();
    CastCheckResult c3Sc = CastCheckResult.builder(c2Result.getStruct(c2Result.fieldIndex(CheckNames.EN_STABILITY_CHECK.getName()))).build();

    assertEquals(3, c3Bg.getCastNumber());
    assertEquals(3, c3Cv.getCastNumber());
    assertEquals(3, c3Id.getCastNumber());
    assertEquals(3, c3Rc.getCastNumber());
    assertEquals(3, c3Ssc.getCastNumber());
    assertEquals(3, c3Sc.getCastNumber());

  }

  @Test
  public void testEnStdLevelBkgAndBuddyCheckTemperature() throws Exception {
    Cast cast = Cast.builder()
        .withLatitude(55.6)
        .withLongitude(12.9)
        .withGeohash("u3c")
        .withYear((short) 1900)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(0D)
        .withCruiseNumber(1234)
        .withCastNumber(8888)
        .withAttributes(Collections.singletonList(Attribute.builder().withCode(PROBE_TYPE).withValue(7D).build()))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0.0)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(1.8)
                    .build()))
                .build(),
            Depth.builder().withDepth(2.5)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(1.8).build()))
                .build(),
            Depth.builder().withDepth(5.0)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(1.8).build()))
                .build(),
            Depth.builder().withDepth(7.5)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(7.1).build()))
                .build()
        ))
        .build();

    Cast buddy = Cast.builder()
        .withLatitude(0D)
        .withLongitude(0D)
        .withGeohash("s00")
        .withYear((short) 1999)
        .withMonth((short) 12)
        .withDay((short) 31)
        .withTime(23.99)
        .withCruiseNumber(1234)
        .withCastNumber(1)
        .withAttributes(Collections.singletonList(Attribute.builder().withCode(PROBE_TYPE).withValue(7D).build()))
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(0.0)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0.0)
                    .build()))
                .build(),
            Depth.builder().withDepth(0.0)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0.0).build()))
                .build(),
            Depth.builder().withDepth(0.0)
                .withData(Collections.singletonList(ProfileData.builder()
                    .withVariable(TEMPERATURE).withValue(0.0).build()))
                .build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Arrays.asList(cast, buddy), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(8888).withPassed(false).withFailedDepths(Collections.singletonList(3)).build(),
        CastCheckResult.builder().withCastNumber(1).withPassed(false).withFailedDepths(Collections.singletonList(2)).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_background_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(8888).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
        ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_constant_value_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(8888).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
        ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_increasing_depth_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(8888).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
        ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_range_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(8888).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
        ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(8888).withPassed(false).withFailedDepths(Collections.singletonList(3)).build(),
        CastCheckResult.builder().withCastNumber(1).withPassed(false).withFailedDepths(Collections.singletonList(2)).build()
        ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_suspect.parquet").toString());

   otherResult = spark.createDataset(Arrays.asList(
       CastCheckResult.builder().withCastNumber(8888).withPassed(true).build(),
       CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
       ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_stability_check.parquet").toString());


    CastCheckResult expected = CastCheckResult.builder()
        .withCastNumber(8888)
        .withPassed(true)
        .build();

    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    results.sort(Comparator.comparingInt(CastCheckResult::getCastNumber));
    assertEquals(2, results.size());
    assertEquals(CastCheckResult.builder().withCastNumber(1).withPassed(true).build(), results.get(0));
    assertEquals(CastCheckResult.builder().withCastNumber(8888).withPassed(true).build(), results.get(1));
  }

  @Test
  public void testEnStdLevelBkgAndBuddyRealProfiles1() throws Exception {
    Cast realProfile1 = Cast.builder()
        .withCastNumber(1)
        .withLatitude(-39.889)
        .withLongitude(17.650000)
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 15)
        .withTime(12D)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(9).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(15).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(21).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(27).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(33).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(39).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(45).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(51).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6900).build())).build(),
            Depth.builder().withDepth(57).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6600).build())).build(),
            Depth.builder().withDepth(63).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.4300).build())).build(),
            Depth.builder().withDepth(68).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.9100).build())).build(),
            Depth.builder().withDepth(74).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.6600).build())).build(),
            Depth.builder().withDepth(80).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.5300).build())).build(),
            Depth.builder().withDepth(86).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.3000).build())).build(),
            Depth.builder().withDepth(92).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.2200).build())).build(),
            Depth.builder().withDepth(98).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.1300).build())).build(),
            Depth.builder().withDepth(104).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.0400).build())).build(),
            Depth.builder().withDepth(110).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.9600).build())).build(),
            Depth.builder().withDepth(116).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.8200).build())).build(),
            Depth.builder().withDepth(122).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.7400).build())).build(),
            Depth.builder().withDepth(140).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.4300).build())).build(),
            Depth.builder().withDepth(170).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.0900).build())).build(),
            Depth.builder().withDepth(199).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.6900).build())).build(),
            Depth.builder().withDepth(229).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.2300).build())).build(),
            Depth.builder().withDepth(259).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.8300).build())).build(),
            Depth.builder().withDepth(289).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.4200).build())).build(),
            Depth.builder().withDepth(318).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.9900).build())).build(),
            Depth.builder().withDepth(348).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.4600).build())).build(),
            Depth.builder().withDepth(378).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.9400).build())).build(),
            Depth.builder().withDepth(407).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.6400).build())).build(),
            Depth.builder().withDepth(437).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.1800).build())).build(),
            Depth.builder().withDepth(467).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.7500).build())).build(),
            Depth.builder().withDepth(511).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.2200).build())).build(),
            Depth.builder().withDepth(571).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(12.7000).build())).build(),
            Depth.builder().withDepth(630).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(12.0100).build())).build(),
            Depth.builder().withDepth(690).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(11.3000).build())).build(),
            Depth.builder().withDepth(749).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.6400).build())).build(),
            Depth.builder().withDepth(808).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.0000).build())).build(),
            Depth.builder().withDepth(867).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(9.36000).build())).build(),
            Depth.builder().withDepth(927).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(8.66000).build())).build(),
            Depth.builder().withDepth(986).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(8.37000).build())).build(),
            Depth.builder().withDepth(1045).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(7.58000).build())).build(),
            Depth.builder().withDepth(1104).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(6.86000).build())).build(),
            Depth.builder().withDepth(1164).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.46000).build())).build(),
            Depth.builder().withDepth(1223).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.03000).build())).build(),
            Depth.builder().withDepth(1282).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.79000).build())).build(),
            Depth.builder().withDepth(1341).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.42000).build())).build(),
            Depth.builder().withDepth(1400).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.10000).build())).build(),
            Depth.builder().withDepth(1460).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.66000).build())).build(),
            Depth.builder().withDepth(1519).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.53000).build())).build(),
            Depth.builder().withDepth(1578).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.42000).build())).build(),
            Depth.builder().withDepth(1637).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.17000).build())).build(),
            Depth.builder().withDepth(1696).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.05000).build())).build(),
            Depth.builder().withDepth(1755).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.02000).build())).build(),
            Depth.builder().withDepth(1814).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(2.93000).build())).build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Arrays.asList(realProfile1), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_background_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_constant_value_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_increasing_depth_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_range_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_suspect.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(1).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_stability_check.parquet").toString());


    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    results.sort(Comparator.comparingInt(CastCheckResult::getCastNumber));
    assertEquals(1, results.size());
    assertEquals(CastCheckResult.builder().withCastNumber(1).withPassed(false).withFailedDepths(Arrays.asList(
        37, 38, 39, 40, 41, 42, 43, 44, 45
    )).build(), results.get(0));

  }


}