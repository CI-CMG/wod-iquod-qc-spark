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
import java.util.Iterator;
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

  private static EnBkgBuddyCheck check;
  private static EnBkgBuddyCheckTester checkCustom;

  static {
    Iterator<CastCheck> it = ServiceLoader.load(CastCheck.class).iterator();
    while (it.hasNext()) {
      CastCheck c = it.next();
      if (c instanceof EnBkgBuddyCheckTester) {
        checkCustom = (EnBkgBuddyCheckTester) c;
      } else {
        check = (EnBkgBuddyCheck) c;
      }
    }
  }


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

    CastCheckInitializationContext initializationContext = new CastCheckInitializationContext() {
      @Override
      public Properties getProperties() {
        return properties;
      }
    };

    check.initialize(initializationContext);
    checkCustom.initialize(initializationContext);
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
        CheckNames.EN_SPIKE_AND_STEP_SUSPECT.getName(),
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

  @Test
  public void testEnStdLevelBkgAndBuddyRealProfiles2() throws Exception {
    Cast realProfile2 = Cast.builder()
        .withCastNumber(2)
        .withLatitude(-30.229)
        .withLongitude(2.658)
        .withGeohash("k49")
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 10)
        .withTime(0D)
        .withCruiseNumber(2)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(21.4200).build())).build(),
            Depth.builder().withDepth(10).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(21.1300).build())).build(),
            Depth.builder().withDepth(15).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.4800).build())).build(),
            Depth.builder().withDepth(20).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.8400).build())).build(),
            Depth.builder().withDepth(25).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.2000).build())).build(),
            Depth.builder().withDepth(30).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.9400).build())).build(),
            Depth.builder().withDepth(35).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.7600).build())).build(),
            Depth.builder().withDepth(40).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.5000).build())).build(),
            Depth.builder().withDepth(45).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.0700).build())).build(),
            Depth.builder().withDepth(50).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.5900).build())).build(),
            Depth.builder().withDepth(55).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.3400).build())).build(),
            Depth.builder().withDepth(60).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.0000).build())).build(),
            Depth.builder().withDepth(65).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.8000).build())).build(),
            Depth.builder().withDepth(70).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.6200).build())).build(),
            Depth.builder().withDepth(74).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.5700).build())).build(),
            Depth.builder().withDepth(79).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.4900).build())).build(),
            Depth.builder().withDepth(84).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.4500).build())).build(),
            Depth.builder().withDepth(89).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.4100).build())).build(),
            Depth.builder().withDepth(94).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3900).build())).build(),
            Depth.builder().withDepth(99).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3500).build())).build(),
            Depth.builder().withDepth(104).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3300).build())).build(),
            Depth.builder().withDepth(109).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3300).build())).build(),
            Depth.builder().withDepth(114).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3300).build())).build(),
            Depth.builder().withDepth(119).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3200).build())).build(),
            Depth.builder().withDepth(124).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3000).build())).build(),
            Depth.builder().withDepth(129).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2800).build())).build(),
            Depth.builder().withDepth(134).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2700).build())).build(),
            Depth.builder().withDepth(139).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2400).build())).build(),
            Depth.builder().withDepth(144).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2300).build())).build(),
            Depth.builder().withDepth(149).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2100).build())).build(),
            Depth.builder().withDepth(154).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2000).build())).build(),
            Depth.builder().withDepth(159).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.1700).build())).build(),
            Depth.builder().withDepth(164).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.1400).build())).build(),
            Depth.builder().withDepth(169).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.1100).build())).build(),
            Depth.builder().withDepth(174).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.0800).build())).build(),
            Depth.builder().withDepth(179).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.0500).build())).build(),
            Depth.builder().withDepth(184).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.0200).build())).build(),
            Depth.builder().withDepth(189).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.9900).build())).build(),
            Depth.builder().withDepth(194).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.9700).build())).build(),
            Depth.builder().withDepth(199).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.9400).build())).build(),
            Depth.builder().withDepth(218).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.7500).build())).build(),
            Depth.builder().withDepth(238).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.6000).build())).build(),
            Depth.builder().withDepth(258).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.3700).build())).build(),
            Depth.builder().withDepth(278).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.9300).build())).build(),
            Depth.builder().withDepth(298).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.7200).build())).build(),
            Depth.builder().withDepth(318).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.4800).build())).build(),
            Depth.builder().withDepth(337).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.1600).build())).build(),
            Depth.builder().withDepth(357).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.8000).build())).build(),
            Depth.builder().withDepth(377).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.6600).build())).build(),
            Depth.builder().withDepth(397).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.3100).build())).build(),
            Depth.builder().withDepth(446).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(12.4700).build())).build(),
            Depth.builder().withDepth(496).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(11.7400).build())).build(),
            Depth.builder().withDepth(546).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.9700).build())).build(),
            Depth.builder().withDepth(595).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.4300).build())).build(),
            Depth.builder().withDepth(645).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(9.69000).build())).build(),
            Depth.builder().withDepth(694).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(8.42000).build())).build(),
            Depth.builder().withDepth(744).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(7.20000).build())).build(),
            Depth.builder().withDepth(793).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(6.22000).build())).build(),
            Depth.builder().withDepth(842).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.48000).build())).build(),
            Depth.builder().withDepth(892).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.02000).build())).build(),
            Depth.builder().withDepth(941).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.59000).build())).build(),
            Depth.builder().withDepth(991).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.18000).build())).build(),
            Depth.builder().withDepth(1040).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.05000).build())).build(),
            Depth.builder().withDepth(1068).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.01000).build())).build()
            ))
        .build();


    Cast realProfile3 = Cast.builder()
        .withCastNumber(3)
        .withLatitude(-28.36)
        .withLongitude(-0.752)
        .withGeohash("7fz")
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 10)
        .withTime(0.1895833)
        .withCruiseNumber(3)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(22.9400).build())).build(),
            Depth.builder().withDepth(9).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(21.8800).build())).build(),
            Depth.builder().withDepth(15).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(21.1200).build())).build(),
            Depth.builder().withDepth(21).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6100).build())).build(),
            Depth.builder().withDepth(27).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.3600).build())).build(),
            Depth.builder().withDepth(33).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.5500).build())).build(),
            Depth.builder().withDepth(39).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.1200).build())).build(),
            Depth.builder().withDepth(45).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.5600).build())).build(),
            Depth.builder().withDepth(51).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.9400).build())).build(),
            Depth.builder().withDepth(57).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.6900).build())).build(),
            Depth.builder().withDepth(63).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.4800).build())).build(),
            Depth.builder().withDepth(69).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.2600).build())).build(),
            Depth.builder().withDepth(74).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.1000).build())).build(),
            Depth.builder().withDepth(80).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.9000).build())).build(),
            Depth.builder().withDepth(86).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.7400).build())).build(),
            Depth.builder().withDepth(92).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.5800).build())).build(),
            Depth.builder().withDepth(98).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.4200).build())).build(),
            Depth.builder().withDepth(104).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2900).build())).build(),
            Depth.builder().withDepth(110).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.1900).build())).build(),
            Depth.builder().withDepth(116).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.0700).build())).build(),
            Depth.builder().withDepth(122).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.9200).build())).build(),
            Depth.builder().withDepth(140).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.3700).build())).build(),
            Depth.builder().withDepth(170).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.5000).build())).build(),
            Depth.builder().withDepth(200).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.8400).build())).build(),
            Depth.builder().withDepth(229).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.2600).build())).build(),
            Depth.builder().withDepth(259).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(12.8200).build())).build(),
            Depth.builder().withDepth(289).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(12.3100).build())).build(),
            Depth.builder().withDepth(319).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(11.9200).build())).build(),
            Depth.builder().withDepth(348).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(11.4300).build())).build(),
            Depth.builder().withDepth(378).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.9800).build())).build(),
            Depth.builder().withDepth(408).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.4100).build())).build(),
            Depth.builder().withDepth(438).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(9.77000).build())).build(),
            Depth.builder().withDepth(482).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(8.75000).build())).build(),
            Depth.builder().withDepth(542).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(7.66000).build())).build(),
            Depth.builder().withDepth(601).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(6.77000).build())).build(),
            Depth.builder().withDepth(660).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.91000).build())).build(),
            Depth.builder().withDepth(720).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.14000).build())).build(),
            Depth.builder().withDepth(779).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.63000).build())).build(),
            Depth.builder().withDepth(839).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.25000).build())).build(),
            Depth.builder().withDepth(898).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.00000).build())).build(),
            Depth.builder().withDepth(957).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.75000).build())).build(),
            Depth.builder().withDepth(1017).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.61000).build())).build(),
            Depth.builder().withDepth(1076).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.46000).build())).build(),
            Depth.builder().withDepth(1135).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.34000).build())).build(),
            Depth.builder().withDepth(1194).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.24000).build())).build(),
            Depth.builder().withDepth(1254).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.21000).build())).build(),
            Depth.builder().withDepth(1313).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.21000).build())).build(),
            Depth.builder().withDepth(1372).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.21000).build())).build(),
            Depth.builder().withDepth(1431).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.18000).build())).build(),
            Depth.builder().withDepth(1491).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.12000).build())).build(),
            Depth.builder().withDepth(1550).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.08000).build())).build(),
            Depth.builder().withDepth(1609).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.06000).build())).build(),
            Depth.builder().withDepth(1668).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.04000).build())).build(),
            Depth.builder().withDepth(1727).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.02000).build())).build(),
            Depth.builder().withDepth(1786).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(2.97000).build())).build(),
            Depth.builder().withDepth(1845).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(2.88000).build())).build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Arrays.asList(realProfile2, realProfile3), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_background_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_constant_value_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_increasing_depth_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_range_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_suspect.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_stability_check.parquet").toString());


    List<CastCheckResult> results = checkCustom.joinResultDataset(context).collectAsList();
    results.sort(Comparator.comparingInt(CastCheckResult::getCastNumber));
    assertEquals(2, results.size());
    assertEquals(CastCheckResult.builder().withCastNumber(2).withPassed(false).withFailedDepths(Arrays.asList(
        54, 55
    )).build(), results.get(0));

  }


  @Test
  public void testEnStdLevelBkgAndBuddyRealProfiles3() throws Exception {
    Cast realProfile2 = Cast.builder()
        .withCastNumber(2)
        .withLatitude(-30.229)
        .withLongitude(2.658)
        .withGeohash("k49")
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 10)
        .withTime(0D)
        .withCruiseNumber(2)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(21.4200).build())).build(),
            Depth.builder().withDepth(10).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(21.1300).build())).build(),
            Depth.builder().withDepth(15).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.4800).build())).build(),
            Depth.builder().withDepth(20).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.8400).build())).build(),
            Depth.builder().withDepth(25).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.2000).build())).build(),
            Depth.builder().withDepth(30).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.9400).build())).build(),
            Depth.builder().withDepth(35).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.7600).build())).build(),
            Depth.builder().withDepth(40).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.5000).build())).build(),
            Depth.builder().withDepth(45).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.0700).build())).build(),
            Depth.builder().withDepth(50).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.5900).build())).build(),
            Depth.builder().withDepth(55).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.3400).build())).build(),
            Depth.builder().withDepth(60).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.0000).build())).build(),
            Depth.builder().withDepth(65).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.8000).build())).build(),
            Depth.builder().withDepth(70).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.6200).build())).build(),
            Depth.builder().withDepth(74).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.5700).build())).build(),
            Depth.builder().withDepth(79).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.4900).build())).build(),
            Depth.builder().withDepth(84).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.4500).build())).build(),
            Depth.builder().withDepth(89).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.4100).build())).build(),
            Depth.builder().withDepth(94).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3900).build())).build(),
            Depth.builder().withDepth(99).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3500).build())).build(),
            Depth.builder().withDepth(104).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3300).build())).build(),
            Depth.builder().withDepth(109).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3300).build())).build(),
            Depth.builder().withDepth(114).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3300).build())).build(),
            Depth.builder().withDepth(119).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3200).build())).build(),
            Depth.builder().withDepth(124).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.3000).build())).build(),
            Depth.builder().withDepth(129).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2800).build())).build(),
            Depth.builder().withDepth(134).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2700).build())).build(),
            Depth.builder().withDepth(139).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2400).build())).build(),
            Depth.builder().withDepth(144).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2300).build())).build(),
            Depth.builder().withDepth(149).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2100).build())).build(),
            Depth.builder().withDepth(154).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2000).build())).build(),
            Depth.builder().withDepth(159).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.1700).build())).build(),
            Depth.builder().withDepth(164).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.1400).build())).build(),
            Depth.builder().withDepth(169).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.1100).build())).build(),
            Depth.builder().withDepth(174).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.0800).build())).build(),
            Depth.builder().withDepth(179).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.0500).build())).build(),
            Depth.builder().withDepth(184).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.0200).build())).build(),
            Depth.builder().withDepth(189).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.9900).build())).build(),
            Depth.builder().withDepth(194).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.9700).build())).build(),
            Depth.builder().withDepth(199).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.9400).build())).build(),
            Depth.builder().withDepth(218).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.7500).build())).build(),
            Depth.builder().withDepth(238).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.6000).build())).build(),
            Depth.builder().withDepth(258).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.3700).build())).build(),
            Depth.builder().withDepth(278).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.9300).build())).build(),
            Depth.builder().withDepth(298).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.7200).build())).build(),
            Depth.builder().withDepth(318).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.4800).build())).build(),
            Depth.builder().withDepth(337).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.1600).build())).build(),
            Depth.builder().withDepth(357).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.8000).build())).build(),
            Depth.builder().withDepth(377).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.6600).build())).build(),
            Depth.builder().withDepth(397).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.3100).build())).build(),
            Depth.builder().withDepth(446).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(12.4700).build())).build(),
            Depth.builder().withDepth(496).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(11.7400).build())).build(),
            Depth.builder().withDepth(546).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.9700).build())).build(),
            Depth.builder().withDepth(595).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.4300).build())).build(),
            Depth.builder().withDepth(645).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(9.69000).build())).build(),
            Depth.builder().withDepth(694).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(8.42000).build())).build(),
            Depth.builder().withDepth(744).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(7.20000).build())).build(),
            Depth.builder().withDepth(793).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(6.22000).build())).build(),
            Depth.builder().withDepth(842).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.48000).build())).build(),
            Depth.builder().withDepth(892).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.02000).build())).build(),
            Depth.builder().withDepth(941).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.59000).build())).build(),
            Depth.builder().withDepth(991).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.18000).build())).build(),
            Depth.builder().withDepth(1040).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.05000).build())).build(),
            Depth.builder().withDepth(1068).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.01000).build())).build()
        ))
        .build();


    Cast realProfile3 = Cast.builder()
        .withCastNumber(3)
        .withLatitude(-28.36)
        .withLongitude(-0.752)
        .withGeohash("7fz")
        .withYear((short) 2000)
        .withMonth((short) 1)
        .withDay((short) 10)
        .withTime(0.1895833)
        .withCruiseNumber(3)
        .withDepths(Arrays.asList(
            Depth.builder().withDepth(5).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(22.9400).build())).build(),
            Depth.builder().withDepth(9).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(21.8800).build())).build(),
            Depth.builder().withDepth(15).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(21.1200).build())).build(),
            Depth.builder().withDepth(21).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.6100).build())).build(),
            Depth.builder().withDepth(27).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(20.3600).build())).build(),
            Depth.builder().withDepth(33).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.5500).build())).build(),
            Depth.builder().withDepth(39).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(19.1200).build())).build(),
            Depth.builder().withDepth(45).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(18.5600).build())).build(),
            Depth.builder().withDepth(51).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.9400).build())).build(),
            Depth.builder().withDepth(57).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.6900).build())).build(),
            Depth.builder().withDepth(63).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.4800).build())).build(),
            Depth.builder().withDepth(69).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.2600).build())).build(),
            Depth.builder().withDepth(74).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(17.1000).build())).build(),
            Depth.builder().withDepth(80).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.9000).build())).build(),
            Depth.builder().withDepth(86).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.7400).build())).build(),
            Depth.builder().withDepth(92).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.5800).build())).build(),
            Depth.builder().withDepth(98).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.4200).build())).build(),
            Depth.builder().withDepth(104).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.2900).build())).build(),
            Depth.builder().withDepth(110).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.1900).build())).build(),
            Depth.builder().withDepth(116).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(16.0700).build())).build(),
            Depth.builder().withDepth(122).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.9200).build())).build(),
            Depth.builder().withDepth(140).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(15.3700).build())).build(),
            Depth.builder().withDepth(170).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(14.5000).build())).build(),
            Depth.builder().withDepth(200).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.8400).build())).build(),
            Depth.builder().withDepth(229).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(13.2600).build())).build(),
            Depth.builder().withDepth(259).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(12.8200).build())).build(),
            Depth.builder().withDepth(289).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(12.3100).build())).build(),
            Depth.builder().withDepth(319).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(11.9200).build())).build(),
            Depth.builder().withDepth(348).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(11.4300).build())).build(),
            Depth.builder().withDepth(378).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.9800).build())).build(),
            Depth.builder().withDepth(408).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(10.4100).build())).build(),
            Depth.builder().withDepth(438).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(9.77000).build())).build(),
            Depth.builder().withDepth(482).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(8.75000).build())).build(),
            Depth.builder().withDepth(542).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(7.66000).build())).build(),
            Depth.builder().withDepth(601).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(6.77000).build())).build(),
            Depth.builder().withDepth(660).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.91000).build())).build(),
            Depth.builder().withDepth(720).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(5.14000).build())).build(),
            Depth.builder().withDepth(779).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.63000).build())).build(),
            Depth.builder().withDepth(839).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.25000).build())).build(),
            Depth.builder().withDepth(898).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(4.00000).build())).build(),
            Depth.builder().withDepth(957).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.75000).build())).build(),
            Depth.builder().withDepth(1017).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.61000).build())).build(),
            Depth.builder().withDepth(1076).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.46000).build())).build(),
            Depth.builder().withDepth(1135).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.34000).build())).build(),
            Depth.builder().withDepth(1194).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.24000).build())).build(),
            Depth.builder().withDepth(1254).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.21000).build())).build(),
            Depth.builder().withDepth(1313).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.21000).build())).build(),
            Depth.builder().withDepth(1372).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.21000).build())).build(),
            Depth.builder().withDepth(1431).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.18000).build())).build(),
            Depth.builder().withDepth(1491).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.12000).build())).build(),
            Depth.builder().withDepth(1550).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.08000).build())).build(),
            Depth.builder().withDepth(1609).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.06000).build())).build(),
            Depth.builder().withDepth(1668).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.04000).build())).build(),
            Depth.builder().withDepth(1727).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(3.02000).build())).build(),
            Depth.builder().withDepth(1786).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(2.97000).build())).build(),
            Depth.builder().withDepth(1845).withData(Collections.singletonList(ProfileData.builder().withVariable(TEMPERATURE).withValue(2.88000).build())).build()
        ))
        .build();

    Dataset<Cast> dataset = spark.createDataset(Arrays.asList(realProfile2, realProfile3), Encoders.bean(Cast.class));
    dataset.write().parquet(TEST_PARQUET);

    Dataset<CastCheckResult> otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_background_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_constant_value_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_increasing_depth_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_range_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_check.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_spike_and_step_suspect.parquet").toString());

    otherResult = spark.createDataset(Arrays.asList(
        CastCheckResult.builder().withCastNumber(2).withPassed(true).build(),
        CastCheckResult.builder().withCastNumber(3).withPassed(true).build()
    ), Encoders.bean(CastCheckResult.class));
    otherResult.write().parquet(TEMP_DIR.resolve("EN_stability_check.parquet").toString());


    List<CastCheckResult> results = check.joinResultDataset(context).collectAsList();
    results.sort(Comparator.comparingInt(CastCheckResult::getCastNumber));
    assertEquals(2, results.size());
    assertEquals(CastCheckResult.builder().withCastNumber(2).withPassed(true).build(), results.get(0));

  }


}