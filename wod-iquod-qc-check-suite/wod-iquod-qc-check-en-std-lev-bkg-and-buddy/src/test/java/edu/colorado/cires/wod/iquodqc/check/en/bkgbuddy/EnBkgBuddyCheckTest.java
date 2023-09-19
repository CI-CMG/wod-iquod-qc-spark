package edu.colorado.cires.wod.iquodqc.check.en.bkgbuddy;


import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckInitializationContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
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
    CastCheckResult c1Sss = CastCheckResult.builder(rows.get(0).getStruct(rows.get(0).fieldIndex(CheckNames.EN_SPIKE_AND_STEP_SUSPECT.getName()))).build();
    CastCheckResult c1Sc = CastCheckResult.builder(rows.get(0).getStruct(rows.get(0).fieldIndex(CheckNames.EN_STABILITY_CHECK.getName()))).build();

    assertEquals(1, c1Bg.getCastNumber());
    assertEquals(1, c1Cv.getCastNumber());
    assertEquals(1, c1Id.getCastNumber());
    assertEquals(1, c1Rc.getCastNumber());
    assertEquals(1, c1Ssc.getCastNumber());
    assertEquals(1, c1Sss.getCastNumber());
    assertEquals(1, c1Sc.getCastNumber());

    Row c1Buddy = rows.get(0).getStruct(rows.get(0).fieldIndex("buddy"));
    Cast c1b = Cast.builder(c1Buddy.getStruct(c1Buddy.fieldIndex("cast"))).build();

    Row c2Buddy = rows.get(1).getStruct(rows.get(1).fieldIndex("buddy"));
    Cast c2b = Cast.builder(c2Buddy.getStruct(c2Buddy.fieldIndex("cast"))).build();

    Row c3Buddy = rows.get(2).getStruct(rows.get(2).fieldIndex("buddy"));
    Cast c3b = Cast.builder(c3Buddy.getStruct(c3Buddy.fieldIndex("cast"))).build();

    Row c4Buddy = rows.get(3).getStruct(rows.get(3).fieldIndex("buddy"));
    Row c5Buddy = rows.get(4).getStruct(rows.get(4).fieldIndex("buddy"));

    assertEquals(3, c1b.getCastNumber());
    assertEquals(3, c2b.getCastNumber());
    assertEquals(2, c3b.getCastNumber());
    assertNull(c4Buddy);
    assertNull(c5Buddy);

    CastCheckResult c3Bg = CastCheckResult.builder(c2Buddy.getStruct(c2Buddy.fieldIndex(CheckNames.EN_BACKGROUND_CHECK.getName()))).build();
    CastCheckResult c3Cv = CastCheckResult.builder(c2Buddy.getStruct(c2Buddy.fieldIndex(CheckNames.EN_CONSTANT_VALUE_CHECK.getName()))).build();
    CastCheckResult c3Id = CastCheckResult.builder(c2Buddy.getStruct(c2Buddy.fieldIndex(CheckNames.EN_INCREASING_DEPTH_CHECK.getName()))).build();
    CastCheckResult c3Rc = CastCheckResult.builder(c2Buddy.getStruct(c2Buddy.fieldIndex(CheckNames.EN_RANGE_CHECK.getName()))).build();
    CastCheckResult c3Ssc = CastCheckResult.builder(c2Buddy.getStruct(c2Buddy.fieldIndex(CheckNames.EN_SPIKE_AND_STEP_CHECK.getName()))).build();
    CastCheckResult c3Sss = CastCheckResult.builder(c2Buddy.getStruct(c2Buddy.fieldIndex(CheckNames.EN_SPIKE_AND_STEP_SUSPECT.getName()))).build();
    CastCheckResult c3Sc = CastCheckResult.builder(c2Buddy.getStruct(c2Buddy.fieldIndex(CheckNames.EN_STABILITY_CHECK.getName()))).build();

    assertEquals(3, c3Bg.getCastNumber());
    assertEquals(3, c3Cv.getCastNumber());
    assertEquals(3, c3Id.getCastNumber());
    assertEquals(3, c3Rc.getCastNumber());
    assertEquals(3, c3Ssc.getCastNumber());
    assertEquals(3, c3Sss.getCastNumber());
    assertEquals(3, c3Sc.getCastNumber());

  }


}