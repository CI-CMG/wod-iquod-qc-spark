package edu.colorado.cires.wod.spark.iquodqc;

import static edu.colorado.cires.wod.iquodqc.common.CastConstants.ORIGINATORS_FLAGS;
import static edu.colorado.cires.wod.iquodqc.common.CastConstants.TEMPERATURE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.CastIoUtils;
import edu.colorado.cires.wod.iquodqc.check.api.Failures;
import edu.colorado.cires.wod.iquodqc.check.api.Summary;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.Metadata;
import edu.colorado.cires.wod.parquet.model.PrincipalInvestigator;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import edu.colorado.cires.wod.parquet.model.QcAttribute;
import edu.colorado.cires.wod.parquet.model.TaxonomicDataset;
import edu.colorado.cires.wod.parquet.model.Variable;
import io.findify.s3mock.S3Mock;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

public class SparklerExecutorTest {

  private static final String WCS84_PROJJSON = "{\"$schema\": \"https://proj.org/schemas/v0.7/projjson.schema.json\",\"type\": \"GeographicCRS\",\"name\": \"WGS 84\",\"datum_ensemble\": {\"name\": \"World Geodetic System 1984 ensemble\",\"members\": [{\"name\": \"World Geodetic System 1984 (Transit)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1166}},{\"name\": \"World Geodetic System 1984 (G730)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1152}},{\"name\": \"World Geodetic System 1984 (G873)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1153}},{\"name\": \"World Geodetic System 1984 (G1150)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1154}},{\"name\": \"World Geodetic System 1984 (G1674)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1155}},{\"name\": \"World Geodetic System 1984 (G1762)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1156}},{\"name\": \"World Geodetic System 1984 (G2139)\",\"id\": {\"authority\": \"EPSG\",\"code\": 1309}}],\"ellipsoid\": {\"name\": \"WGS 84\",\"semi_major_axis\": 6378137,\"inverse_flattening\": 298.257223563},\"accuracy\": \"2.0\",\"id\": {\"authority\": \"EPSG\",\"code\": 6326}},\"coordinate_system\": {\"subtype\": \"ellipsoidal\",\"axis\": [{\"name\": \"Geodetic latitude\",\"abbreviation\": \"Lat\",\"direction\": \"north\",\"unit\": \"degree\"},{\"name\": \"Geodetic longitude\",\"abbreviation\": \"Lon\",\"direction\": \"east\",\"unit\": \"degree\"}]},\"scope\": \"Horizontal component of 3D system.\",\"area\": \"World.\",\"bbox\": {\"south_latitude\": -90,\"west_longitude\": -180,\"north_latitude\": 90,\"east_longitude\": 180},\"id\": {\"authority\": \"EPSG\",\"code\": 4326}}";
  private static final String GEOPARQUET_VERSION = "1.0.0";

  private static final List<String> CHECK_NAMES = Arrays.asList(
      "Argo_impossible_date_test",
      "Argo_impossible_location_test"
  );

  private static final String inputBucket = "wod-input-bucket";
  private static final String outputBucket = "wod-qc-results-bucket";
  private static final String outputPrefix = "2024-02/data/qc";
  private static final String inputPrefix = "2024-02/data/parquet/yearly";
  private static final List<String> processingLevels = Collections.singletonList("OBS");

  private S3Mock s3Mock;
  private SparkSession spark;
  private S3Client s3;

  @BeforeEach
  public void before() throws Exception {
    s3Mock = new S3Mock.Builder().withInMemoryBackend().withPort(8001).build();
    s3Mock.start();
    spark = SedonaContext.create(SedonaContext
        .builder()
        .appName("test")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", "foo")
        .config("spark.hadoop.fs.s3a.secret.key", "bar")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:8001")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.change.detection.mode", "warn")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
        .getOrCreate());
    s3 = S3Client.builder()
        .serviceConfiguration(S3Configuration.builder()
            .pathStyleAccessEnabled(true)
            .build())
        .credentialsProvider(AnonymousCredentialsProvider.create())
        .endpointOverride(new URI("http://localhost:8001"))
        .region(Region.US_EAST_1)
        .build();
    s3.createBucket(c -> c.bucket(inputBucket));
    s3.createBucket(c -> c.bucket(outputBucket));
  }

  @AfterEach
  public void after() throws IOException {
    spark.close();
    s3Mock.shutdown();
  }

  @Test
  public void test() throws Exception {

    final String inputKey = inputPrefix + "/APB/OBS/APBO2006.parquet";
    final List<String> datasets = Collections.singletonList("APB");

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(Cast.builder()
        .withDataset("APB")
        .withCruiseNumber(5)
        .withCastNumber(1)
        .withYear(2006)
        .withMonth(6)
        .withDay(11)
        .withTime(0D)
        .withTimestamp(LocalDateTime.of(2006, 6, 11, 0, 0).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli())
        .withLongitude(55.4D)
        .withLatitude(10.5)
        .withProfileType(1)
        .withOriginatorsStationCode("foo")
        .withVariables(Collections.singletonList(Variable.builder()
            .withCode(5)
            .withMetadata(Collections.singletonList(Metadata.builder().withCode(2).withValue(55.4).build()))
            .build()))
        .withPrincipalInvestigators(Collections.singletonList(PrincipalInvestigator.builder()
            .withVariableCode(2)
            .withPiCode(88)
            .build()))
        .withAttributes(Collections.singletonList(Attribute.builder()
            .withCode(9)
            .withValue(534.5)
            .build()))
        .withBiologicalAttributes(Collections.singletonList(Attribute.builder()
            .withCode(7)
            .withValue(41.2)
            .build()))
        .withTaxonomicDatasets(Collections.singletonList(TaxonomicDataset.builder()
            .withValues(Collections.singletonList(QcAttribute.builder()
                .withCode(3)
                .withValue(88.4)
                .withQcFlag(1)
                .withOriginatorsFlag(2)
                .build()))
            .build()))
        .withDepths(Collections.singletonList(Depth.builder()
            .withDepth(25.0)
            .withDepthErrorFlag(1)
            .withOriginatorsFlag(0)
            .withData(Collections.singletonList(ProfileData.builder()
                .withVariableCode(3)
                .withValue(446.3)
                .withOriginatorsFlag(1)
                .withQcFlag(3)
                .build()))
            .build()))
        .build()), Encoders.bean(Cast.class));

    dataset.write()
        .format("geoparquet")
        .option("geoparquet.version", GEOPARQUET_VERSION)
        .option("geoparquet.crs", WCS84_PROJJSON)
        .save(String.format("s3a://%s/%s", inputBucket, inputKey));

    dataset.printSchema();

    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(Paths.get("src/test/resources/spark.properties"))) {
      properties.load(in);
    }

    SparklerExecutor executor = new SparklerExecutor(
        spark,
        inputBucket,
        outputBucket,
        inputPrefix,
        datasets,
        processingLevels,
        outputPrefix,
        new HashSet<>(CHECK_NAMES),
        properties,
        FileSystemType.s3, null, s3,
        false, false);
    executor.run();

    for (String name : CHECK_NAMES) {
      List<CastCheckResult> testResult = spark.read()
          .parquet(String.format("s3a://wod-qc-results-bucket/2024-02/data/qc/APB/OBS/2006/%s.parquet", name))
          .as(Encoders.bean(CastCheckResult.class))
          .collectAsList();
      assertEquals(1, testResult.size());
      CastCheckResult result = testResult.get(0);
      assertEquals(1, result.getCastNumber());
    }
  }

  @Test
  public void testSUR() throws Exception {

    final String inputKey = inputPrefix + "/SUR/OBS/SUR_ALL.parquet";
    final List<String> datasets = Collections.singletonList("SUR");

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(Cast.builder()
        .withDataset("SUR")
        .withCruiseNumber(5)
        .withCastNumber(1)
        .withYear(2006)
        .withMonth(6)
        .withDay(11)
        .withTime(0D)
        .withTimestamp(LocalDateTime.of(2006, 6, 11, 0, 0).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli())
        .withLongitude(55.4D)
        .withLatitude(10.5)
        .withProfileType(1)
        .withOriginatorsStationCode("foo")
        .withVariables(Collections.singletonList(Variable.builder()
            .withCode(5)
            .withMetadata(Collections.singletonList(Metadata.builder().withCode(2).withValue(55.4).build()))
            .build()))
        .withPrincipalInvestigators(Collections.singletonList(PrincipalInvestigator.builder()
            .withVariableCode(2)
            .withPiCode(88)
            .build()))
        .withAttributes(Collections.singletonList(Attribute.builder()
            .withCode(9)
            .withValue(534.5)
            .build()))
        .withBiologicalAttributes(Collections.singletonList(Attribute.builder()
            .withCode(7)
            .withValue(41.2)
            .build()))
        .withTaxonomicDatasets(Collections.singletonList(TaxonomicDataset.builder()
            .withValues(Collections.singletonList(QcAttribute.builder()
                .withCode(3)
                .withValue(88.4)
                .withQcFlag(1)
                .withOriginatorsFlag(2)
                .build()))
            .build()))
        .withDepths(Collections.singletonList(Depth.builder()
            .withDepth(25.0)
            .withDepthErrorFlag(1)
            .withOriginatorsFlag(0)
            .withData(Collections.singletonList(ProfileData.builder()
                .withVariableCode(3)
                .withValue(446.3)
                .withOriginatorsFlag(1)
                .withQcFlag(3)
                .build()))
            .build()))
        .build()), Encoders.bean(Cast.class));

    dataset.write()
        .format("geoparquet")
        .option("geoparquet.version", GEOPARQUET_VERSION)
        .option("geoparquet.crs", WCS84_PROJJSON)
        .save(String.format("s3a://%s/%s", inputBucket, inputKey));

    dataset.printSchema();

    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(Paths.get("src/test/resources/spark.properties"))) {
      properties.load(in);
    }

    SparklerExecutor executor = new SparklerExecutor(
        spark,
        inputBucket,
        outputBucket,
        inputPrefix,
        datasets,
        processingLevels,
        outputPrefix,
        new HashSet<>(CHECK_NAMES),
        properties,
        FileSystemType.s3, null, s3,
        false, false);
    executor.run();

    for (String name : CHECK_NAMES) {
      List<CastCheckResult> testResult = spark.read()
          .parquet(String.format("s3a://wod-qc-results-bucket/2024-02/data/qc/SUR/OBS/SUR_ALL/%s.parquet", name))
          .as(Encoders.bean(CastCheckResult.class))
          .collectAsList();
      assertEquals(1, testResult.size());
      CastCheckResult result = testResult.get(0);
      assertEquals(1, result.getCastNumber());
    }

  }

  @Test
  public void testPostProcessing() throws Exception {

    final String inputKey = inputPrefix + "/APB/OBS/APBO2006.parquet";
    final List<String> datasets = Collections.singletonList("APB");

    Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(Cast.builder()
        .withProfileType(0)
        .withDataset("TEST")
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
        .build()), Encoders.bean(Cast.class));

    dataset.write()
        .format("geoparquet")
        .option("geoparquet.version", GEOPARQUET_VERSION)
        .option("geoparquet.crs", WCS84_PROJJSON)
        .save(String.format("s3a://%s/%s", inputBucket, inputKey));



    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(Paths.get("src/test/resources/spark.properties"))) {
      properties.load(in);
    }

    SparklerExecutor executor = new SparklerExecutor(
        spark,
        inputBucket,
        outputBucket,
        inputPrefix,
        datasets,
        processingLevels,
        outputPrefix,
        Collections.singleton(CheckNames.IQUOD_FLAGS_CHECK.getName()),
        properties,
        FileSystemType.s3, null, s3,
        true, true);
    executor.run();

    List<Summary> summaryDataset = spark.read()
        .schema(Summary.structType())
        .json("s3a://wod-qc-results-bucket/2024-02/data/qc/APB/OBS/2006/summary.json")
        .as(Encoders.bean(Summary.class))
        .collectAsList();
    assertEquals(1, summaryDataset.size());
    Summary summary = summaryDataset.get(0);
    assertEquals("TEST", summary.getDataset());

    List<Failures> failureDataset = spark.read()
        .schema(Failures.structType())
        .json("s3a://wod-qc-results-bucket/2024-02/data/qc/APB/OBS/2006/failures.json")
        .as(Encoders.bean(Failures.class))
        .collectAsList();
    assertEquals(1, failureDataset.size());
    Failures failure = failureDataset.get(0);
    assertEquals(2, failure.getCastNumber());

    List<Cast> flagDataset = CastIoUtils.readCastDataset(spark, "s3a://wod-qc-results-bucket/2024-02/data/qc/APB/OBS/2006/APBO2006_flags.parquet").collectAsList();
    assertEquals(1, flagDataset.size());
    Cast cast = flagDataset.get(0);
    assertEquals(1, cast.getDepths().get(0).getData().get(0).getQcFlag());

  }

}