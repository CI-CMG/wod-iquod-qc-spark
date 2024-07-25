package edu.colorado.cires.wod.spark.iquodqc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
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

}