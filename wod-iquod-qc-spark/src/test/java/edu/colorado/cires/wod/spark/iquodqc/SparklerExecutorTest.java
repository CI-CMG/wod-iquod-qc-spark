package edu.colorado.cires.wod.spark.iquodqc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Disabled
@Testcontainers
public class SparklerExecutorTest {

  private static final List<String> CHECK_NAMES = Arrays.asList(
      "AOML_gradient",
      "AOML_gross",
      "AOML_spike",
      "Argo_impossible_date_test",
      "Argo_impossible_location_test",
      "Argo_regional_range_test",
      "CSIRO_constant_bottom",
      "EN_background_available_check"
  );


  @Container
  public LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
      .withServices(S3);

  @Test
  public void test() throws Exception {
    final String inputBucket = "wod-input-bucket";
    final String outputBucket = "wod-qc-results-bucket";
    final String inputKey = "source/OBS/WOD_APB_OBS.parquet";
    final String outputPrefix = "qc";
    final String inputPrefix = "source";
    final List<String> datasets = Collections.singletonList("APB");
    final List<String> processingLevels = Collections.singletonList("OBS");

    SparkSession spark = SparkSession
        .builder()
        .appName("test")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", localstack.getAccessKey())
        .config("spark.hadoop.fs.s3a.secret.key", localstack.getSecretKey())
        .config("spark.hadoop.fs.s3a.endpoint", localstack.getEndpoint().toString())
        .config("spark.hadoop.fs.s3a.endpoint.region", localstack.getRegion())
        .getOrCreate();
    try {
      S3Client s3 = S3Client.builder()
          .credentialsProvider(StaticCredentialsProvider.create(
              AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
          ))
          .endpointOverride(localstack.getEndpoint())
          .region(Region.of(localstack.getRegion()))
          .build();

      s3.createBucket(c -> c.bucket(inputBucket));
      s3.createBucket(c -> c.bucket(outputBucket));

      Dataset<Cast> dataset = spark.createDataset(Collections.singletonList(Cast.builder()
          .withDataset("APB")
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
          .withGeohash("rdty")
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

      dataset.write().parquet(String.format("s3a://%s/%s", inputBucket, inputKey));

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
          new HashSet<>(),
          properties,
          FileSystemType.s3, null, s3,
          false, false);
      assert false;
      executor.run();

      for (String name : CHECK_NAMES) {
        List<CastCheckResult> testResult = spark.read()
            .parquet(String.format("s3a://wod-qc-results-bucket/qc/APB/OBS/%s.parquet", name))
            .as(Encoders.bean(CastCheckResult.class))
            .collectAsList();
        assertEquals(1, testResult.size());
        CastCheckResult result = testResult.get(0);
        assertEquals(1, result.getCastNumber());
      }
    } finally {
      spark.close();
    }

  }

}