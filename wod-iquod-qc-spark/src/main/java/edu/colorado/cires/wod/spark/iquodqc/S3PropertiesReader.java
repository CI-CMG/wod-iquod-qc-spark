package edu.colorado.cires.wod.spark.iquodqc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class S3PropertiesReader implements PropertiesReader {

  private final S3Client s3;
  private final String bucket;
  private final String key;

  public S3PropertiesReader(String bucket, String key, String region, String accessKey, String secretKey) {
    this.bucket = bucket;
    this.key = key;
    S3ClientBuilder s3Builder = S3Client.builder();
    if (accessKey != null) {
      s3Builder.credentialsProvider(StaticCredentialsProvider.create(
          AwsBasicCredentials.create(accessKey, secretKey)
      ));
    }
    s3Builder.region(Region.of(region));
    s3 = s3Builder.build();
  }

  @Override
  public Properties readProperties() {
    Properties properties = new Properties();
    try (InputStream in = s3.getObject(c -> c.bucket(bucket).key(key))) {
      properties.load(in);
    } catch (IOException e) {
      throw new RuntimeException("Unable to load properties", e);
    }
    return properties;
  }
}
