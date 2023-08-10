package edu.colorado.cires.wod.spark.iquodqc;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class SparklerExecutor implements Runnable {

  private final SparkSession spark;
  private final String inputBucket;
  private final String outputBucket;
  private final String inputKey;
  private final String outputPrefix;
  private final Properties properties;

  private final Set<CastCheck> runningChecks = new HashSet<>();
  private final CheckResolver checkResolver;

  private boolean complete = false;


  public SparklerExecutor(
      SparkSession spark,
      String inputBucket,
      String outputBucket,
      String inputKey,
      String outputPrefix,
      Set<String> checksToRun
  ) {
    this.spark = spark;
    this.inputBucket = inputBucket;
    this.outputBucket = outputBucket;
    this.inputKey = inputKey;
    this.outputPrefix = outputPrefix;
    checkResolver = new CheckResolver(checksToRun);
    properties = new Properties();
    try (InputStream in = this.getClass().getResourceAsStream("spark.properties")) {
      properties.load(in);
    } catch (IOException e) {
      throw new RuntimeException("Unable to load properties", e);
    }
  }

  @Override
  public void run() {
    Collection<CastCheck> initialChecks = checkResolver.getRunnableChecks();

    if (initialChecks.isEmpty()) {
      throw new IllegalStateException("Could not determine tests to run");
    }

    initialChecks.forEach(this::startCheck);

    synchronized (this) {
      while (!complete) {
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Waiting check executor was interrupted", e);
        }
      }
    }
  }

  private synchronized void startCheck(CastCheck check) {
    runningChecks.add(check);
    System.err.println("Starting check: " + check.getName());
    Thread thread = new Thread(new CheckRunner(check, properties));
    thread.start();
  }

  private synchronized void completeCheck(CastCheck check) {
    runningChecks.remove(check);
    Collection<CastCheck> newOrRunningChecks = checkResolver.completedCheck(check);

    Set<CastCheck> newChecks = new HashSet<>(newOrRunningChecks);
    newChecks.removeAll(runningChecks);

    if (newChecks.isEmpty()) {
      if (runningChecks.isEmpty()) {
        complete = true;
        notifyAll();
      }
    } else {
      newChecks.forEach(this::startCheck);
    }
  }

  private class CheckRunner implements Runnable {

    private final CastCheck check;
    private final Properties properties;

    private CheckRunner(CastCheck check, Properties properties) {
      this.check = check;
      this.properties = properties;
    }

    @Override
    public void run() {
      System.err.println("Running " + check.getName());
      CastCheckContext context = new CastCheckContext() {
        @Override
        public SparkSession getSparkSession() {
          return spark;
        }

        @Override
        public Dataset<Cast> readCastDataset() {
          return spark.read()
              .parquet(String.format("s3a://%s/%s", inputBucket, inputKey))
              .as(Encoders.bean(Cast.class));
        }

        @Override
        public Properties getProperties() {
          return properties;
        }
      };
      Dataset<CastCheckResult> resultDataset = check.joinResultDataset(context);

      StringBuilder parquetUri = new StringBuilder("s3a://")
          .append(outputBucket).append("/");
      if (outputPrefix != null) {
        parquetUri.append(outputPrefix.replaceAll("/+$", "")).append("/");
      }
      parquetUri.append(check.getName()).append(".parquet");
      System.err.println("Writing " + parquetUri.toString());
      resultDataset.write()
          .mode(SaveMode.Overwrite)
          .option("maxRecordsPerFile", 500000)
          .parquet(parquetUri.toString());
      completeCheck(check);
      System.err.println("Finished " + check.getName());
    }
  }


}
