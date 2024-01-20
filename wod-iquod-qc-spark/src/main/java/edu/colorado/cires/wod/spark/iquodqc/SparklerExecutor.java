package edu.colorado.cires.wod.spark.iquodqc;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class SparklerExecutor implements Runnable {

  private static final int MAX_RECORDS_PER_FILE = 500000;

  private final SparkSession spark;
  private final String inputBucket;
  private final String outputBucket;
  private final String inputPrefix;
  private final List<String> datasets;
  private final List<String> processingLevels;
  private final String outputPrefix;
  private final Set<String> checksToRun;
  private final Properties properties;
  private final boolean emr;

  public SparklerExecutor(
      SparkSession spark,
      String inputBucket,
      String outputBucket,
      String inputPrefix,
      List<String> datasets,
      List<String> processingLevels,
      String outputPrefix,
      Set<String> checksToRun,
      Properties properties,
      boolean emr) {
    this.spark = spark;
    this.inputBucket = inputBucket;
    this.outputBucket = outputBucket;
    this.inputPrefix = inputPrefix;
    this.datasets = datasets;
    this.processingLevels = processingLevels;
    this.outputPrefix = outputPrefix;
    this.checksToRun = Collections.unmodifiableSet(new LinkedHashSet<>(checksToRun));
    this.properties = properties;
    this.emr = emr;
  }

  @Override
  public void run() {
    List<CastCheck> checks = CheckResolver.getChecks(checksToRun, properties);
    for (String dataset : datasets) {
      for (String processingLevel : processingLevels) {
        for (CastCheck check : checks) {
          CheckRunner runner = new CheckRunner(dataset, processingLevel, check, properties);
          runner.run();
        }
      }
    }
  }

  private class CheckRunner implements Runnable {

    private final String dataset;
    private final String processingLevel;
    private final CastCheck check;
    private final Properties properties;
    private final String inputUri;
    private final String outputUri;

    private CheckRunner(String dataset, String processingLevel, CastCheck check, Properties properties) {
      this.dataset = dataset;
      this.processingLevel = processingLevel;
      this.check = check;
      this.properties = properties;
      inputUri = getInputUri();
      outputUri = getOutputUri(this.check.getName());
    }

    private String getInputUri() {
      StringBuilder sb = new StringBuilder(emr ? "s3://" : "s3a://").append(inputBucket).append("/");
      if (inputPrefix != null) {
        sb.append(inputPrefix.replaceAll("/+$", "")).append("/");
      }
      sb.append(processingLevel).append("/")
          .append("WOD_").append(dataset).append("_").append(processingLevel).append(".parquet");
      return sb.toString();
    }

    private String getOutputUri(String checkName) {
      StringBuilder parquetUri = new StringBuilder(emr ? "s3://" : "s3a://").append(outputBucket).append("/");
      if (outputPrefix != null) {
        parquetUri.append(outputPrefix.replaceAll("/+$", "")).append("/");
      }
      parquetUri.append(dataset).append("/").append(processingLevel).append("/")
          .append(checkName).append(".parquet");
      return parquetUri.toString();
    }

    @Override
    public void run() {
      long start = System.currentTimeMillis();
      System.err.println("Running " + check.getName());

      CastCheckContext context = new CastCheckContext() {
        @Override
        public SparkSession getSparkSession() {
          return spark;
        }

        @Override
        public Dataset<Cast> readCastDataset() {
          return spark.read().parquet(inputUri).as(Encoders.bean(Cast.class));
        }

        @Override
        public Dataset<CastCheckResult> readCastCheckResultDataset(String checkName) {
          return spark.read().parquet(getOutputUri(checkName)).as(Encoders.bean(CastCheckResult.class));
        }

        @Override
        public Properties getProperties() {
          return properties;
        }
      };

      Dataset<CastCheckResult> resultDataset = check.joinResultDataset(context);

      System.err.println("Writing " + outputUri);

      resultDataset.write().mode(SaveMode.Overwrite).option("maxRecordsPerFile", MAX_RECORDS_PER_FILE).parquet(outputUri);
      long end = System.currentTimeMillis();
      Duration duration = Duration.ofMillis(end - start);
      System.err.println("Finished " + check.getName() + " in " + duration);
    }
  }


}
