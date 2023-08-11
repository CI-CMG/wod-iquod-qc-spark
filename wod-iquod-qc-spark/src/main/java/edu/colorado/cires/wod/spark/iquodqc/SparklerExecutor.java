package edu.colorado.cires.wod.spark.iquodqc;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
  private List<String> processingLevels;
  private final String outputPrefix;
  private final Set<String> checksToRun;
  private final Properties properties;
  private final ExecutorService executor;
  private final Set<CheckRunner> runningChecks = new HashSet<>();

  private boolean complete = false;

  public SparklerExecutor(
      SparkSession spark,
      String inputBucket,
      String outputBucket,
      String inputPrefix,
      List<String> datasets,
      List<String> processingLevels,
      String outputPrefix,
      int concurrency,
      Set<String> checksToRun
  ) {
    this.spark = spark;
    this.inputBucket = inputBucket;
    this.outputBucket = outputBucket;
    this.inputPrefix = inputPrefix;
    this.datasets = datasets;
    this.processingLevels = processingLevels;
    this.outputPrefix = outputPrefix;
    this.checksToRun = Collections.unmodifiableSet(new LinkedHashSet<>(checksToRun));
    properties = new Properties();
    try (InputStream in = this.getClass().getResourceAsStream("spark.properties")) {
      properties.load(in);
    } catch (IOException e) {
      throw new RuntimeException("Unable to load properties", e);
    }
    executor = Executors.newFixedThreadPool(concurrency);
  }

  @Override
  public void run() {
    for (String dataset : datasets) {
      for (String processingLevel : processingLevels) {
        CheckResolver checkResolver = new CheckResolver(checksToRun, properties);
        Collection<CastCheck> initialChecks = checkResolver.getRunnableChecks();
        if (initialChecks.isEmpty()) {
          throw new IllegalStateException("Could not determine tests to run");
        }
        initialChecks.forEach(check -> startCheck(check, dataset, processingLevel, checkResolver));
      }
    }

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

    executor.shutdown();
    try {
      if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
    }

  }

  private synchronized void startCheck(CastCheck check, String dataset, String processingLevel, CheckResolver checkResolver) {
    CheckRunner runner = new CheckRunner(dataset, processingLevel, check, properties, checkResolver);
    runningChecks.add(runner);
    executor.submit(runner);
  }

  private synchronized void startCheck(CastCheck check, CheckRunner parent) {
    CheckRunner runner = new CheckRunner(check, parent);
    runningChecks.add(runner);
    executor.submit(runner);
  }

  private synchronized void completeCheck(CheckRunner runner) {
    runningChecks.remove(runner);
    Set<CastCheck> newChecks = runner.completed();

    if (newChecks.isEmpty()) {
      if (runningChecks.isEmpty()) {
        complete = true;
        notifyAll();
      }
    } else {
      newChecks.forEach(check -> startCheck(check, runner));
    }
  }

  private class CheckRunner implements Runnable {

    private final String dataset;
    private final String processingLevel;
    private final CastCheck check;
    private final Properties properties;
    private final CheckResolver checkResolver;

    private CheckRunner(String dataset, String processingLevel, CastCheck check, Properties properties, CheckResolver checkResolver) {
      this.dataset = dataset;
      this.processingLevel = processingLevel;
      this.check = check;
      this.properties = properties;
      this.checkResolver = checkResolver;
    }

    private CheckRunner(CastCheck check, CheckRunner parent) {
      this.dataset = parent.dataset;
      this.processingLevel = parent.processingLevel;
      this.check = check;
      this.properties = parent.properties;
      this.checkResolver = parent.checkResolver;
    }

    public Set<CastCheck> completed() {
      Collection<CastCheck> newOrRunningChecks = checkResolver.completedCheck(check);
      Set<CastCheck> newChecks = new HashSet<>(newOrRunningChecks);
      Set<CastCheck> runningChecksForDatasetAndProcessingLevel = runningChecks.stream()
          .filter(cr -> cr.dataset.equals(dataset) && cr.processingLevel.equals(processingLevel))
          .map(cr -> cr.check).collect(Collectors.toSet());
      newChecks.removeAll(runningChecksForDatasetAndProcessingLevel);
      return newChecks;
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
          StringBuilder sb = new StringBuilder("s3a://").append(inputBucket).append("/");
          if (inputPrefix != null) {
            sb.append(inputPrefix.replaceAll("/+$", "")).append("/");
          }
          sb.append(processingLevel).append("/")
              .append("WOD_").append(dataset).append("_").append(processingLevel).append(".parquet");
          return spark.read()
              .parquet(sb.toString())
              .as(Encoders.bean(Cast.class));
        }

        @Override
        public Properties getProperties() {
          return properties;
        }
      };

      Dataset<CastCheckResult> resultDataset = check.joinResultDataset(context);

      StringBuilder parquetUri = new StringBuilder("s3a://").append(outputBucket).append("/");
      if (outputPrefix != null) {
        parquetUri.append(outputPrefix.replaceAll("/+$", "")).append("/");
      }
      parquetUri.append(dataset).append("/").append(processingLevel).append("/")
          .append(check.getName()).append(".parquet");
      System.err.println("Writing " + parquetUri.toString());
      resultDataset.write()
          .mode(SaveMode.Overwrite)
          .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
          .parquet(parquetUri.toString());
      completeCheck(this);
      System.err.println("Finished " + check.getName());
    }
  }


}
