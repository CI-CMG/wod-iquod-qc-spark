package edu.colorado.cires.wod.spark.iquodqc;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.Collection;
import java.util.HashSet;
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

  private final Set<CastCheck> runningChecks = new HashSet<>();
  private final CheckResolver checkResolver = new CheckResolver();

  private boolean complete = false;

  public SparklerExecutor(SparkSession spark, String inputBucket, String outputBucket, String inputKey, String outputPrefix) {
    this.spark = spark;
    this.inputBucket = inputBucket;
    this.outputBucket = outputBucket;
    this.inputKey = inputKey;
    this.outputPrefix = outputPrefix;
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
    Thread thread = new Thread(new CheckRunner(check));
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

    private CheckRunner(CastCheck check) {
      this.check = check;
    }

    @Override
    public void run() {
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
      };
      Dataset<CastCheckResult> resultDataset = check.joinResultDataset(context);
      resultDataset.write()
          .mode(SaveMode.Overwrite)
          .option("maxRecordsPerFile", 500000)
          .parquet(String.format("s3a://%s/%s/%s.parquet", outputBucket, outputPrefix.replaceAll("/+$", ""), check.getName()));
      completeCheck(check);
    }
  }


}
