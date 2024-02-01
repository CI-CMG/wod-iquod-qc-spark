package edu.colorado.cires.wod.spark.iquodqc;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.check.api.Failures;
import edu.colorado.cires.wod.iquodqc.check.api.Summary;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.postprocess.DatasetIO;
import edu.colorado.cires.wod.postprocess.PostProcessorContext;
import edu.colorado.cires.wod.postprocess.PostProcessorExecutor;
import edu.colorado.cires.wod.postprocess.addresulttocast.AddResultToCastPostProcessor;
import edu.colorado.cires.wod.postprocess.failurereport.FailureReportPostProcessor;
import edu.colorado.cires.wod.postprocess.summaryreport.CreateSummaryPostProcessor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;


public class SparklerExecutor implements Runnable {

  private static final int MAX_RECORDS_PER_FILE = 500000;
  private static final String IQUOD_FLAG_PRODUCING_CHECK = CheckNames.IQUOD_FLAGS_CHECK.getName();

  private final SparkSession spark;
  private final String inputBucket;
  private final String outputBucket;
  private final String inputPrefix;
  private final List<String> datasets;
  private final List<String> processingLevels;
  private final String outputPrefix;
  private final Set<String> checksToRun;
  private final Properties properties;
  private final FileSystemType fs;
  private final List<Integer> years;
  private final S3Client s3;
  private final boolean generateReports;
  private final boolean addFlagsToCast;

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
      FileSystemType fs, List<Integer> years, S3Client s3, boolean generateReports, boolean addFlagsToCast) {
    this.spark = spark;
    this.inputBucket = inputBucket;
    this.outputBucket = outputBucket;
    this.inputPrefix = inputPrefix;
    this.datasets = datasets;
    this.processingLevels = processingLevels;
    this.outputPrefix = outputPrefix;
    this.checksToRun = Collections.unmodifiableSet(new LinkedHashSet<>(checksToRun));
    this.properties = properties;
    this.fs = fs;
    this.years = years;
    this.s3 = s3;
    this.generateReports = generateReports;
    this.addFlagsToCast = addFlagsToCast;
  }

  @Override
  public void run() {
    List<CastCheck> checks = CheckResolver.getChecks(checksToRun, properties);

    Map<String, List<Integer>> resolvedYears = new HashMap<>(0); 
    
    for (String dataset : datasets) {
      for (String processingLevel : processingLevels) {
        resolvedYears.put(
            processingLevel,
            YearResolver.resolveYears(years, s3, inputBucket, inputPrefix, dataset, processingLevel)
        );
        for (CastCheck check : checks) {
          for (int year : resolvedYears.get(processingLevel)) {
            CheckRunner runner = new CheckRunner(dataset, processingLevel, check, properties, year);
            runner.run();
          }
        }
      }
    }
    
    if (addFlagsToCast) {
      if (checks.stream().map(CastCheck::getName).noneMatch(n -> n.equals(IQUOD_FLAG_PRODUCING_CHECK))) {
        System.out.printf(
            "%s has not been run. Flags will not be added to casts.%n\n",
            IQUOD_FLAG_PRODUCING_CHECK
        );
        System.err.printf(
            "%s has not been run. Flags will not be added to casts.%n\n",
            IQUOD_FLAG_PRODUCING_CHECK
        );
      } else {
        PostProcessorExecutor<Cast> postProcessorExecutor = new PostProcessorExecutor<>(
            new AddResultToCastPostProcessor()
        );
        for (String dataset : datasets) {
          for (String processingLevel: processingLevels) {
            for (int year : years) {
              postProcessorExecutor.execute(
                  getOutputCastURI(dataset, processingLevel, year),
                  new PostProcessorContext() {
                    @Override
                    public Dataset<Cast> readCastDataset() {
                      return DatasetIO.readDataset(
                          new String[]{getCastURI(dataset, processingLevel, year)},
                          spark,
                          Cast.class
                      );
                    }

                    @Override
                    public Dataset<CastCheckResult> readCheckResultDataset() {
                      return DatasetIO.readDataset(
                          new String[]{getCheckResultURI(IQUOD_FLAG_PRODUCING_CHECK, dataset, processingLevel, year)},
                          spark,
                          CastCheckResult.class
                      );
                    }
                  },
                  SaveMode.Overwrite,
                  MAX_RECORDS_PER_FILE
              );
            }
          }
        }
      }
    }
    
    if (generateReports) {
      PostProcessorExecutor<Summary> summaryExecutor = new PostProcessorExecutor<>(
          new CreateSummaryPostProcessor()
      );
      
      PostProcessorExecutor<Failures> failureExecutor = new PostProcessorExecutor<>(
          new FailureReportPostProcessor()
      );
      
      for (String dataset : datasets) {
        for (String processingLevel : processingLevels) {
          for (int year : resolvedYears.get(processingLevel)) {
            PostProcessorContext context = new PostProcessorContext() {
              @Override
              public Dataset<Cast> readCastDataset() {
                return DatasetIO.readDataset(
                    new String[]{getCastURI(dataset, processingLevel, year)},
                    spark,
                    Cast.class
                );
              }

              @Override
              public Dataset<CastCheckResult> readCheckResultDataset() {
                return DatasetIO.readDataset(
                    checks.stream()
                        .map(CastCheck::getName)
                        .map(name -> getCheckResultURI(name, dataset, processingLevel, year)).toArray(String[]::new),
                    spark,
                    CastCheckResult.class
                );
              }
            };
            
            summaryExecutor.execute(
                getOutputSummaryURI(dataset, processingLevel, year),
                context,
                SaveMode.Overwrite,
                1
            );
            
            failureExecutor.execute(
                getOutputFailuresURI(dataset, processingLevel, year),
                context,
                SaveMode.Overwrite,
                1
            );
          }
        }
      }
    }
  }

  private String getCastURI(String dataset, String processingLevel, int year) {
    StringBuilder sb = new StringBuilder(FileSystemPrefix.resolve(fs)).append(inputBucket).append("/");
    if (inputPrefix != null) {
      sb.append(inputPrefix.replaceAll("/+$", "")).append("/");
    }
    sb.append(dataset).append("/")
        .append(processingLevel).append("/")
        .append(dataset).append(processingLevel.charAt(0)).append(year).append(".parquet");
    return sb.toString();
  }
  
  private String getOutputCastURI(String dataset, String processingLevel, int year) {
    StringBuilder sb = new StringBuilder(FileSystemPrefix.resolve(fs)).append(outputBucket).append("/");
    if (outputPrefix != null) {
      sb.append(outputPrefix.replaceAll("/+$", "")).append("/");
    }

    sb.append(dataset).append("/")
        .append(processingLevel).append("/")
        .append(dataset).append(processingLevel.charAt(0)).append(year).append(".parquet");
    return sb.toString();
  }

  private String getOutputSummaryURI(String dataset, String processingLevel, int year) {
    StringBuilder sb = new StringBuilder(FileSystemPrefix.resolve(fs)).append(outputBucket).append("/");
    if (outputPrefix != null) {
      sb.append(outputPrefix.replaceAll("/+$", "")).append("/");
    }

    sb.append(dataset).append("/")
        .append(processingLevel).append("/")
        .append(dataset).append(processingLevel.charAt(0)).append(year).append("_summary").append(".json");
    return sb.toString();
  }

  private String getOutputFailuresURI(String dataset, String processingLevel, int year) {
    StringBuilder sb = new StringBuilder(FileSystemPrefix.resolve(fs)).append(outputBucket).append("/");
    if (outputPrefix != null) {
      sb.append(outputPrefix.replaceAll("/+$", "")).append("/");
    }

    sb.append(dataset).append("/")
        .append(processingLevel).append("/")
        .append(dataset).append(processingLevel.charAt(0)).append(year).append("_failures").append(".json");
    return sb.toString();
  }

  private String getCheckResultURI(String checkName, String dataset, String processingLevel, int year) {
    StringBuilder parquetUri = new StringBuilder(FileSystemPrefix.resolve(fs)).append(outputBucket).append("/");
    if (outputPrefix != null) {
      parquetUri.append(outputPrefix.replaceAll("/+$", "")).append("/");
    }
    parquetUri.append(dataset).append("/").append(processingLevel).append("/")
        .append(year).append("/")
        .append(checkName).append(".parquet");
    return parquetUri.toString();
  }

  private class CheckRunner implements Runnable {

    private final String dataset;
    private final String processingLevel;
    private final CastCheck check;
    private final Properties properties;
    private final String inputUri;
    private final String outputUri;
    private final int year;
    private final String prefix;

    private CheckRunner(String dataset, String processingLevel, CastCheck check, Properties properties, int year) {
      this.dataset = dataset;
      this.processingLevel = processingLevel;
      this.check = check;
      this.properties = properties;
      this.year = year;
      inputUri = getInputUri();
      outputUri = getOutputUri(this.check.getName());
      prefix = outputUri.replaceFirst("s3a://" + outputBucket + "/", "").replaceFirst("s3://" + outputBucket + "/", "").replaceFirst("file://" + outputBucket + "/", "");
    }

    private String getInputUri() {
      StringBuilder sb = new StringBuilder(FileSystemPrefix.resolve(fs)).append(inputBucket).append("/");
      if (inputPrefix != null) {
        sb.append(inputPrefix.replaceAll("/+$", "")).append("/");
      }
      sb.append(dataset).append("/")
          .append(processingLevel).append("/")
          .append(dataset).append(processingLevel.charAt(0)).append(year).append(".parquet");
      return sb.toString();
    }

    private String getOutputUri(String checkName) {
      StringBuilder parquetUri = new StringBuilder(FileSystemPrefix.resolve(fs)).append(outputBucket).append("/");
      if (outputPrefix != null) {
        parquetUri.append(outputPrefix.replaceAll("/+$", "")).append("/");
      }
      parquetUri.append(dataset).append("/").append(processingLevel).append("/")
          .append(year).append("/")
          .append(checkName).append(".parquet");
      return parquetUri.toString();
    }

    @Override
    public void run() {
      long start = System.currentTimeMillis();
      System.err.println("Running " + check.getName());
      System.out.println("Running " + check.getName());

      if (exists(s3, outputBucket, prefix + "/_SUCCESS")) {
        System.out.println("Skipping existing " + check.getName());
      } else {
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
        System.out.println("Writing " + outputUri);
        resultDataset.write().mode(SaveMode.Overwrite).option("maxRecordsPerFile", MAX_RECORDS_PER_FILE).parquet(outputUri);
        long end = System.currentTimeMillis();
        Duration duration = Duration.ofMillis(end - start);
        System.err.println("Finished " + check.getName() + " in " + duration);
        System.out.println("Finished " + check.getName() + " in " + duration);
      }
    }
  }
  private boolean exists(S3Client s3, String bucket, String key) {
    if (fs == FileSystemType.s3 || fs == FileSystemType.emrS3) {
      try {
        s3.headObject(c -> c.bucket(bucket).key(key));
      } catch (NoSuchKeyException e) {
        return false;
      }
      return true;
    } else {
      return Files.exists(Paths.get(bucket).resolve(key));
    }
  }

}
