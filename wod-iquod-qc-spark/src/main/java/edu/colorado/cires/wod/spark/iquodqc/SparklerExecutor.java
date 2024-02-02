package edu.colorado.cires.wod.spark.iquodqc;

import edu.colorado.cires.wod.iquodqc.check.api.CastCheck;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckContext;
import edu.colorado.cires.wod.iquodqc.check.api.CastCheckResult;
import edu.colorado.cires.wod.iquodqc.common.CheckNames;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.postprocess.DatasetIO;
import edu.colorado.cires.wod.postprocess.PostProcessorContext;
import edu.colorado.cires.wod.postprocess.PostProcessorRunner;
import edu.colorado.cires.wod.postprocess.addresulttocast.AddResultToCastPostProcessor;
import edu.colorado.cires.wod.postprocess.failurereport.FailureReportPostProcessor;
import edu.colorado.cires.wod.postprocess.summaryreport.CreateSummaryPostProcessor;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;


public class SparklerExecutor implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparklerExecutor.class);
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

    boolean willGenerateIquodFlags = checks.stream().map(CastCheck::getName).anyMatch(n -> n.equals(IQUOD_FLAG_PRODUCING_CHECK));
    if (generateReports && !willGenerateIquodFlags) {
      LOGGER.warn("{} not specified in --checks/-qc. Will not generate summary/failure reports.", IQUOD_FLAG_PRODUCING_CHECK);
    }
    
    if (addFlagsToCast && !willGenerateIquodFlags) {
      LOGGER.warn("{} not specified in --checks/-qc. Will not generate IQUOD flags.", IQUOD_FLAG_PRODUCING_CHECK);
    }
    
    for (String dataset : datasets) {
      for (String processingLevel : processingLevels) {
        List<Integer> resolvedYears = YearResolver.resolveYears(years, s3, inputBucket, inputPrefix, dataset, processingLevel);
        for (CastCheck check : checks) {
          for (int year : resolvedYears) {
            CheckRunner runner = new CheckRunner(dataset, processingLevel, check, properties, year);
            runner.run();
          }
        }

        if (addFlagsToCast && willGenerateIquodFlags) {
          for (int year : resolvedYears) {
            String outputURI = getOutputCastURI(dataset, processingLevel, year);
            if (
                exists(
                s3, 
                outputBucket,
                outputURI.replaceFirst("s3a://" + outputBucket + "/", "")
                    .replaceFirst("s3://" + outputBucket + "/", "")
                    .replaceFirst("file://" + outputBucket + "/", "") +
                    "/_SUCCESS"
                )
            ) {
              LOGGER.warn("Found existing casts with IQUOD flags. Will not reprocess casts: {}/{}/{}", dataset, processingLevel, year);
            } else {
              LOGGER.info("Adding IQUOD flags to casts: {}/{}/{}", dataset, processingLevel, year);
              new PostProcessorRunner<>(
                  new AddResultToCastPostProcessor(),
                  outputURI,
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
              ).run(); 
            }
          }
        }

        if (generateReports) {
          for (int year : resolvedYears) {
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

            if (willGenerateIquodFlags) {
              String outputURI = getOutputSummaryURI(dataset, processingLevel, year);
              if (
                  exists(
                      s3,
                      outputBucket,
                      outputURI.replaceFirst("s3a://" + outputBucket + "/", "")
                          .replaceFirst("s3://" + outputBucket + "/", "")
                          .replaceFirst("file://" + outputBucket + "/", "") +
                          "/_SUCCESS"
                  )
              ) {
                LOGGER.warn("Found existing summary report. Will not regenerate summary report: {}/{}/{}", dataset, processingLevel, year);
              } else {
                LOGGER.info("Generating summary report: {}/{}/{}", dataset, processingLevel, year);
                new PostProcessorRunner<>(
                    new CreateSummaryPostProcessor(),
                    outputURI,
                    context,
                    SaveMode.Overwrite,
                    1
                ).run(); 
              }

              outputURI = getOutputFailuresURI(dataset, processingLevel, year);
              if (
                  exists(
                      s3,
                      outputBucket,
                      outputURI.replaceFirst("s3a://" + outputBucket + "/", "")
                          .replaceFirst("s3://" + outputBucket + "/", "")
                          .replaceFirst("file://" + outputBucket + "/", "") +
                          "/_SUCCESS"
                  )
              ) {
                LOGGER.warn("Found existing failure reports. Will not regenerate failure reports: {}/{}/{}", dataset, processingLevel, year);
              } else {
                LOGGER.info("Generating failure reports: {}/{}/{}", dataset, processingLevel, year);
                new PostProcessorRunner<>(
                    new FailureReportPostProcessor(),
                    outputURI,
                    context,
                    SaveMode.Overwrite,
                    1
                ).run();
              }
              
            }
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
        .append(year).append("/")
        .append(dataset).append(processingLevel.charAt(0)).append(year).append("_flags").append(".parquet");
    return sb.toString();
  }

  private String getOutputSummaryURI(String dataset, String processingLevel, int year) {
    StringBuilder sb = new StringBuilder(FileSystemPrefix.resolve(fs)).append(outputBucket).append("/");
    if (outputPrefix != null) {
      sb.append(outputPrefix.replaceAll("/+$", "")).append("/");
    }

    sb.append(dataset).append("/")
        .append(processingLevel).append("/")
        .append(year).append("/")
        .append(dataset).append(processingLevel.charAt(0)).append(year).append("_summary").append(".parquet");
    return sb.toString();
  }

  private String getOutputFailuresURI(String dataset, String processingLevel, int year) {
    StringBuilder sb = new StringBuilder(FileSystemPrefix.resolve(fs)).append(outputBucket).append("/");
    if (outputPrefix != null) {
      sb.append(outputPrefix.replaceAll("/+$", "")).append("/");
    }

    sb.append(dataset).append("/")
        .append(processingLevel).append("/")
        .append(year).append("/")
        .append(dataset).append(processingLevel.charAt(0)).append(year).append("_failures").append(".parquet");
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

      if (exists(s3, outputBucket, prefix + "/_SUCCESS")) {
        LOGGER.info("Skipping existing {}: {}/{}/{}", check.getName(), dataset, processingLevel, year);
      } else {
        LOGGER.info("Running {}: {}/{}/{}", check.getName(), dataset, processingLevel, year);
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
        
        LOGGER.info("Writing {} output for {}/{}/{}: {}", check.getName(), dataset, processingLevel, year, outputUri);
        resultDataset.write().mode(SaveMode.Overwrite).option("maxRecordsPerFile", MAX_RECORDS_PER_FILE).parquet(outputUri);
        long end = System.currentTimeMillis();
        Duration duration = Duration.ofMillis(end - start);
        LOGGER.info("Finished {} in {}: {}/{}/{}", check.getName(), duration, dataset, processingLevel, year);
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
