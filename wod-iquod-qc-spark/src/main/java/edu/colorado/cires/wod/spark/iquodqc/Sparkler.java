package edu.colorado.cires.wod.spark.iquodqc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobResult;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;


@Command(
    name = "wod-iquod-qc-spark",
    description = "Executes Spark jobs generate IQUOD QC flags",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class
)
public class Sparkler implements Serializable, Runnable {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(Sparkler.class);

  private static final long serialVersionUID = 0L;

  @Option(names = {"-ib", "--input-bucket"}, required = true, description = "The input S3 bucket WOD Parquet files")
  private String inputBucket;

  @Option(names = {"-ibr", "--input-bucket-region"}, required = false, description = "The input S3 bucket region")
  private String inputBucketRegion;

  @Option(names = {"-ob", "--output-bucket"}, required = true, description = "The output S3 bucket where to put QC results")
  private String outputBucket;

  @Option(names = {"-obr", "--output-bucket-region"}, required = false, description = "The output S3 bucket region")
  private String outputBucketRegion;

  @Option(names = {"-qc", "--checks"}, split = ",", description = "A comma separated list of tests to run. If not provided, all tests will run")
  private List<String> checksToRun = new ArrayList<>(0);
  @Option(names = {"-ds", "--data-set"}, required = true, split = ",", defaultValue = "APB,CTD,DRB,GLD,MBT,MRB,OSD,PFL,SUR,UOR,XBT", description = "A comma separated list of data codes - Default: ${DEFAULT-VALUE}")
  private List<String> datasets;

  @Option(names = {"-p", "--processing-level"}, required = true, split = ",", defaultValue = "OBS", description = "A comma separated list of processing levels - Default: ${DEFAULT-VALUE}")
  private List<String> processingLevels;

  @Option(names = {"-y", "--year"}, required = false, split = ",", description = "A comma separated list of years")
  private List<Integer> years = new ArrayList<>(0);

  @Option(names = {"-ip", "--input-prefix"}, description = "An optional key prefix of where the dataset directory starts for the input file")
  private String inputPrefix;

  @Option(names = {"-op", "--output-prefix"}, description = "An optional key prefix of where to write output files if not in the root of the output bucket")
  private String outputPrefix;

  @Option(names = {"-pb", "--properties-bucket"}, required = true, description = "The bucket containing the properties file for this job")
  private String propertiesBucket;

  @Option(names = {"-pbr", "--properties-bucket-region"}, required = false, description = "The properties S3 bucket region")
  private String propertiesBucketRegion;

  @Option(names = {"-pk", "--properties-key"}, required = true, description = "The key to the properties file")
  private String propertiesKey;

  @Option(names = {"-pa", "--properties-access"}, description = "An optional access key for the properties bucket")
  private String propertiesAccessKey;
  @Option(names = {"-ps", "--properties-secret"}, description = "An optional secret key for the properties bucket")
  private String propertiesSecretKey;


  @Option(names = {"-ia", "--input-access"}, description = "An optional access key for the input bucket")
  private String inputAccessKey;
  @Option(names = {"-is", "--input-secret"}, description = "An optional secret key for the input bucket")
  private String inputSecretKey;

  @Option(names = {"-oa", "--output-access"}, description = "An optional access key for the output bucket")
  private String outputAccessKey;
  @Option(names = {"-os", "--output-secret"}, description = "An optional secret key for the output bucket")
  private String outputSecretKey;
  
  @Option(names = {"-gr", "--generate-reports"}, description = "Create failure reports for each cast and a summary report for each dataset - Default: ${DEFAULT-VALUE}", defaultValue = "false")
  private Boolean generateReports;
  
  @Option(names = {"-ftc", "--flags-to-cast"}, description = "Save new set of casts containing IQUOD flags (Requires that IQUOD flags are generated via IQUOD flags check) - ${DEFAULT-VALUE}", defaultValue = "false")
  private Boolean flagsToCast;

  @Option(names = {"-fs", "--file-system"}, description = "Optimize S3 access for EMR")
  private FileSystemType fs = FileSystemType.local;

  @Override
  public void run() {
    SparkSession.Builder sparkBuilder = SparkSession.builder();

    S3Client s3 = null;
    if (fs == FileSystemType.s3 || fs == FileSystemType.emrS3) {
      sparkBuilder.config(String.format("spark.hadoop.fs.s3a.bucket.%s.endpoint.region", outputBucket), outputBucketRegion);
      if (outputAccessKey != null) {
        sparkBuilder.config(String.format("spark.hadoop.fs.s3a.bucket.%s.access.key", outputBucket), outputAccessKey);
        sparkBuilder.config(String.format("spark.hadoop.fs.s3a.bucket.%s.secret.key", outputBucket), outputSecretKey);
      }
      if (inputAccessKey != null) {
        sparkBuilder.config(String.format("spark.hadoop.fs.s3a.bucket.%s.access.key", inputBucket), inputAccessKey);
        sparkBuilder.config(String.format("spark.hadoop.fs.s3a.bucket.%s.secret.key", inputBucket), inputSecretKey);
      }
      S3ClientBuilder s3Builder = S3Client.builder();
      s3 = s3Builder.build();
    }

    SparkSession spark = sparkBuilder.getOrCreate();

    spark.sparkContext().addSparkListener(new SparkListener() {

      @Override
      public void onJobEnd(SparkListenerJobEnd jobEnd) {
        JobResult result = jobEnd.jobResult();
        if (result instanceof JobFailed) {
          LOGGER.error("Failed job detected. Exiting.");
          spark.sparkContext().stop(1);
        }
      }
    });

    Properties properties;
    switch (fs) {
      case local:
        properties = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get(propertiesBucket).resolve(propertiesKey))) {
          properties.load(in);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        break;
      case emrS3:
      case s3:
        properties = new S3PropertiesReader(propertiesBucket, propertiesKey, propertiesBucketRegion, propertiesAccessKey, propertiesSecretKey).readProperties();
        break;
      default:
        throw new IllegalStateException("Unsupported file system type " + fs);
    }





    SparklerExecutor executor = new SparklerExecutor(
        spark,
        inputBucket,
        outputBucket,
        inputPrefix,
        datasets,
        processingLevels,
        outputPrefix,
        new HashSet<>(checksToRun),
        properties,
        fs, years, s3,
        generateReports,
        flagsToCast
    );
    executor.run();
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new Sparkler()).execute(args));
  }

}
