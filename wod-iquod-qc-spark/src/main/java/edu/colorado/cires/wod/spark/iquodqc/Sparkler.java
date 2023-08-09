package edu.colorado.cires.wod.spark.iquodqc;

import java.io.Serializable;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;


@Command(
    name = "wod-iquod-qc-spark",
    description = "Executes Spark jobs generate IQUOD QC flags",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class
)
public class Sparkler implements Serializable, Runnable {

  private static final long serialVersionUID = 0L;

  @Option(names = {"-ib", "--input-bucket"}, required = true, description = "The input S3 bucket containing compressed ASCII WOD files")
  private String inputBucket;
  @Option(names = {"-ibr", "--input-bucket-region"}, required = true, description = "The input S3 bucket region")
  private String inputBucketRegion;
  @Option(names = {"-ob", "--output-bucket"}, required = true, description = "The output S3 bucket where to put converted Parquet files")
  private String outputBucket;
  @Option(names = {"-obr", "--output-bucket-region"}, required = true, description = "The output S3 bucket region")
  private String outputBucketRegion;
//  @Option(names = {"-ds", "--data-set"}, required = true, split = ",", defaultValue = "APB,CTD,DRB,GLD,MBT,MRB,OSD,PFL,SUR,UOR,XBT", description = "A comma separated list of data codes - Default: ${DEFAULT-VALUE}")
//  private List<String> datasets;
//  @Option(names = {"-p", "--processing-level"}, required = true, split = ",", defaultValue = "OBS,STD", description = "A comma separated list of processing levels - Default: ${DEFAULT-VALUE}")
//  private List<String> processingLevels;
//  @Option(names = {"-c", "--concurrency"}, required = true, defaultValue = "1", description = "The number of source files to process at a time")
//  private int concurrency;
//
//  @Option(names = {"-ip", "--input-prefix"}, description = "An optional key prefix of where to read input files if not in the root of the input bucket")
//  private String sourcePrefix;
//  @Option(names = {"-op", "--output-prefix"}, description = "An optional key prefix of where to write output files if not in the root of the output bucket")
//  private String outputPrefix;
//  @Option(names = {"-s", "--subset"}, split = ",", description = "A comma separated list file names to process. If omitted all files defined by the dataset and processing levels will be processed")
//  private List<String> sourceFileSubset;
//  @Option(names = {"-td", "--temp-directory"}, description = "A working directory where input files can be placed while processing. Defaults to the \"java.io.tmpdir\" directory")
//  private Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"));

  @Option(names = {"-ia", "--input-access"}, description = "An optional access key for the input bucket")
  private String inputAccessKey;
  @Option(names = {"-is", "--input-secret"}, description = "An optional secret key for the input bucket")
  private String inputSecretKey;

  @Option(names = {"-oa", "--output-access"}, description = "An optional access key for the output bucket")
  private String outputAccessKey;
  @Option(names = {"-os", "--output-secret"}, description = "An optional secret key for the output bucket")
  private String outputSecretKey;

  @Override
  public void run() {
    SparkSession.Builder sparkBuilder = SparkSession.builder()
        .config(String.format("spark.hadoop.fs.s3a.bucket.%s.endpoint.region", outputBucket), outputBucketRegion);
    if (outputAccessKey != null) {
      sparkBuilder.config(String.format("spark.hadoop.fs.s3a.bucket.%s.access.key", outputBucket), outputAccessKey);
      sparkBuilder.config(String.format("spark.hadoop.fs.s3a.bucket.%s.secret.key", outputBucket), outputSecretKey);
    }
    if (inputAccessKey != null) {
      sparkBuilder.config(String.format("spark.hadoop.fs.s3a.bucket.%s.access.key", inputBucket), inputAccessKey);
      sparkBuilder.config(String.format("spark.hadoop.fs.s3a.bucket.%s.secret.key", inputBucket), inputSecretKey);
    }

    SparkSession spark = sparkBuilder.getOrCreate();

//    SparklerExecutor executor = new SparklerExecutor(spark, inputBucket, outputBucket, inputKey, outputPrefix);
//    executor.run();
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new Sparkler()).execute(args));
  }

}
